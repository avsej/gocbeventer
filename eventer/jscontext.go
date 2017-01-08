package eventer

import (
	"time"
	"github.com/robertkrimen/otto"
	"fmt"
	"net/http"
	"io/ioutil"
)

type EventFunc func(vm *otto.Otto)

type JsContext struct {
	vm *otto.Otto
	eventQueue chan EventFunc
	loopRunning bool
}

func MakeJsContext() (*JsContext, error) {
	ctx := &JsContext{}
	ctx.vm = otto.New()
	ctx.eventQueue = make(chan EventFunc)
	ctx.loopRunning = false
	ctx.setupDefaultStuff()
	return ctx, nil
}

func (ctx *JsContext) setupDefaultStuff() error {
	consoleMap := make(map[string]interface{})
	consoleMap["log"] = func(call otto.FunctionCall) otto.Value {
		for i, arg := range call.ArgumentList {
			if i != 0 {
				fmt.Print(" ")
			}
			fmt.Print(arg.String())
		}
		fmt.Print("\n")

		return otto.TrueValue()
	}
	ctx.vm.Set("console", consoleMap)

	httpMap := make(map[string]interface{})
	httpMap["get"] = func(call otto.FunctionCall) otto.Value {
		uri, _ := call.Argument(0).ToString()
		callback := call.Argument(1)

		go func() {
			resp, err := http.Get(uri)
			var body []byte
			if err == nil {
				body, err = ioutil.ReadAll(resp.Body)
				resp.Body.Close()
			}

			ctx.eventQueue <- func(vm *otto.Otto) {
				if err != nil {
					jsErr := ctx.vm.MakeCustomError("Error", err.Error())
					callback.Call(otto.UndefinedValue(), jsErr, otto.UndefinedValue())
					return
				}

				callback.Call(otto.UndefinedValue(), otto.NullValue(), string(body))
			}
		}()
		return otto.TrueValue()
	}
	ctx.vm.Set("http", httpMap)

	setImmediateFn := func(call otto.FunctionCall) otto.Value {
		callbackFn := call.Argument(0)
		ctx.eventQueue <- func(vm *otto.Otto) {
			callbackFn.Call(otto.UndefinedValue())
		}
		return otto.UndefinedValue()
	}
	ctx.vm.Set("setImmediate", setImmediateFn)

	setTimeoutFn := func(call otto.FunctionCall) otto.Value {
		callbackFn := call.Argument(0)

		// TODO: Do not ignore this error...
		timeoutMs, _ := call.Argument(1).ToInteger()

		waitCh := time.After(time.Duration(timeoutMs) * time.Millisecond)
		go func() {
			<- waitCh

			ctx.eventQueue <- func(vm *otto.Otto) {
				callbackFn.Call(otto.UndefinedValue())
			}
		}()
		return otto.UndefinedValue()
	}
	ctx.vm.Set("setTimeout", setTimeoutFn)

	return nil
}

func (ctx *JsContext) RunAsync(fn EventFunc) {
	if !ctx.loopRunning {
		panic("Can only RunAsync when event loop is running.")
	}

	// This needs to use an internal list instead...
	ctx.eventQueue <- fn
}

func (ctx *JsContext) RunImmediate(fn EventFunc) {
	if ctx.loopRunning {
		panic("Can only RunImmediate when event loop is not running.")
	}

	fn(ctx.vm)
}

func (ctx *JsContext) RunEventLoop() {
	ctx.loopRunning = true

	go func() {
		for {
			event := <-ctx.eventQueue
			event(ctx.vm)
		}
	}()
}