package eventer

import (
	"github.com/robertkrimen/otto"
	"fmt"
	sdcp "github.com/brett19/gosimpledcp"
)

type AckOptions struct {
	PersistTo int
	ReplicateTo int
}

type AckData struct {
	AckId uint64
	Options AckOptions
}

type Executor struct {
	parent *Watcher

	jsVm *JsContext
	waitCh chan AckData
	mutationFn otto.Value
	deletionFn otto.Value
	expirationFn otto.Value
	ackCounter uint64
}

func makeExecutor(w *Watcher, script string) (*Executor, error) {
	executor := &Executor{}
	executor.parent = w

	jsVm, err := MakeJsContext()
	if err != nil {
		return nil, err
	}

	jsVm.RunImmediate(func (vm *otto.Otto) {
		vm.Run(script)

		executor.mutationFn, _ = vm.Get("on_mutation")
		executor.deletionFn, _ = vm.Get("on_deletion")
		executor.expirationFn, _ = vm.Get("on_expiration")
	})

	jsVm.RunEventLoop()

	executor.jsVm = jsVm

	executor.waitCh = make(chan AckData)

	return executor, nil
}

func (e *Executor) ackFunc() func(call otto.FunctionCall) otto.Value {
	ackId := e.ackCounter
	return func(call otto.FunctionCall) otto.Value {
		e.waitCh <- AckData{
			AckId: ackId,
		}
		return otto.UndefinedValue()
	}
}

func (e *Executor) waitAck() AckOptions {
	for {
		ackId := e.ackCounter
		ackData := <-e.waitCh
		if ackData.AckId == ackId {
			return ackData.Options
		} else {
			fmt.Printf("User used ack on wrong object...")
		}
	}
}

func (e *Executor) runEvent(fn otto.Value, itemData map[string]interface{}) (AckOptions, error) {
	itemData["ack"] = e.ackFunc()

	e.jsVm.RunAsync(func(vm *otto.Otto) {
		fn.Call(otto.UndefinedValue(), itemData)
	})

	ackOptions := e.waitAck()
	return ackOptions, nil
}

func (e *Executor) RunMutation(vbId uint16, item sdcp.DcpMutation) (AckOptions, error) {
	if !e.mutationFn.IsFunction() {
		return AckOptions{}, nil
	}

	itemData := make(map[string]interface{})
	itemData["key"] = string(item.Key)
	return e.runEvent(e.mutationFn, itemData)
}

func (e *Executor) RunDeletion(vbId uint16, item sdcp.DcpDeletion) (AckOptions, error) {
	if !e.deletionFn.IsFunction() {
		return AckOptions{}, nil
	}

	itemData := make(map[string]interface{})
	itemData["key"] = string(item.Key)
	return e.runEvent(e.deletionFn, itemData)
}

func (e *Executor) RunExpiration(vbId uint16, item sdcp.DcpExpiration) (AckOptions, error) {
	if !e.expirationFn.IsFunction() {
		return AckOptions{}, nil
	}

	itemData := make(map[string]interface{})
	itemData["key"] = string(item.Key)
	return e.runEvent(e.expirationFn, itemData)
}
