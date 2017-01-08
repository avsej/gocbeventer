package eventer

import (
	"github.com/couchbase/gocb"
	"time"
	"fmt"
	"io/ioutil"
)

func Start() {
	cluster, err := gocb.Connect("couchbase://localhost")
	if err != nil {
		panic(err)
	}

	bucket, err := cluster.OpenBucket("eventerctrl", "")
	if err != nil {
		panic(err)
	}

	manager, err := newEventingManager(bucket)
	if err != nil {
		panic(err)
	}

	fmt.Printf("Assigned myself UUID %s\n", manager.Manager.GetUuid())

	//*
	watcherScript, _ := ioutil.ReadFile("test.js")
	watcherCfg := WatcherConfig {
		KeyFilter: "^.*test.*$",
		Script: string(watcherScript),
		ExecutorsPerNode: 2,
		MaxAckWriteDelay: time.Second * 5,
	}
	manager.AddWatcher(watcherCfg)
	//*/

	time.Sleep(time.Second * 10)

	manager.Close()
}
