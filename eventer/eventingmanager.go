package eventer

import (
	"errors"
	"fmt"
	"time"

	"github.com/brett19/gocbdistapp"
	"github.com/google/uuid"
	"gopkg.in/couchbase/gocb.v1"
)

var (
	errDoNotUpdate = errors.New("Do not update")
)

type jsonWatcher struct {
	KeyFilter string `json:"keyFilter"`
	Script string	`json:"script"`
	ExecutorsPerNode int `json:"executorsPerNode"`
	MaxAckWriteDelay time.Duration `json:"maxAckWriteDelay"`
}

type jsonDataWatcher struct {
	Uuid string `json:"uuid"`
	NumVbuckets uint16	`json:"numVbuckets"`
	Assigned map[string][]uint16 `json:"assigned"`
	Running map[string][]uint16 `json:"running"`
}

type jsonData struct {
	Watchers []jsonDataWatcher `json:"watchers"`
}

type eventingManager struct {
	Manager *cbdistapp.ClusterManager
	Controller *WatcherCtrlr
}

func newEventingManager(bucket *gocb.Bucket) (*eventingManager, error) {
	handler := &eventingManager{}

	manager, err := cbdistapp.NewClusterManager(bucket, handler)
	if err != nil {
		return nil, err
	}
	handler.Manager = manager

	handler.Controller = &WatcherCtrlr{
		cntlBucket: bucket,
	}

	manager.Start()

	return handler, nil
}

func (h eventingManager) Close() {
	h.Manager.Close()
}

func (h eventingManager) loadWatcherConfig(uuid string) (*WatcherConfig, error) {
	dataKey := fmt.Sprintf("watcher::%s", uuid)
	var jsonConfig jsonWatcher
	err := h.Manager.KvGet(dataKey, &jsonConfig)
	if err != nil {
		return nil, err
	}

	config := WatcherConfig {
		KeyFilter: jsonConfig.KeyFilter,
		Script: jsonConfig.Script,
		ExecutorsPerNode: jsonConfig.ExecutorsPerNode,
		MaxAckWriteDelay: jsonConfig.MaxAckWriteDelay,
	}
	return &config, nil
}

func (h eventingManager) AddWatcher(config WatcherConfig) error {
	watcherUuid := uuid.New().String()
	jsonConfig := jsonWatcher {
		KeyFilter: config.KeyFilter,
		Script: config.Script,
		ExecutorsPerNode: config.ExecutorsPerNode,
		MaxAckWriteDelay: config.MaxAckWriteDelay,
	}

	dataKey := fmt.Sprintf("watcher::%s", watcherUuid)
	err := h.Manager.KvInsert(dataKey, &jsonConfig)
	if err != nil {
		return err
	}

	jsonDataWatcher := jsonDataWatcher{
		Uuid: watcherUuid,
		NumVbuckets: 0,
		Assigned: make(map[string][]uint16),
		Running: make(map[string][]uint16),
	}

	h.Manager.Update(func(state cbdistapp.IMutableState) error {
		var data jsonData
		err := state.GetData(&data)
		if err != nil {
			return err
		}

		data.Watchers = append(data.Watchers, jsonDataWatcher)

		err = state.SetData(data)
		if err != nil {
			return err
		}

		return nil
	})

	return nil
}

func normalizeWatcherNodes(state cbdistapp.IState, watcher *jsonDataWatcher) bool {
	nodes := state.GetNodes()

	var deletedAssignedNodes []string
	for nodeUuid := range watcher.Assigned {
		foundNode := false
		for _, node := range nodes {
			if node.GetUuid() == nodeUuid {
				foundNode = true
				break
			}
		}

		if !foundNode {
			deletedAssignedNodes = append(deletedAssignedNodes, nodeUuid)
		}
	}

	var deletedRunningNodes []string
	for nodeUuid := range watcher.Running {
		foundNode := false
		for _, node := range nodes {
			if node.GetUuid() == nodeUuid {
				foundNode = true
				break
			}
		}

		if !foundNode {
			deletedRunningNodes = append(deletedRunningNodes, nodeUuid)
		}
	}

	var addedNodes []string
	for _, node := range nodes {
		foundNode := false
		for nodeUuid := range watcher.Assigned {
			if nodeUuid == node.GetUuid() {
				foundNode = true
				break
			}
		}

		if !foundNode {
			addedNodes = append(addedNodes, node.GetUuid())
		}
	}

	if len(deletedAssignedNodes) == 0 && len(deletedRunningNodes) == 0 && len(addedNodes) == 0 {
		return false
	}

	for _, nodeUuid := range deletedAssignedNodes {
		delete(watcher.Assigned, nodeUuid)
	}

	for _, nodeUuid := range deletedRunningNodes {
		delete(watcher.Running, nodeUuid)
	}

	for _, nodeUuid := range addedNodes {
		watcher.Assigned[nodeUuid] = nil
	}

	return true
}

func normalizeWatcher(state cbdistapp.IState, watcher *jsonDataWatcher) bool {
	assignedMap := make(map[uint16]bool)

	for i := uint16(0); i < watcher.NumVbuckets; i++ {
		assignedMap[i] = false
	}

	for _, vbList := range watcher.Assigned {
		for _, vbId := range vbList {
			assignedMap[vbId] = true
		}
	}

	var freeVbs []uint16

	for vbId, isAssigned := range assignedMap {
		if !isAssigned {
			freeVbs = append(freeVbs, vbId)
		}
	}

	hasChanged := false

	maxVbsPerNode := (int(watcher.NumVbuckets) / len(watcher.Assigned)) + 1

	for nodeUuid := range watcher.Assigned {
		if len(watcher.Assigned[nodeUuid]) > maxVbsPerNode {
			newFreeVbs := watcher.Assigned[nodeUuid][maxVbsPerNode:]
			watcher.Assigned[nodeUuid] = watcher.Assigned[nodeUuid][:maxVbsPerNode]
			freeVbs = append(freeVbs, newFreeVbs...)
			hasChanged = true
		}
	}

	SortUint16s(freeVbs)

	for nodeUuid := range watcher.Assigned {
		numVbs := len(watcher.Assigned[nodeUuid])
		vbsNeeded := maxVbsPerNode - numVbs

		if vbsNeeded > 0 {
			if vbsNeeded > len(freeVbs) {
				vbsNeeded = len(freeVbs)
			}

			newVbs := freeVbs[:vbsNeeded]
			freeVbs = freeVbs[vbsNeeded:]
			watcher.Assigned[nodeUuid] = append(watcher.Assigned[nodeUuid], newVbs...)
			hasChanged = true
		}
	}

	if len(freeVbs) > 0 {
		// TODO: Maybe handle this better...
		panic("Woah, shit")
	}

	return hasChanged
}

func (h eventingManager) NormalizeState(state cbdistapp.IMutableState) {
	fmt.Printf("NormalizeState\n")

	var data jsonData
	state.GetData(&data)

	hasChanged := false

	for i, watcher := range data.Watchers {
		// Skip watchers we don't have a vbucket count for yet
		if watcher.NumVbuckets == 0 {
			continue
		}

		if normalizeWatcherNodes(state, &data.Watchers[i]) {
			hasChanged = true
		}

		if normalizeWatcher(state, &data.Watchers[i]) {
			hasChanged = true
		}
	}

	if hasChanged {
		state.SetData(data)
	}
}

func (h eventingManager) updateWatcherNumVbs(lclWatcher *Watcher) {
	h.Manager.Update(func(state cbdistapp.IMutableState) error {
		var data jsonData
		err := state.GetData(&data)
		if err != nil {
			return err
		}

		var foundWatcher *jsonDataWatcher
		for i, watcher := range data.Watchers {
			if watcher.Uuid == lclWatcher.Uuid {
				foundWatcher = &data.Watchers[i]
				break
			}
		}

		if foundWatcher == nil || foundWatcher.NumVbuckets != 0 {
			return nil
		}

		foundWatcher.NumVbuckets = lclWatcher.GetNumVbuckets()
		state.SetData(data)

		return nil
	})
}

func (h eventingManager) updateRunnings() {
	myNodeUuid := h.Manager.GetUuid()

	h.Manager.Update(func(state cbdistapp.IMutableState) error {
		var data jsonData
		err := state.GetData(&data)
		if err != nil {
			return err
		}

		hasChanged := false

		for i, watcher := range data.Watchers {
			toRunMap := make(map[uint16]bool)

			for _, vbId := range watcher.Assigned[myNodeUuid] {
				toRunMap[vbId] = true
			}

			for _, vbList := range watcher.Running {
				for _, vbId := range vbList {
					toRunMap[vbId] = false
				}
			}

			var toRunList []uint16
			for vbId, needsRun := range toRunMap {
				if needsRun {
					toRunList = append(toRunList, vbId)
				}
			}

			if len(toRunList) == 0 {
				continue
			}

			data.Watchers[i].Running[myNodeUuid] = append(data.Watchers[i].Running[myNodeUuid], toRunList...)
			SortUint16s(data.Watchers[i].Running[myNodeUuid])
			hasChanged = true
		}

		if !hasChanged {
			return errDoNotUpdate
		}

		state.SetData(data)
		return nil
	})
}

func (h eventingManager) updateWatcher(watcher jsonDataWatcher) {
	fmt.Printf("Updating Watcher %+v\n", watcher)

	nodeUuid := h.Manager.GetUuid()
	runningVbs := watcher.Running[nodeUuid]

	lclWatchers := h.Controller.GetWatchers()
	var lclWatcher *Watcher
	for _, watcherIter := range lclWatchers {
		if watcherIter.Uuid == watcher.Uuid {
			lclWatcher = watcherIter
			break
		}
	}

	if lclWatcher == nil {
		// TODO: Make this better
		panic("Could not find local watcher")
	}

	// TODO: This will not work, when we have to shut down something, we
	//   will stop the stream before removing it from running which will
	//   make this code thing it needs to start it!
	// WARNING: LKDJGLSKDJGLKJSDGLKJSDLGKJSLKDJGLJSKDG
	activeVbs := lclWatcher.GetActiveVbuckets()
	for _, vbId := range runningVbs {
		foundActive := false
		for _, activeVbId := range activeVbs {
			if activeVbId == vbId {
				foundActive = true
				break
			}
		}

		if !foundActive {
			lclWatcher.StartVbucket(vbId)
		}
	}
}

func (h eventingManager) updateWatchers(config jsonData) {
	lclWatchers := h.Controller.GetWatchers()

	var newWatchers []jsonDataWatcher
	for _, watcher := range config.Watchers {
		foundWatcher := false
		for _, lclWatcher := range lclWatchers {
			if lclWatcher.Uuid == watcher.Uuid {
				foundWatcher = true
				break
			}
		}
		if !foundWatcher {
			newWatchers = append(newWatchers, watcher)
		}
	}

	var goneWatchers []*Watcher
	for _, lclWatcher := range lclWatchers {
		foundWatcher := false
		for _, watcher := range config.Watchers {
			if watcher.Uuid == lclWatcher.Uuid {
				foundWatcher = true
				break
			}
		}
		if !foundWatcher {
			goneWatchers = append(goneWatchers, lclWatcher)
		}
	}

	// TODO: Make this actually work...
	/*
	for _, lclWatcher := range goneWatchers {
		lclWatcher.Close()
	}
	*/

	for _, watcher := range newWatchers {
		watcherCfg, err := h.loadWatcherConfig(watcher.Uuid)
		if err != nil {
			// TODO: Don't do this!!
			panic(err)
		}

		lclWatcher, err := h.Controller.AddWatcher(watcher.Uuid, watcherCfg)
		if err != nil {
			// TODO: Handle this better...
			panic(err)
		}

		if watcher.NumVbuckets == 0 {
			go h.updateWatcherNumVbs(lclWatcher)
		} else if watcher.NumVbuckets != lclWatcher.GetNumVbuckets() {
			// TODO: Do something else here...
			panic("Wrong number of vbuckets!!")
		}
	}

	for _, watcher := range config.Watchers {
		h.updateWatcher(watcher)
	}

	// We do this asynchronously!  StateChanged cannot call back into the
	//  ClusterManager
	go h.updateRunnings()
}

func (h eventingManager) StateChanged(state cbdistapp.IState) {
	fmt.Printf("StateChanged\n")

	var config jsonData
	state.GetData(&config)

	h.updateWatchers(config)
}

func (h eventingManager) Disconnected() {
	panic("Disconnected")
}
