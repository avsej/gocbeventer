package eventer

import "gopkg.in/couchbase/gocb.v1"

type WatcherCtrlr struct {
	cntlBucket *gocb.Bucket
	watchers []*Watcher
}

func (w *WatcherCtrlr) GetWatchers() []*Watcher {
	return w.watchers
}

func (w *WatcherCtrlr) AddWatcher(uuid string, config *WatcherConfig) (*Watcher, error) {
	watcher, err := makeWatcher(uuid, w.cntlBucket, config)
	if err != nil {
		return nil, err
	}

	w.watchers = append(w.watchers, watcher)
	return watcher, nil
}
