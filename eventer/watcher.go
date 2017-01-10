package eventer

import (
	"fmt"
	"regexp"
	"time"

	sdcp "github.com/brett19/gosimpledcp"
	"gopkg.in/couchbase/gocb.v1"
	"gopkg.in/couchbase/gocbcore.v2"
)

type WatcherConfig struct {
	KeyFilter string
	Script string
	ExecutorsPerNode int
	MaxAckWriteDelay time.Duration
}

type Watcher struct {
	Uuid string
	KeyFilter *regexp.Regexp

	streamer *sdcp.DcpStreamer
	executors []*Executor
	workers map[uint16]*watcherWorker
	cntlBucket *gocb.Bucket
}

type watcherWorker struct {
	sdcp.DcpEventHandler

	parent *Watcher
	executor *Executor
	vbId uint16

	state sdcp.DcpStreamState
	stream *sdcp.DcpStream
}

func (w *watcherWorker) Run() {
	w.retrieveState()

	stream, err := w.parent.streamer.OpenStream(w.vbId, w.state, w)
	if err != nil {
		// TODO: Don't do this!
		panic(err)
	}

	w.stream = stream
}

func (w *watcherWorker) Close() {
	// TODO: Implement worker shutdown
	panic("Not yet supported")
}

type jsonStreamState struct {
	SeqNo gocbcore.SeqNo `json:"seqNo"`
	SnapStartSeqNo gocbcore.SeqNo `json:"snapStartSeqNo"`
	SnapEndSeqNo gocbcore.SeqNo `json:"snapEndSeqNo"`
	VbUuids []gocbcore.FailoverEntry `json:"vbUuids"`
}

func (w *watcherWorker) retrieveState() {
	dataKey := fmt.Sprintf("watchervb::%d::%s", w.vbId, w.parent.Uuid)

	var dbState jsonStreamState
	_, err := w.parent.cntlBucket.Get(dataKey, &dbState)
	if err == gocbcore.ErrKeyNotFound {
		w.state.SeqNo = 0
		w.state.SnapState.StartSeqNo = 0
		w.state.SnapState.EndSeqNo = 0
		w.state.VbState.Entries = nil
		return
	} else if err != nil {
		panic(err)
	}

	w.state.SeqNo = dbState.SeqNo
	w.state.SnapState.StartSeqNo = dbState.SnapStartSeqNo
	w.state.SnapState.EndSeqNo = dbState.SnapEndSeqNo
	w.state.VbState.Entries = dbState.VbUuids
}

// TODO: Make this not use the AckOptions
func (w *watcherWorker) persistState(opts AckOptions) {
	dataKey := fmt.Sprintf("watchervb::%d::%s", w.vbId, w.parent.Uuid)

	dbState := jsonStreamState{
		SeqNo: w.state.SeqNo,
		SnapStartSeqNo: w.state.SnapState.StartSeqNo,
		SnapEndSeqNo: w.state.SnapState.EndSeqNo,
		VbUuids: w.state.VbState.Entries,
	}

	if opts.PersistTo > 0 || opts.ReplicateTo > 0 {
		fmt.Printf("Writing VB data %d (%d,%d)\n", w.vbId, opts.ReplicateTo, opts.PersistTo)
		w.parent.cntlBucket.UpsertDura(dataKey, &dbState, 0, uint(opts.ReplicateTo), uint(opts.PersistTo))
	} else {
		// TODO: Make this delayed instead...
		fmt.Printf("Writing VB data %d (-,-)\n", w.vbId)
		go w.parent.cntlBucket.Upsert(dataKey, &dbState, 0)
	}
}


func (w *watcherWorker) Rollback(vbId uint16, newState sdcp.DcpStreamState) {
	w.state = newState
	w.persistState(AckOptions{})
}
func (w *watcherWorker) NewStateEntries(vbId uint16, vbState sdcp.DcpVbucketState) {
	w.state.VbState = vbState
	w.persistState(AckOptions{})
}
func (w *watcherWorker) BeginSnapshot(vbId uint16, snapState sdcp.DcpSnapshotState) {
	w.state.SnapState = snapState
	w.persistState(AckOptions{})
}
func (w *watcherWorker) Mutation(vbId uint16, item sdcp.DcpMutation) {
	// TODO: Do not ignore errors
	ackOpts, _ := w.executor.RunMutation(vbId, item)
	w.state.SeqNo = item.SeqNo
	w.persistState(ackOpts)
}
func (w *watcherWorker) Deletion(vbId uint16, item sdcp.DcpDeletion) {
	// TODO: Do not ignore errors
	ackOpts, _ := w.executor.RunDeletion(vbId, item)
	w.state.SeqNo = item.SeqNo
	w.persistState(ackOpts)
}
func (w *watcherWorker) Expiration(vbId uint16, item sdcp.DcpExpiration) {
	// TODO: Do not ignore errors
	ackOpts, _ := w.executor.RunExpiration(vbId, item)
	w.state.SeqNo = item.SeqNo
	w.persistState(ackOpts)
}

func (w *Watcher) getExecutor(vbId uint16) *Executor {
	numExecutors := len(w.executors)
	return w.executors[int(vbId) % numExecutors]
}

func (w *Watcher) GetNumVbuckets() uint16 {
	return w.streamer.GetNumVbuckets()
}

func (w *Watcher) GetActiveVbuckets() []uint16 {
	var vbs []uint16
	for i := range w.workers {
		vbs = append(vbs, i)
	}
	return vbs
}

func (w *Watcher) StartVbucket(vbId uint16) {
	if w.workers[vbId] != nil {
		panic("Attempted to start a vbucket thats already running.")
	}

	fmt.Printf("Starting worker for %d\n", vbId)

	worker := &watcherWorker{
		parent: w,
		executor: w.getExecutor(vbId),
		vbId: vbId,
	}

	w.workers[vbId] = worker

	worker.Run()
}

func (w *Watcher) StopVbucket(vbId uint16) {
	worker := w.workers[vbId]
	if worker == nil {
		panic("Attempted to stop a vbucket that is not running.")
	}

	fmt.Printf("Stopping worker for %d\n", vbId)

	delete(w.workers, vbId)
	worker.Close()
}

func makeWatcher(uuid string, cntlBucket *gocb.Bucket, config *WatcherConfig) (*Watcher, error) {
	watcher := &Watcher{
		workers:make(map[uint16]*watcherWorker),
	}

	fmt.Printf("Creating watcher %s\n", uuid)

	watcher.Uuid = uuid
	watcher.cntlBucket = cntlBucket

	keyFilter, err := regexp.Compile(config.KeyFilter)
	if err != nil {
		return nil, err
	}
	watcher.KeyFilter = keyFilter

	authFn := func(srv gocbcore.AuthClient, deadline time.Time) error {
		// Build PLAIN auth data
		userBuf := []byte("default")
		passBuf := []byte("")
		authData := make([]byte, 1+len(userBuf)+1+len(passBuf))
		authData[0] = 0
		copy(authData[1:], userBuf)
		authData[1+len(userBuf)] = 0
		copy(authData[1+len(userBuf)+1:], passBuf)

		// Execute PLAIN authentication
		_, err := srv.ExecSaslAuth([]byte("PLAIN"), authData, deadline)

		return err
	}

	agentCfg := &gocbcore.AgentConfig{
		MemdAddrs: []string{"localhost:11210"},
		BucketName: "default",
		AuthHandler: authFn,
		ConnectTimeout:       60000 * time.Millisecond,
		ServerConnectTimeout: 7000 * time.Millisecond,
		NmvRetryDelay:        100 * time.Millisecond,
	}


	for i := 0; i < config.ExecutorsPerNode; i++ {
		executor, err := makeExecutor(watcher, config.Script)
		if err != nil {
			return nil, err
		}

		watcher.executors = append(watcher.executors, executor)
	}

	streamer, err := sdcp.NewDcpStreamer(agentCfg, "STRM-" + watcher.Uuid)
	if err != nil {
		// TODO: Shut down the executors!
		return nil, err
	}

	watcher.streamer = streamer

	return watcher, nil
}
