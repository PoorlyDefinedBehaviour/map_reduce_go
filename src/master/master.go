package master

type taskID = uint64
type workerID = uint64
type taskState uint8

const (
	// The task is ready to be picked up.
	taskStateIdle taskState = 1 << 0
	// The task is being executed by some worker.
	taskStateInProgress taskState = 1 << 1
	// The task has been executed and is completed.
	taskStateInCompleted taskState = 1 << 2
)

type Master struct {
	config Config
	tasks  map[taskID]task
}

type Config struct {
	NumberOfMapWorkers uint32
}

type task struct {
	state    taskState
	workerID uint64
}

func New(config Config) *Master {
	return &Master{
		config: config,
		tasks:  make(map[uint64]task, 0),
	}
}

func (master *Master) Assign(workers []workerID) {
	// assign map tasks to map workers
}
