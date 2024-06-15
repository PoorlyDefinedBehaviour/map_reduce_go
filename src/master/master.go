package master

import (
	"context"
	"fmt"

	"github.com/poorlydefinedbehaviour/map_reduce_go/src/set"
)

type taskID = uint64
type WorkerID = uint64
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
	nextTaskID taskID
	config     Config
	// To keep track of the state of each task.
	tasks   map[taskID]task
	workers map[WorkerID]*Worker
	// Workers that are ready to execute tasks.
	idleWorkers *set.Set[WorkerID]
	// Workers that are executing tasks.
	busyWorkers *set.Set[WorkerID]
}

type Config struct {
	// The max number of workers that should be used to run Map tasks.
	NumberOfMapWorkers uint32
	// A list of workers that can be used to run tasks.
	Workers []Worker
}

type worker struct {
	id WorkerID
}

type task struct {
	state taskState
	id    taskID
	// Path to the file that contains the input data.
	filePath string
	// The ID of the worker that's executing the task.
	workerID uint64
}

func New(config Config) (*Master, error) {
	if config.NumberOfMapWorkers == 0 {
		return nil, fmt.Errorf("number of map taks must be greater than 0")
	}
	master := &Master{
		config:      config,
		tasks:       make(map[uint64]task, 0),
		idleWorkers: set.New[WorkerID](),
		busyWorkers: set.New[WorkerID](),
	}

	for _, worker := range config.Workers {
		worker := worker
		master.idleWorkers.Add(worker.ID)
		master.workers[worker.ID] = &worker
	}

	return master, nil
}

func (master *Master) getNextTaskID() taskID {
	taskID := master.nextTaskID
	master.nextTaskID++
	return taskID
}

func (master *Master) RunTask() error {
	panic("todo")
}

func (master *Master) Add(ctx context.Context, filePath string) error {
	// Grab any worker.
	workerID, found := master.idleWorkers.Find(func(_ *uint64) bool { return true })
	if !found {
		panic("todo")
	}
	master.idleWorkers.Remove(*workerID)

	fmt.Printf("\n\naaaaaaa workerID %+v\n\n", workerID)

	worker := master.workers[*workerID]
	if err := worker.AssignMapTask(ctx, filePath); err != nil {
		return fmt.Errorf("sending AssignMapTask message to worker: %w", err)
	}

	task := task{
		id:       master.getNextTaskID(),
		state:    taskStateIdle,
		filePath: filePath,
		workerID: *workerID,
	}
	master.tasks[task.id] = task
	master.busyWorkers.Add(*workerID)

	return nil
}
