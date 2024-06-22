package master

import (
	"context"
	"fmt"
	"hash/maphash"
	"sync"
	"time"

	"github.com/poorlydefinedbehaviour/map_reduce_go/src/constants"
	"github.com/poorlydefinedbehaviour/map_reduce_go/src/contracts"
	"github.com/poorlydefinedbehaviour/map_reduce_go/src/partitioning"
)

type WorkerAddr = string
type taskState uint8
type taskType = uint8

const (
	// The task is ready to be picked up.
	taskStateIdle taskState = 1 << 0
	// The task is being executed by some worker.
	taskStateInProgress taskState = 1 << 1
	// The task has been executed and is completed.
	taskStateInCompleted taskState = 1 << 2

	taskTypeMap    taskType = 1 << 0
	taskTypeReduce taskType = 1 << 1
)

type Master struct {
	mu         *sync.Mutex
	nextTaskID contracts.TaskID
	config     Config
	// To keep track of the state of each task.
	tasks map[contracts.TaskID]*task
	// Workers that are ready to execute tasks.
	idleWorkers map[WorkerAddr]worker
	// Workers that are executing tasks.
	busyWorkers map[WorkerAddr]worker
	partitioner partitioning.Partitioner
	bus         contracts.MessageBus
}

type worker struct {
	lastHeartbeatAt time.Time
	addr            WorkerAddr
}

type Config struct {
	// The max number of workers that should be used to run Map tasks.
	NumberOfMapWorkers uint32
	// Folder used to write partitioned input files.
	WorkspaceFolder string
}

type pendingTask struct {
	partitionFilePaths []string
}

type task struct {
	state    taskState
	taskType taskType
	script   string
	id       contracts.TaskID

	// Path to the file that contains the input data.
	filePath string
	// The address of the worker that's executing the task.
	WorkerAddr WorkerAddr
}

func New(config Config, partitioner partitioning.Partitioner, bus contracts.MessageBus) (*Master, error) {
	if config.NumberOfMapWorkers == 0 {
		return nil, fmt.Errorf("number of map taks must be greater than 0")
	}
	if config.WorkspaceFolder == "" {
		return nil, fmt.Errorf("workspace folder is required")
	}
	master := &Master{
		mu:          &sync.Mutex{},
		nextTaskID:  1,
		config:      config,
		tasks:       make(map[contracts.TaskID]*task, 0),
		idleWorkers: make(map[WorkerAddr]worker),
		busyWorkers: make(map[WorkerAddr]worker),
		partitioner: partitioner,
		bus:         bus,
	}

	return master, nil
}

func (master *Master) ControlLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			fmt.Println("todo: clean up workers that haven't sent heartbeats")
			time.Sleep(10 * time.Second)
		}
	}
}

func (master *Master) getNextTaskID() contracts.TaskID {
	taskID := master.nextTaskID
	master.nextTaskID++
	return taskID
}

// [Input] after it has been validated.
type ValidatedInput struct {
	value *contracts.Input
}

func NewValidatedInput(input contracts.Input) (ValidatedInput, error) {
	return validateInput(&input)
}

func validateInput(input *contracts.Input) (ValidatedInput, error) {
	if input.File == "" {
		return ValidatedInput{}, fmt.Errorf("input file path is required")
	}
	if input.NumberOfMapTasks == 0 {
		return ValidatedInput{}, fmt.Errorf("number of map tasks cannot be 0")
	}
	if input.NumberOfReduceTasks == 0 {
		return ValidatedInput{}, fmt.Errorf("number of reduce tasks cannot be 0")
	}

	return ValidatedInput{value: input}, nil
}

func (master *Master) HeartbeatReceived(ctx context.Context, workerState contracts.WorkerState, workerAddr string) error {
	master.mu.Lock()
	defer master.mu.Unlock()

	worker := worker{lastHeartbeatAt: time.Now(), addr: workerAddr}

	switch workerState {
	case constants.WorkerStateIdle:
		delete(master.busyWorkers, workerAddr)
		master.idleWorkers[workerAddr] = worker
	case constants.WorkerStateWorking:
		delete(master.idleWorkers, workerAddr)
		master.busyWorkers[workerAddr] = worker
	default:
		return fmt.Errorf("unexpected worker state: %+v", workerState)
	}

	return nil
}

func (master *Master) Run(ctx context.Context, input ValidatedInput) error {
	master.mu.Lock()
	defer master.mu.Unlock()

	partitionFilePaths, err := master.partitioner.Partition(
		input.value.File,
		master.config.WorkspaceFolder,
		input.value.NumberOfPartitions,
	)
	if err != nil {
		return fmt.Errorf("partitioning input file: %w", err)
	}

	// Create tasks that will be assigned to workers later.
	for _, filePath := range partitionFilePaths {
		task := &task{state: taskStateIdle, id: master.getNextTaskID(), script: input.value.Script, filePath: filePath, WorkerAddr: ""}
		master.tasks[task.id] = task
	}

	master.tryAssignTasks(ctx)

	return nil
}

// Assigns pending tasks to idle workers.
func (master *Master) tryAssignTasks(ctx context.Context) {
	for _, task := range master.tasks {
		if task.state != taskStateIdle {
			continue
		}
		fmt.Printf("\n\naaaaaaa master.idleWorkers %+v\n\n", master.idleWorkers)

		var anyIdleWorker *worker
		for _, worker := range master.idleWorkers {
			anyIdleWorker = &worker
			break
		}
		if anyIdleWorker == nil {
			return
		}

		fmt.Printf("\n\naaaaaaa assigning file %s to worker %s\n\n", task.filePath, anyIdleWorker.addr)
		if err := master.bus.AssignMapTask(ctx, anyIdleWorker.addr, task.id, task.script, task.filePath); err != nil {
			fmt.Printf("assigning task to worker: %s\n", err)
			continue
		}

		delete(master.idleWorkers, anyIdleWorker.addr)
		master.busyWorkers[anyIdleWorker.addr] = *anyIdleWorker
		task.state = taskStateInProgress
	}
}

// TODO: remove if this won't be used.

// Simple hash(key) % partitions partitioning function.
func defaultPartitionFunction(key string, numberOfReduceTasks uint32) int64 {
	var hash maphash.Hash
	// maphash.hash.Write never fails. See the docs.
	_, _ = hash.Write([]byte(key))
	return int64(hash.Sum64()) % int64(numberOfReduceTasks)
}
