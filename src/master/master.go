package master

import (
	"context"
	"fmt"
	"hash/maphash"

	"github.com/poorlydefinedbehaviour/map_reduce_go/src/contracts"
	"github.com/poorlydefinedbehaviour/map_reduce_go/src/partitioning"
	"github.com/poorlydefinedbehaviour/map_reduce_go/src/set"
)

type taskID = uint64
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
	nextTaskID taskID
	config     Config
	// To keep track of the state of each task.
	tasks   map[taskID]*task
	workers map[WorkerAddr]contracts.WorkerClient

	// Workers that are ready to execute tasks.
	idleWorkers *set.Set[WorkerAddr]
	// Workers that are executing tasks.
	busyWorkers *set.Set[WorkerAddr]
	partitioner partitioning.Partitioner
}

type Config struct {
	// The max number of workers that should be used to run Map tasks.
	NumberOfMapWorkers uint32
}

type pendingTask struct {
	partitionFilePaths []string
}

type task struct {
	state    taskState
	taskType taskType
	id       taskID

	// Path to the file that contains the input data.
	filePath string
	// The address of the worker that's executing the task.
	WorkerAddr WorkerAddr
}

func New(config Config, partitioner partitioning.Partitioner) (*Master, error) {
	if config.NumberOfMapWorkers == 0 {
		return nil, fmt.Errorf("number of map taks must be greater than 0")
	}
	master := &Master{
		nextTaskID:  1,
		config:      config,
		tasks:       make(map[uint64]*task, 0),
		idleWorkers: set.New[WorkerAddr](),
		busyWorkers: set.New[WorkerAddr](),
		workers:     make(map[WorkerAddr]contracts.WorkerClient),
		partitioner: partitioner,
	}

	return master, nil
}

func (master *Master) getNextTaskID() taskID {
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
	if input.Folder == "" {
		return ValidatedInput{}, fmt.Errorf("folder to store intermediary files is required")
	}
	if input.NumberOfMapTasks == 0 {
		return ValidatedInput{}, fmt.Errorf("number of map tasks cannot be 0")
	}
	if input.NumberOfReduceTasks == 0 {
		return ValidatedInput{}, fmt.Errorf("number of reduce tasks cannot be 0")
	}
	if input.Map == nil {
		return ValidatedInput{}, fmt.Errorf("map function is required")
	}
	if input.Reduce == nil {
		return ValidatedInput{}, fmt.Errorf("map function is required")
	}
	return ValidatedInput{value: input}, nil
}

func (master *Master) HeartbeatReceived(ctx context.Context, workerAddr string) {
	if err := master.bus.AssignMapTask(workerAddr, filePath); err != nil {
		panic("todo")
	}
}

func (master *Master) Run(ctx context.Context, input ValidatedInput) error {
	partitionFilePaths, err := master.partitioner.Partition(
		input.value.File,
		input.value.Folder,
		input.value.NumberOfPartitions,
	)
	if err != nil {
		return fmt.Errorf("partitioning input file: %w", err)
	}

	// Create tasks that will be assigned to workers later.
	for _, filePath := range partitionFilePaths {
		task := &task{state: taskStateIdle, id: master.getNextTaskID(), filePath: filePath, WorkerAddr: ""}
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

		workerAddr := master.idleWorkers.First()
		if workerAddr == nil {
			return
		}

		worker := master.workers[*workerAddr]
		if err := worker.AssignMapTask(ctx, task.filePath); err != nil {
			fmt.Println("assigning task to worker: %s", err)
			continue
		}

		master.idleWorkers.Remove(*workerAddr)
		master.busyWorkers.Add(*workerAddr)
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
