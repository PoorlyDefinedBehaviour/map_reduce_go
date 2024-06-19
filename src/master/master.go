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
	partitioner partitioning.Partitioner
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

func New(config Config, partitioner partitioning.Partitioner) (*Master, error) {
	if config.NumberOfMapWorkers == 0 {
		return nil, fmt.Errorf("number of map taks must be greater than 0")
	}
	master := &Master{
		nextTaskID:  1,
		config:      config,
		tasks:       make(map[uint64]task, 0),
		idleWorkers: set.New[WorkerID](),
		busyWorkers: set.New[WorkerID](),
		workers:     make(map[uint64]*Worker),
		partitioner: partitioner,
	}

	for _, worker := range config.Workers {
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

func (master *Master) Add(ctx context.Context, filePath string) error {
	// Grab any worker.
	workerID, found := master.idleWorkers.Find(func(_ *uint64) bool { return true })
	if !found {
		return fmt.Errorf("no idle worker found")
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

func (master *Master) Run(ctx context.Context, input ValidatedInput) error {
	fmt.Printf("\n\naaaaaaa input.value.File %+v\n\n", input.value.File)
	fmt.Printf("\n\naaaaaaa input.value.Folder %+v\n\n", input.value.Folder)
	fmt.Printf("\n\naaaaaaa master %+v\n\n", master)
	fmt.Printf("\n\naaaaaaa master.partitioner %+v\n\n", master.partitioner)
	partitionFilePaths, err := master.partitioner.Partition(
		input.value.File,
		input.value.Folder,
		input.value.NumberOfPartitions,
	)
	if err != nil {
		return fmt.Errorf("partitioning input file: %w", err)
	}
	fmt.Printf("\n\naaaaaaa partitionFilePaths %+v\n\n", partitionFilePaths)

	for _, filePath := range partitionFilePaths {
		if err := master.Add(ctx, filePath); err != nil {
			return fmt.Errorf("assigning partition file to worker: %w", err)
		}
	}

	return nil
}

// TODO: remove if this won't be used.
// Simple hash(key) % partitions partitioning function.
func defaultPartitionFunction(key string, numberOfReduceTasks uint32) int64 {
	var hash maphash.Hash
	// maphash.hash.Write never fails. See the docs.
	_, _ = hash.Write([]byte(key))
	return int64(hash.Sum64()) % int64(numberOfReduceTasks)
}
