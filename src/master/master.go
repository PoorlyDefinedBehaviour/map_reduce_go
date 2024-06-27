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
type workerState = uint8

const (
	// The task is ready to be picked up.
	taskStateIdle taskState = 1 << 0
	// The task is being executed by some worker.
	taskStateInProgress taskState = 1 << 1
	// The task has been executed and is completed.
	taskStateInCompleted taskState = 1 << 2

	taskTypeMap    taskType = 1 << 0
	taskTypeReduce taskType = 1 << 1

	workerStateIdle workerState = 1 << 0
	workerStateBusy workerState = 1 << 1
)

type Master struct {
	mu         *sync.Mutex
	nextTaskID contracts.TaskID
	config     Config
	// To keep track of the state of each task.
	tasks map[contracts.TaskID]*task
	// The workers available.
	workers     map[WorkerAddr]*worker
	partitioner partitioning.Partitioner
	bus         contracts.MessageBus
}

type worker struct {
	state           workerState
	lastHeartbeatAt time.Time
	addr            WorkerAddr
}

type Config struct {
	// The max number of workers that should be used to run Map tasks.
	NumberOfMapWorkers uint32
	// Folder used to write partitioned input files.
	WorkspaceFolder string
	// The amount of time a worker can go without sending a heartbeat before being removed from the worker list.
	MaxWorkerHeartbeatInterval time.Duration
}

type pendingTask struct {
	partitionFilePaths []string
}

type task struct {
	id       contracts.TaskID
	taskType taskType
	// The script that the worker should execute.
	script string

	files       map[contracts.FileID]pendingFile
	outputFiles map[contracts.FileID]contracts.OutputFile
}

type pendingFile struct {
	fileID   contracts.FileID
	filePath string
	// The address of the worker that's processing the file.
	// May be empty if there's not worker assigned yet.
	workerAddr WorkerAddr
}

type Assignment struct {
	// The worker that should receive the task.
	WorkerAddr string
	// The task that should be given to the worker.
	Task contracts.MapTask
}

func New(config Config, partitioner partitioning.Partitioner, bus contracts.MessageBus) (*Master, error) {
	if config.NumberOfMapWorkers == 0 {
		return nil, fmt.Errorf("number of map workers must be greater than 0")
	}
	if config.WorkspaceFolder == "" {
		return nil, fmt.Errorf("workspace folder is required")
	}
	if config.MaxWorkerHeartbeatInterval == 0 {
		return nil, fmt.Errorf("MaxWorkerHeartbeatInterval is required")
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
			master.tryToRemoveDeadWorkers()
			time.Sleep(10 * time.Second)
		}
	}
}

func (master *Master) tryToRemoveDeadWorkers() {
	master.mu.Lock()
	removeDeadWorkers(master.config.MaxWorkerHeartbeatInterval, master.idleWorkers)
	removeDeadWorkers(master.config.MaxWorkerHeartbeatInterval, master.busyWorkers)
	master.mu.Unlock()
}

func removeDeadWorkers(maxDurationWithoutHeartbeat time.Duration, workers map[WorkerAddr]worker) {
	for _, worker := range workers {
		if time.Since(worker.lastHeartbeatAt) >= maxDurationWithoutHeartbeat {
			delete(workers, worker.addr)
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

func (master *Master) MapTasksCompletedReceived(ctx context.Context, workerAddr WorkerAddr, tasks []contracts.CompletedTask) error {
	master.mu.Lock()
	defer master.mu.Unlock()

	for _, completedTask := range tasks {
		task, ok := master.tasks[completedTask.TaskID]
		if !ok {
			continue
		}

		for _, outputFile := range completedTask.OutputFiles {
			delete(task.files, outputFile.FileID)
			task.outputFiles[outputFile.FileID] = outputFile
		}

		master.markWorkerAsIdle(workerAddr)
	}

	fmt.Printf("\n\naaaaaaa Master.MapTasksCompletedReceived tasks %+v\n\n", tasks)
	fmt.Printf("\n\naaaaaaa master.tasks %+v\n\n", master.tasks)

	master.tryAssignTasks(context.Background())

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

	files := make(map[contracts.FileID]pendingFile, len(partitionFilePaths))
	for i, filePath := range partitionFilePaths {
		fileID := contracts.FileID(i)
		files[fileID] = pendingFile{fileID: fileID, filePath: filePath}
	}

	// Create tasks that will be assigned to workers later.
	task := &task{
		id:          master.getNextTaskID(),
		taskType:    taskTypeMap,
		script:      input.value.Script,
		files:       files,
		outputFiles: make(map[contracts.FileID]contracts.OutputFile),
	}
	master.tasks[task.id] = task

	master.tryAssignTasks(ctx)

	return nil
}

// Assigns pending tasks to idle workers.
func (master *Master) tryAssignTasks(ctx context.Context) []Assignment {
	assignments := make([]Assignment, 0)

	for _, task := range master.tasks {
		// Try to find a file that's not assigned to a worker.
		for _, pendingFile := range task.files {
			if pendingFile.workerAddr != "" {
				continue
			}

			worker := master.findIdleWorker()
			if worker == nil {
				return assignments
			}

			mapTask := contracts.MapTask{
				ID:       task.id,
				Script:   task.script,
				FileID:   pendingFile.fileID,
				FilePath: pendingFile.filePath,
			}
			fmt.Printf("\n\naaaaaaa assigning map task to worker: worker=%s  taskID=%+v taskFileID=%+v taskFilePath=%+v\n\n", anyIdleWorker.addr, mapTask.ID, mapTask.FileID, mapTask.FilePath)
			if err := master.bus.AssignMapTask(ctx, worker.addr, mapTask); err != nil {
				fmt.Printf("assigning task to worker: %s\n", err)
				continue
			}

			pendingFile := task.files[pendingFile.fileID]
			pendingFile.workerAddr = worker.addr
			worker.state = workerStateBusy
			task.files[pendingFile.fileID] = pendingFile
		}
	}

	return assignments
}

func (master *Master) findIdleWorker() *worker {
	for _, worker := range master.workers {
		if worker.state == workerStateIdle {
			return worker
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
