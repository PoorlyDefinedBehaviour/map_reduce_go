package master

import (
	"context"
	"errors"
	"fmt"
	"hash/maphash"
	"sync"
	"time"

	"github.com/poorlydefinedbehaviour/map_reduce_go/src/contracts"
	"github.com/poorlydefinedbehaviour/map_reduce_go/src/partitioning"
)

var ErrNoWorkerAvailable = errors.New("no worker is available")

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

type InputMessage interface {
	inputMessage() string
}

type NewTaskMessage struct {
	Input ValidatedInput
}

func (*NewTaskMessage) inputMessage() string {
	return "NewTaskMessage"
}

type HeartbeatMessage struct {
	WorkerState     contracts.WorkerState
	WorkerAddr      contracts.WorkerAddr
	MemoryAvailable uint64
}

func (*HeartbeatMessage) inputMessage() string {
	return "HeartbeatMessage"
}

type CleanFailedWorkersMessage struct {
}

func (CleanFailedWorkersMessage) inputMessage() string {
	return "CleanFailedWorkersMessage"
}

type MapTasksCompletedMessage struct {
	WorkerAddr contracts.WorkerAddr
	Tasks      []contracts.CompletedTask
}

func (MapTasksCompletedMessage) inputMessage() string {
	return "MapTasksCompletedMessage"
}

type OutputMessage interface {
	outputMessage() string
}

type Master struct {
	mu         *sync.Mutex
	nextTaskID contracts.TaskID
	config     Config
	// To keep track of the state of each task.
	tasks map[contracts.TaskID]*task
	// The workers available.
	workers     map[contracts.WorkerAddr]*worker
	partitioner partitioning.Partitioner
	clock       contracts.Clock
}

type worker struct {
	state           contracts.WorkerState
	lastHeartbeatAt time.Time
	addr            contracts.WorkerAddr
	memoryAvailable uint64
}

type Config struct {
	// The max number of workers that should be used to run Map tasks.
	NumberOfMapWorkers uint32
	// Folder used to write partitioned input files.
	WorkspaceFolder string
	// The amount of time a worker can go without sending a heartbeat before being removed from the worker list.
	MaxWorkerHeartbeatInterval time.Duration
}

type task struct {
	id contracts.TaskID
	// The amount of memory the task will require in bytes.
	requestsMemory uint64
	taskType       taskType
	// The script that the worker should execute.
	script string

	files       map[contracts.FileID]pendingFile
	outputFiles map[contracts.FileID]contracts.OutputFile
}

// Returns true when the files that make up this task have been processed.
func (t *task) IsCompleted() bool {
	return len(t.files) == 0
}

type pendingFile struct {
	fileID    contracts.FileID
	filePath  string
	sizeBytes uint64
	// The address of the worker that's processing the file.
	// May be empty if there's not worker assigned yet.
	workerAddr contracts.WorkerAddr
}

type Assignment struct {
	// The worker that should receive the task.
	WorkerAddr string
	// The task that should be given to the worker.
	Task contracts.MapTask
}

func New(config Config, partitioner partitioning.Partitioner, bus contracts.MessageBus, clock contracts.Clock) (*Master, error) {
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
		workers:     make(map[string]*worker),
		partitioner: partitioner,
		clock:       clock,
	}

	return master, nil
}

func (master *Master) controlLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			out, err := master.OnMessage(ctx, CleanFailedWorkersMessage{})
			if err != nil {
				// This message should never result in an error.
				panic(fmt.Errorf("unexpected error handling CleanFailedWorkersMessage: %w", err))
			}
			if len(out) > 0 {
				// This message should never result in outgoing messages.
				panic(fmt.Sprintf("unexpected output messages handling CleanFailedWorkersMessage: %+v", out))
			}
			master.clock.Sleep(master.config.MaxWorkerHeartbeatInterval * 2)
		}
	}
}

func (master *Master) cleanUpFailedWorkers() {
	for _, worker := range master.workers {
		if master.clock.Since(worker.lastHeartbeatAt) >= master.config.MaxWorkerHeartbeatInterval {
			delete(master.workers, worker.addr)
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
	if input.RequestsMemory == 0 {
		return ValidatedInput{}, fmt.Errorf("memory request cannot be 0")
	}

	return ValidatedInput{value: input}, nil
}

func (master *Master) OnMessage(ctx context.Context, msg InputMessage) ([]Assignment, error) {
	master.mu.Lock()
	defer master.mu.Unlock()

	switch msg := msg.(type) {
	case *HeartbeatMessage:
		master.onHeartbeatReceived(msg)

	case CleanFailedWorkersMessage:
		master.cleanUpFailedWorkers()

	case *NewTaskMessage:
		assignments, err := master.onNewTask(msg.Input)
		if err != nil {
			return assignments, fmt.Errorf("handling NewTaskMessage: %w", err)
		}
		return assignments, nil

	case *MapTasksCompletedMessage:
		assignments, err := master.onMapTasksCompletedReceived(msg.WorkerAddr, msg.Tasks)
		if err != nil {
			return nil, fmt.Errorf("handling MapTasksCompletedMessage: %w", err)
		}
		return assignments, nil

	default:
		return nil, fmt.Errorf("unknown message type: %T %+v", msg, msg)
	}

	return nil, nil
}

func (master *Master) onHeartbeatReceived(msg *HeartbeatMessage) {
	master.workers[msg.WorkerAddr] = &worker{
		lastHeartbeatAt: master.clock.Now(),
		addr:            msg.WorkerAddr,
		state:           msg.WorkerState,
		memoryAvailable: msg.MemoryAvailable,
	}
}

// Called when a new task is received. New tasks are sent by clients.
func (master *Master) onNewTask(input ValidatedInput) ([]Assignment, error) {
	partitionFilePaths, err := master.partitioner.Partition(
		input.value.File,
		master.config.WorkspaceFolder,
		input.value.NumberOfPartitions,
	)
	if err != nil {
		return nil, fmt.Errorf("partitioning input file: %w", err)
	}

	files := partitionFilePathsToPendingFiles(partitionFilePaths)

	// Create tasks that will be assigned to workers later.
	task := &task{
		id:             master.getNextTaskID(),
		requestsMemory: input.value.RequestsMemory,
		taskType:       taskTypeMap,
		script:         input.value.Script,
		files:          files,
		outputFiles:    make(map[contracts.FileID]contracts.OutputFile),
	}

	assignment, err := master.tryAssignTask(task)
	if err != nil {
		return nil, fmt.Errorf("trying to assign task to a worker: %w", err)
	}

	master.tasks[task.id] = task

	return []Assignment{*assignment}, nil
}

func (master *Master) onMapTasksCompletedReceived(workerAddr contracts.WorkerAddr, tasks []contracts.CompletedTask) ([]Assignment, error) {
	for _, completedTask := range tasks {
		task, ok := master.tasks[completedTask.TaskID]
		if !ok {
			continue
		}

		for _, outputFile := range completedTask.OutputFiles {
			delete(task.files, outputFile.FileID)
			task.outputFiles[outputFile.FileID] = outputFile
		}

		if worker, ok := master.workers[workerAddr]; ok {
			worker.state = contracts.WorkerStateIdle
		}
	}

	for _, t := range master.tasks {
		if t.IsCompleted() && t.taskType == taskTypeReduce {
			files := make(map[contracts.FileID]pendingFile)
			for _, file := range t.outputFiles {
				files[file.FileID] = pendingFile{
					fileID:    file.FileID,
					filePath:  file.FilePath,
					sizeBytes: file.SizeBytes,
				}
			}
			master.tasks[t.id] = &task{
				id:             t.id,
				requestsMemory: t.requestsMemory,
				taskType:       taskTypeReduce,
				script:         t.script,
				files:          files,
				outputFiles:    make(map[contracts.FileID]contracts.OutputFile),
			}
		}
	}

	for _, task := range master.tasks {
		if !task.IsCompleted() {
			assignment, err := master.tryAssignTask(task)
			if err != nil {
				return nil, fmt.Errorf("trying to assign task to a worker: %w", err)
			}

			return []Assignment{*assignment}, nil
		} else {

		}
	}

	return nil, nil
}

func partitionFilePathsToPendingFiles(partitionFilePaths []string) map[contracts.FileID]pendingFile {
	files := make(map[contracts.FileID]pendingFile, len(partitionFilePaths))
	for i, filePath := range partitionFilePaths {
		fileID := contracts.FileID(i)
		files[fileID] = pendingFile{fileID: fileID, filePath: filePath}
	}
	return files
}

// Assigns pending tasks to idle workers.
func (master *Master) tryAssignTask(task *task) (*Assignment, error) {
	// Try to find a file that's not assigned to a worker.
	for _, pendingFile := range task.files {
		if pendingFile.workerAddr != "" {
			continue
		}

		worker := master.findWorker(task.requestsMemory)
		if worker == nil {
			return nil, ErrNoWorkerAvailable
		}

		mapTask := contracts.MapTask{
			ID:       task.id,
			Script:   task.script,
			FileID:   pendingFile.fileID,
			FilePath: pendingFile.filePath,
		}

		pendingFile := task.files[pendingFile.fileID]
		pendingFile.workerAddr = worker.addr
		task.files[pendingFile.fileID] = pendingFile
		worker.state = contracts.WorkerStateWorking

		return &Assignment{
			WorkerAddr: worker.addr,
			Task:       mapTask,
		}, nil
	}

	return nil, nil
}

func (master *Master) findWorker(memoryRequest uint64) *worker {
	for _, worker := range master.workers {
		if worker.state == contracts.WorkerStateIdle && worker.memoryAvailable >= memoryRequest {
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
