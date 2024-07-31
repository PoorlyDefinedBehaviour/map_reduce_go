package master

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"time"

	"github.com/poorlydefinedbehaviour/map_reduce_go/src/contracts"
	"github.com/poorlydefinedbehaviour/map_reduce_go/src/mapsext"
	"github.com/poorlydefinedbehaviour/map_reduce_go/src/slicesext"
)

var (
	ErrNoWorkerAvailable = errors.New("no worker is available")
	ErrNoTaskAvailable   = errors.New("no task is available")
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

type OutputMessage interface {
	outputMessage() string
}

type Master struct {
	mu         *sync.Mutex
	nextTaskID contracts.TaskID
	Config     Config
	// To keep track of the state of each task.
	tasks map[contracts.TaskID]*task
	// The workers available.
	workers     map[contracts.WorkerAddr]*worker
	partitioner contracts.Partitioner
	clock       contracts.Clock
}

type worker struct {
	lastHeartbeatAt time.Time
	addr            contracts.WorkerAddr
	memoryAvailable uint64
	memoryInUse     uint64
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
	// The id of the main task. The task created when a client request came in.
	id contracts.TaskID
	// The id of this map of reduce task. The task is a subtask of the main task.
	subtaskID contracts.TaskID
	// The amount of memory the task will require in bytes.
	requestsMemory      uint64
	numberOfReduceTasks uint32
	taskType            contracts.TaskType
	// The script that the worker should execute.
	script   string
	assigned bool

	files       []pendingFile
	outputFiles map[contracts.FileID]contracts.OutputFile
}

// Returns true when the files that make up this task have been processed.
func (t *task) IsCompleted() bool {
	return len(t.outputFiles) == len(t.files)
}

type pendingFile struct {
	fileID    contracts.FileID
	filePath  string
	sizeBytes uint64
	// The address of the worker that's processing the file.
	// May be empty if there's not worker assigned yet.
	workerAddr contracts.WorkerAddr
}

func (f *pendingFile) isAssignedToWorker() bool {
	return f.workerAddr != ""
}

type TaskAssignment interface {
	GetWorkerAddr() string
}

type MapTaskAssignment struct {
	// The worker that should receive the task.
	WorkerAddr string
	// The task that should be given to the worker.
	Task contracts.MapTask
}

func (t *MapTaskAssignment) GetWorkerAddr() string {
	return t.WorkerAddr
}

type ReduceTaskAssignment struct {
	// The worker that should receive the task.
	WorkerAddr string
	// The task that should be given to the worker.
	Task contracts.ReduceTask
}

func (t *ReduceTaskAssignment) GetWorkerAddr() string {
	return t.WorkerAddr
}

func New(config Config, partitioner contracts.Partitioner, clock contracts.Clock) (*Master, error) {
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
		Config:      config,
		tasks:       make(map[contracts.TaskID]*task, 0),
		workers:     make(map[string]*worker),
		partitioner: partitioner,
		clock:       clock,
	}

	return master, nil
}

func (master *Master) cleanUpFailedWorkers() {
	for _, worker := range master.workers {
		if master.clock.Since(worker.lastHeartbeatAt) >= master.Config.MaxWorkerHeartbeatInterval {
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

func (master *Master) OnMessage(ctx context.Context, msg InputMessage) ([]TaskAssignment, error) {
	master.mu.Lock()
	defer master.mu.Unlock()

	switch msg := msg.(type) {
	case *HeartbeatMessage:
		if err := master.onHeartbeatReceived(msg); err != nil {
			return nil, fmt.Errorf("handling heartbeat message: %w", err)
		}

	case CleanFailedWorkersMessage:
		master.cleanUpFailedWorkers()

	case *NewTaskMessage:
		if err := master.onNewTask(msg.Input); err != nil {
			return nil, fmt.Errorf("handling NewTaskMessage: %w", err)
		}
		assignments, err := master.AssignTasks()
		if err != nil {
			return nil, fmt.Errorf("assigning tasks: %w", err)
		}

		return assignments, nil

	default:
		return nil, fmt.Errorf("unknown message type: %T %+v", msg, msg)
	}

	return nil, nil
}

func (master *Master) onHeartbeatReceived(msg *HeartbeatMessage) error {
	if msg.WorkerAddr == "" {
		return fmt.Errorf("worker addr is required")
	}
	if msg.MemoryAvailable == 0 {
		return fmt.Errorf("worker memory available is required")
	}
	master.workers[msg.WorkerAddr] = &worker{
		lastHeartbeatAt: master.clock.Now(),
		addr:            msg.WorkerAddr,
		memoryAvailable: msg.MemoryAvailable,
	}
	return nil
}

// Called when a new task is received. New tasks are sent by clients.
func (master *Master) onNewTask(input ValidatedInput) error {
	taskID := master.getNextTaskID()

	partitionFilePaths, err := master.partitioner.Partition(
		input.value.File,
		filepath.Join(master.Config.WorkspaceFolder, "master", fmt.Sprint(taskID), "input"),
		input.value.NumberOfPartitions,
	)
	if err != nil {
		return fmt.Errorf("partitioning input file: %w", err)
	}

	files := slicesext.MapIndex(partitionFilePaths, func(i int, path string) pendingFile {
		return pendingFile{
			fileID:   contracts.FileID(i + 1),
			filePath: path,
		}
	})

	// Create tasks that will be assigned to workers later.
	task := &task{
		id:                  taskID,
		requestsMemory:      input.value.RequestsMemory,
		taskType:            contracts.TaskTypeMap,
		script:              input.value.Script,
		numberOfReduceTasks: input.value.NumberOfReduceTasks,
		files:               files,
		outputFiles:         make(map[contracts.FileID]contracts.OutputFile),
	}

	master.tasks[task.id] = task

	return nil
}

func (master *Master) OnMapTaskCompletedReceived(workerAddr contracts.WorkerAddr, completedTask contracts.CompletedTask) {
	task, ok := master.tasks[completedTask.TaskID]
	if !ok {
		return
	}

	for _, outputFile := range completedTask.OutputFiles {
		if outputFile.FileID == 0 {
			panic(fmt.Sprintf("Invalid output file id: %+v", outputFile))
		}
		file := slicesext.Find(task.files, func(file pendingFile) bool { return file.fileID == outputFile.FileID })
		if file == nil {
			continue
		}

		worker := master.workers[workerAddr]
		worker.memoryInUse -= task.requestsMemory

		task.outputFiles[outputFile.FileID] = outputFile
	}
}

func (master *Master) assignTask() (TaskAssignment, error) {
	for _, t := range master.tasks {
		if t.IsCompleted() && t.taskType == contracts.TaskTypeMap {
			outputFiles := mapsext.Values(t.outputFiles)
			filesGroupedByRegion := slicesext.GroupBy(outputFiles, func(file contracts.OutputFile) uint32 {
				return file.Region()
			})

			for _, files := range filesGroupedByRegion {
				subtaskID := master.getNextTaskID()
				master.tasks[subtaskID] = &task{
					id:             t.id,
					subtaskID:      subtaskID,
					requestsMemory: t.requestsMemory,
					taskType:       contracts.TaskTypeReduce,
					script:         t.script,
					files: slicesext.Map(files, func(file contracts.OutputFile) pendingFile {
						return pendingFile{
							fileID:    file.FileID,
							filePath:  file.FilePath,
							sizeBytes: file.SizeBytes,
						}
					}),
					outputFiles: make(map[contracts.FileID]contracts.OutputFile),
				}
			}
		}

		if !t.IsCompleted() && t.taskType == contracts.TaskTypeReduce {
			assignment, err := master.tryAssignReduceTask(t)
			if err != nil {
				if errors.Is(err, ErrNoWorkerAvailable) {
					break
				}
				return nil, fmt.Errorf("trying to assign reduce task to a worker: %w", err)
			}

			if assignment != nil {
				return assignment, nil
			}
		}

		if !t.IsCompleted() && t.taskType == contracts.TaskTypeMap {
			assignment, err := master.tryAssignMapTask(t)
			if err != nil {
				if errors.Is(err, ErrNoWorkerAvailable) {
					break
				}
				return nil, fmt.Errorf("trying to assign map task to a worker: %w", err)
			}

			if assignment != nil {
				return assignment, nil
			}
		}
	}

	return nil, ErrNoTaskAvailable
}

func (master *Master) AssignTasks() ([]TaskAssignment, error) {
	assignments := make([]TaskAssignment, 0)

	for {
		assignment, err := master.assignTask()
		if err != nil {
			if errors.Is(err, ErrNoTaskAvailable) {
				return assignments, nil
			}
			return nil, err
		}

		assignments = append(assignments, assignment)
	}
}

// Assigns pending tasks to idle workers.
func (master *Master) tryAssignMapTask(task *task) (*MapTaskAssignment, error) {
	// Try to find a file that's not assigned to a worker.
	for i, pendingFile := range task.files {
		if pendingFile.isAssignedToWorker() {
			continue
		}

		worker := master.findWorker(task.requestsMemory)
		if worker == nil {
			return nil, ErrNoWorkerAvailable
		}

		mapTask := contracts.MapTask{
			ID:                  task.id,
			Script:              task.script,
			FileID:              pendingFile.fileID,
			FilePath:            pendingFile.filePath,
			NumberOfReduceTasks: task.numberOfReduceTasks,
		}

		pendingFile.workerAddr = worker.addr
		task.files[i] = pendingFile
		worker.memoryInUse += task.requestsMemory

		return &MapTaskAssignment{
			WorkerAddr: worker.addr,
			Task:       mapTask,
		}, nil
	}

	return nil, nil
}

func (master *Master) tryAssignReduceTask(task *task) (*ReduceTaskAssignment, error) {
	worker := master.findWorker(task.requestsMemory)
	if worker == nil {
		return nil, ErrNoWorkerAvailable
	}

	files := make([]contracts.File, 0, len(task.files))
	for _, file := range task.files {
		files = append(files, contracts.File{
			FileID:    file.fileID,
			SizeBytes: file.sizeBytes,
			Path:      file.filePath,
		})
	}

	reduceTask := contracts.ReduceTask{
		ID:     task.id,
		Script: task.script,
		Files:  files,
	}

	task.assigned = true

	worker.memoryInUse += task.requestsMemory

	return &ReduceTaskAssignment{
		WorkerAddr: worker.addr,
		Task:       reduceTask,
	}, nil
}

func (master *Master) findWorker(memoryRequest uint64) *worker {
	for _, worker := range master.workers {
		if worker.memoryAvailable-worker.memoryInUse >= memoryRequest {
			return worker
		}
	}

	return nil
}
