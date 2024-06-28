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

var (
	ErrNoIdleWorkerAvailable = errors.New("no idle worker available to pick up task")
)

type Message interface{}

type NewTaskMessage struct {
	input ValidatedInput
}

type MapTasksCompleted struct {
	workerAddr contracts.WorkerAddr
	tasks      []contracts.CompletedTask
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
	bus         contracts.MessageBus
}

type worker struct {
	state           contracts.WorkerState
	lastHeartbeatAt time.Time
	addr            contracts.WorkerAddr
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
	workerAddr contracts.WorkerAddr
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
		workers:     make(map[string]*worker),
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
			time.Sleep(master.config.MaxWorkerHeartbeatInterval * 2)
		}
	}
}

func (master *Master) tryToRemoveDeadWorkers() {
	master.mu.Lock()
	for _, worker := range master.workers {
		if time.Since(worker.lastHeartbeatAt) >= master.config.MaxWorkerHeartbeatInterval {
			delete(master.workers, worker.addr)
		}
	}
	master.mu.Unlock()
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

func (master *Master) HeartbeatReceived(ctx context.Context, workerState contracts.WorkerState, workerAddr string) {
	master.mu.Lock()
	defer master.mu.Unlock()

	master.workers[workerAddr] = &worker{lastHeartbeatAt: time.Now(), addr: workerAddr, state: workerState}
}

func (master *Master) OnMapTasksCompletedReceived(ctx context.Context, workerAddr contracts.WorkerAddr, tasks []contracts.CompletedTask) error {
	fmt.Printf("\n\naaaaaaa OnMapTasksCompletedReceived worker=%+v %+v\n\n", workerAddr, tasks)
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

		fmt.Printf("\n\naaaaaaa task %+v\n\n", task)
	}

	// master.tryAssignTasks(context.Background())

	return nil
}

func partitionFilePathsToPendingFiles(partitionFilePaths []string) map[contracts.FileID]pendingFile {
	files := make(map[contracts.FileID]pendingFile, len(partitionFilePaths))
	for i, filePath := range partitionFilePaths {
		fileID := contracts.FileID(i)
		files[fileID] = pendingFile{fileID: fileID, filePath: filePath}
	}
	return files
}

// Called when a new task is received. New tasks are sent by clients.
func (master *Master) OnNewTask(ctx context.Context, input ValidatedInput) error {
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

	files := partitionFilePathsToPendingFiles(partitionFilePaths)

	// Create tasks that will be assigned to workers later.
	task := &task{
		id:          master.getNextTaskID(),
		taskType:    taskTypeMap,
		script:      input.value.Script,
		files:       files,
		outputFiles: make(map[contracts.FileID]contracts.OutputFile),
	}
	master.tasks[task.id] = task

	// master.tryAssignTasks(ctx)

	return nil
}

// Assigns pending tasks to idle workers.
func (master *Master) tryAssignTask(task *task) (*Assignment, error) {
	// Try to find a file that's not assigned to a worker.
	for _, pendingFile := range task.files {
		if pendingFile.workerAddr != "" {
			continue
		}

		worker := master.findIdleWorker()
		fmt.Printf("\n\naaaaaaa worker %+v\n\n", worker)
		if worker == nil {
			return nil, ErrNoIdleWorkerAvailable
		}

		mapTask := contracts.MapTask{
			ID:       task.id,
			Script:   task.script,
			FileID:   pendingFile.fileID,
			FilePath: pendingFile.filePath,
		}

		return &Assignment{
			WorkerAddr: worker.addr,
			Task:       mapTask,
		}, nil

		// // fmt.Printf("\n\naaaaaaa assigning task=%d file=%s to worker %+v\n\n", task.id, task.files, worker.addr)
		// if err := master.bus.AssignMapTask(ctx, worker.addr, mapTask); err != nil {
		// 	fmt.Printf("assigning task to worker: %s\n", err)
		// 	continue
		// }

		// pendingFile := task.files[pendingFile.fileID]
		// pendingFile.workerAddr = worker.addr
		// task.files[pendingFile.fileID] = pendingFile
		// worker.state = contracts.WorkerStateWorking
	}

	return nil, nil
}

func (master *Master) handleAssignedTask(ctx context.Context, assignment *Assignment) error {
	if err := master.bus.AssignMapTask(ctx, assignment.WorkerAddr, assignment.Task); err != nil {
		return fmt.Errorf("sending message assigning task to worker: %w", err)
	}

	pendingFile := master.tasks[assignment.Task.ID].files[assignment.Task.FileID]
	pendingFile.workerAddr = assignment.WorkerAddr
	master.tasks[assignment.Task.ID].files[pendingFile.fileID] = pendingFile
	master.workers[assignment.WorkerAddr].state = contracts.WorkerStateWorking

	return nil
}

func (master *Master) findIdleWorker() *worker {
	for _, worker := range master.workers {
		if worker.state == contracts.WorkerStateIdle {
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
