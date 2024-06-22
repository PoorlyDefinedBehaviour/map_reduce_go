package contracts

import "context"

type CompletedTask struct {
	TaskID      TaskID
	OutputFiles []OutputFile
}

type OutputFile struct {
	Path      string
	SizeBytes uint64
}

type MasterClient interface {
	// Can be used by a worker to send a heartbeat to the master.
	Heartbeat(ctx context.Context, workerState WorkerState, workerAddr string) error
	// Can be used by a worker to let the master know that a set of tasks have been completed.
	MapTasksCompleted(ctx context.Context, tasks []CompletedTask) error
}

type WorkerClient interface {
	// Used by the master to let a worker know that it should execute a task.
	AssignMapTask(ctx context.Context, filePath string) error
}
