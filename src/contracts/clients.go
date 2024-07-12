package contracts

import (
	"context"
	"path/filepath"
	"strconv"
	"strings"
)

type CompletedTask struct {
	TaskID      TaskID
	OutputFiles []OutputFile
}

type OutputFile struct {
	FileID    FileID
	FilePath  string
	SizeBytes uint64
}

// Returns the region the file belongs to. Regions are set in the Map phase.
func (f *OutputFile) Region() uint32 {
	name := filepath.Base(f.FilePath)
	_, region, _ := strings.Cut(name, "_")
	n, err := strconv.ParseInt(region, 10, 32)
	if err != nil {
		panic(err)
	}
	return uint32(n)
}

type MasterClient interface {
	// Can be used by a worker to send a heartbeat to the master.
	Heartbeat(ctx context.Context, workerAddr string, memoryAvailable uint64) error
	// Can be used by a worker to let the master know that a set of tasks have been completed.
	MapTasksCompleted(ctx context.Context, workerAddr string, tasks []CompletedTask) error
}

type WorkerClient interface {
	// Used by the master to let a worker know that it should execute a task.
	AssignMapTask(ctx context.Context, task MapTask) error
	AssignReduceTask(ctx context.Context, task ReduceTask) error
}
