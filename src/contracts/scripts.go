package contracts

import "context"

type Input struct {
	File string
	// The folder used to write the partitions created from the input file.
	Folder              string
	NumberOfMapTasks    uint32
	NumberOfReduceTasks uint32
	NumberOfPartitions  uint32
	Map                 func(key, value string, emit func(key, value string)) error
	Reduce              func(key string, nextValueIter func() (string, bool), emit func(key, value string)) error
}

// Type responsible for translating a Go Map call to a javascript, clojure, etc call.
type Script interface {
	Map(key, value string, emit func(key, value string)) error
	// Must be called to release resources.
	Reduce(key string, nextValueIter func() (string, bool), emit func(key, value string)) error
	Close()
}

type MasterClient interface {
	// Can be used by a worker to send a heartbeat to the master.
	Heartbeat(ctx context.Context, workerAddr string) error
}

type WorkerClient interface {
	// Used by the master to let a worker know that it should execute a task.
	AssignMapTask(ctx context.Context, filePath string) error
}
