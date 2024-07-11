package contracts

type Input struct {
	File                string
	Script              string
	NumberOfMapTasks    uint32
	NumberOfReduceTasks uint32
	NumberOfPartitions  uint32
	RequestsMemory      uint64
}

// Type responsible for translating a Go Map call to a javascript, clojure, etc call.
type Script interface {
	// Executes the user provided partition function.
	Partition(key string, numberOfReduceTasks uint32) uint32
	// Executes the user provided map function.
	Map(key, value string, emit func(key, value string)) error
	// Executes the user provided reduce function.
	Reduce(key string, nextValueIter func() (string, bool), emit func(key, value string)) error
	// Must be called to release resources.
	Close()
}
