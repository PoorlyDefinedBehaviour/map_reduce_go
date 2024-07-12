package contracts

import (
	"errors"
)

type TaskID uint64
type TaskType = uint8
type FileID uint64

type WorkerAddr = string

var (
	ErrUnknownWorkerState = errors.New("unknown worker state")
)

const (
	TaskTypeMap    TaskType = 1 << 0
	TaskTypeReduce TaskType = 1 << 1
)

type MapTask struct {
	ID                  TaskID
	NumberOfReduceTasks uint32
	Script              string
	FileID              FileID
	FilePath            string
}

type ReduceTask struct {
	ID     TaskID
	Script string
	Files  []File
}

type File struct {
	FileID    FileID
	SizeBytes uint64
	Path      string
}
