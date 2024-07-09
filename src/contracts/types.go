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

type Task struct {
	ID       TaskID
	TaskType TaskType
	Script   string
	FileID   FileID
	FilePath string
}
