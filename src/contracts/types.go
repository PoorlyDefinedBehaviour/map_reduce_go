package contracts

import (
	"errors"
	"fmt"
)

type TaskID uint64
type FileID uint64

type WorkerAddr = string

type WorkerState struct{ Value int8 }

var (
	WorkerStateIdle    = WorkerState{Value: 1}
	WorkerStateWorking = WorkerState{Value: 2}

	ErrUnknownWorkerState = errors.New("unknown worker state")
)

func NewWorkerState(state int8) (WorkerState, error) {
	switch state {
	case WorkerStateIdle.Value:
		return WorkerStateIdle, nil
	case WorkerStateWorking.Value:
		return WorkerStateWorking, nil
	default:
		return WorkerState{}, fmt.Errorf("%w: %d", ErrUnknownWorkerState, state)
	}
}

type MapTask struct {
	ID       TaskID
	Script   string
	FileID   FileID
	FilePath string
}
