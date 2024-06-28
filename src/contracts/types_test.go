package contracts

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewWorkerState(t *testing.T) {
	t.Parallel()

	cases := []struct {
		input               int8
		expectedWorkerState WorkerState
		expectedErr         error
	}{
		{
			input:       0,
			expectedErr: ErrUnknownWorkerState,
		},
		{
			input:               1,
			expectedWorkerState: WorkerStateIdle,
		},
		{
			input:               2,
			expectedWorkerState: WorkerStateWorking,
		},
		{
			input:       3,
			expectedErr: ErrUnknownWorkerState,
		},
	}

	for _, tt := range cases {
		workerState, err := NewWorkerState(tt.input)
		if tt.expectedErr != nil {
			assert.ErrorIs(t, err, tt.expectedErr)
		} else {
			assert.Equal(t, tt.expectedWorkerState, workerState)
		}
	}
}
