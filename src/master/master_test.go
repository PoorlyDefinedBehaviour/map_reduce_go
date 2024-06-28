package master

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/poorlydefinedbehaviour/map_reduce_go/src/contracts"
	"github.com/poorlydefinedbehaviour/map_reduce_go/src/inmemory"
	"github.com/poorlydefinedbehaviour/map_reduce_go/src/partitioning"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTempInputFile(t *testing.T) *os.File {
	file, err := os.CreateTemp(os.TempDir(), "master_test")
	require.NoError(t, err)

	_, err = file.WriteString("a b c d e")
	require.NoError(t, err)

	return file
}

func TestTryRemoveDeadWorkers(t *testing.T) {
	t.Parallel()

	m, err := New(Config{NumberOfMapWorkers: 10, WorkspaceFolder: "todo", MaxWorkerHeartbeatInterval: 10 * time.Millisecond}, nil, nil)
	require.NoError(t, err)

	ctx := context.Background()

	workerA := "127.0.0.1:8080"
	workerB := "127.0.0.1:8081"

	m.HeartbeatReceived(ctx, contracts.WorkerStateIdle, workerA)

	assert.Contains(t, m.workers, workerA)

	// Pretend MaxWorkerHeartbeatInterval is 10ms and worker A went 15ms without sending a heartbeat.
	time.Sleep(15 * time.Millisecond)

	// Worker B sends a heartbeat.
	m.HeartbeatReceived(ctx, contracts.WorkerStateIdle, workerB)

	m.tryToRemoveDeadWorkers()

	// Only worker A should have been removed.
	assert.NotContains(t, m.workers, workerA)
	assert.Contains(t, m.workers, workerB)

	// Pretend worker B didn't send a heartbeat in time.
	time.Sleep(15 * time.Millisecond)

	m.tryToRemoveDeadWorkers()

	// Worker B should have been removed.
	assert.Empty(t, m.workers)
}

func TestTryAssignTasks(t *testing.T) {
	t.Parallel()

	m, err := New(Config{
		NumberOfMapWorkers:         10,
		WorkspaceFolder:            "./tmp",
		MaxWorkerHeartbeatInterval: 10 * time.Millisecond,
	},
		partitioning.NewLinePartitioner(),
		inmemory.NewMessageBus(),
	)
	require.NoError(t, err)

	ctx := context.Background()

	m.HeartbeatReceived(ctx, contracts.WorkerStateIdle, "127.0.0.1:8080")

	inputFile := newTempInputFile(t)
	defer inputFile.Close()

	input, err := NewValidatedInput(contracts.Input{
		File:                inputFile.Name(),
		NumberOfMapTasks:    3,
		NumberOfReduceTasks: 3,
		NumberOfPartitions:  3,
	})
	require.NoError(t, err)
	require.NoError(t, m.OnNewTask(ctx, input))

	fmt.Printf("\n\naaaaaaa m.workers %+v\n\n", m.workers)
	assignment, err := m.tryAssignTask(&task{id: 1, files: map[contracts.FileID]pendingFile{
		1: {fileID: 1},
		2: {fileID: 2},
		3: {fileID: 3}},
	},
	)
	require.NoError(t, err)

	fmt.Printf("\n\naaaaaaa assignment %+v\n\n", assignment)

	require.NoError(t, m.handleAssignedTask(ctx, assignment))
}
