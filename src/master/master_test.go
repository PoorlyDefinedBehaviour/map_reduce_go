package master

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/poorlydefinedbehaviour/map_reduce_go/src/clock"
	"github.com/poorlydefinedbehaviour/map_reduce_go/src/contracts"
	"github.com/poorlydefinedbehaviour/map_reduce_go/src/memory"
	"github.com/poorlydefinedbehaviour/map_reduce_go/src/partitioning"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTempInputFile(t *testing.T, numLines uint) *os.File {
	file, err := os.CreateTemp(os.TempDir(), "master_test")
	require.NoError(t, err)

	for i := range numLines {
		_, err = file.WriteString(fmt.Sprintf("line_%d\n", i))
		require.NoError(t, err)
	}

	return file
}

func Test_OnMessage_HeartbeatMessage(t *testing.T) {
	t.Parallel()

	clock := clock.NewMock()
	m, err := New(Config{NumberOfMapWorkers: 10, WorkspaceFolder: "todo", MaxWorkerHeartbeatInterval: 10 * time.Millisecond}, nil, clock)
	require.NoError(t, err)

	ctx := context.Background()

	out, err := m.OnMessage(ctx, &HeartbeatMessage{WorkerAddr: "127.0.0.1:8080"})
	require.NoError(t, err)
	assert.Empty(t, out)
}

func Test_OnMessage_CleanFailedWorkersMessage(t *testing.T) {
	t.Parallel()

	clock := clock.NewMock()
	m, err := New(Config{NumberOfMapWorkers: 10, WorkspaceFolder: "todo", MaxWorkerHeartbeatInterval: 10 * time.Millisecond}, nil, clock)
	require.NoError(t, err)

	ctx := context.Background()

	out, err := m.OnMessage(ctx, CleanFailedWorkersMessage{})
	require.NoError(t, err)
	assert.Empty(t, out)
}

func Test_OnMessage_NewTaskMessage(t *testing.T) {
	t.Parallel()

	panic("TODO")
}

func TestCleanUpFailedWorkers(t *testing.T) {
	t.Parallel()

	m, err := New(Config{NumberOfMapWorkers: 10, WorkspaceFolder: "todo", MaxWorkerHeartbeatInterval: 10 * time.Millisecond}, nil, clock.New())
	require.NoError(t, err)

	ctx := context.Background()

	workerA := "127.0.0.1:8080"
	workerB := "127.0.0.1:8081"

	out, err := m.OnMessage(ctx, &HeartbeatMessage{WorkerAddr: workerA})
	require.NoError(t, err)
	assert.Empty(t, out)

	assert.Contains(t, m.workers, workerA)

	// Pretend MaxWorkerHeartbeatInterval is 10ms and worker A went 15ms without sending a heartbeat.
	time.Sleep(15 * time.Millisecond)

	// Worker B sends a heartbeat.
	out, err = m.OnMessage(ctx, &HeartbeatMessage{WorkerAddr: workerB})
	require.NoError(t, err)
	assert.Empty(t, out)

	m.cleanUpFailedWorkers()

	// Only worker A should have been removed.
	assert.NotContains(t, m.workers, workerA)
	assert.Contains(t, m.workers, workerB)

	// Pretend worker B didn't send a heartbeat in time.
	time.Sleep(15 * time.Millisecond)

	m.cleanUpFailedWorkers()

	// Worker B should have been removed.
	assert.Empty(t, m.workers)
}

func TestTryAssignMapTasks(t *testing.T) {
	t.Parallel()

	m, err := New(Config{
		NumberOfMapWorkers:         10,
		WorkspaceFolder:            "./tmp",
		MaxWorkerHeartbeatInterval: 10 * time.Millisecond,
	},
		partitioning.NewLinePartitioner(),
		clock.New(),
	)
	require.NoError(t, err)

	ctx := context.Background()

	out, err := m.OnMessage(ctx, &HeartbeatMessage{WorkerAddr: "127.0.0.1:8080"})
	require.NoError(t, err)
	assert.Empty(t, out)

	inputFile := newTempInputFile(t, 1)
	defer inputFile.Close()

	input, err := NewValidatedInput(contracts.Input{
		File:                inputFile.Name(),
		NumberOfMapTasks:    3,
		NumberOfReduceTasks: 3,
		NumberOfPartitions:  3,
		RequestsMemory:      100 * memory.Mib,
	})
	require.NoError(t, err)
	out, err = m.onNewTask(input)
	require.NoError(t, err)
	assert.Empty(t, out)

	fmt.Printf("\n\naaaaaaa m.workers %+v\n\n", m.workers)
	// assignment, err := m.tryAssignMapTask(&task{id: 1, files: map[contracts.FileID]pendingFile{
	// 	1: {fileID: 1},
	// 	2: {fileID: 2},
	// 	3: {fileID: 3}},
	// },
	// )
	require.NoError(t, err)
}
