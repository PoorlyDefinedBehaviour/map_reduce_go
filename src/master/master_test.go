package master

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/poorlydefinedbehaviour/map_reduce_go/src/clock"
	"github.com/poorlydefinedbehaviour/map_reduce_go/src/contracts"
	"github.com/poorlydefinedbehaviour/map_reduce_go/src/inmemory"
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
	m, err := New(Config{NumberOfMapWorkers: 10, WorkspaceFolder: "todo", MaxWorkerHeartbeatInterval: 10 * time.Millisecond}, nil, nil, clock)
	require.NoError(t, err)

	ctx := context.Background()

	out, err := m.OnMessage(ctx, &HeartbeatMessage{WorkerState: contracts.WorkerStateIdle, WorkerAddr: "127.0.0.1:8080"})
	require.NoError(t, err)
	assert.Empty(t, out)
}

func Test_OnMessage_CleanFailedWorkersMessage(t *testing.T) {
	t.Parallel()

	clock := clock.NewMock()
	m, err := New(Config{NumberOfMapWorkers: 10, WorkspaceFolder: "todo", MaxWorkerHeartbeatInterval: 10 * time.Millisecond}, nil, nil, clock)
	require.NoError(t, err)

	ctx := context.Background()

	out, err := m.OnMessage(ctx, CleanFailedWorkersMessage{})
	require.NoError(t, err)
	assert.Empty(t, out)
}

func Test_OnMessage_NewTaskMessage(t *testing.T) {
	t.Parallel()

	clock := clock.NewMock()

	inputFile := newTempInputFile(t, 1)
	defer inputFile.Close()

	m, err := New(Config{
		NumberOfMapWorkers:         10,
		WorkspaceFolder:            filepath.Dir(inputFile.Name()),
		MaxWorkerHeartbeatInterval: 10 * time.Millisecond,
	},
		partitioning.NewLinePartitioner(),
		nil,
		clock,
	)
	require.NoError(t, err)

	ctx := context.Background()

	input, err := NewValidatedInput(contracts.Input{
		File:                inputFile.Name(),
		NumberOfMapTasks:    3,
		NumberOfReduceTasks: 3,
		NumberOfPartitions:  3,
	})
	require.NoError(t, err)

	// Theres a new task but no workers available.
	_, err = m.OnMessage(ctx, &NewTaskMessage{Input: input})
	assert.ErrorIs(t, err, ErrNoWorkerAvailable)

	// Master receives a heartbeat and registers the worker.
	// There's 1 worker available now.
	out, err := m.OnMessage(ctx, &HeartbeatMessage{WorkerState: contracts.WorkerStateIdle, WorkerAddr: "127.0.0.1:8080"})
	require.NoError(t, err)
	assert.Empty(t, out)

	// There's a worker available so the task should be assigned to it.
	out, err = m.OnMessage(ctx, &NewTaskMessage{Input: input})
	require.NoError(t, err)

	assert.Equal(t, 1, len(out))
	assert.Equal(t, "127.0.0.1:8080", out[0].WorkerAddr)
	inProgressTask := out[0]
	fmt.Printf("\n\naaaaaaa inProgressTask %+v\n\n", inProgressTask)

	// If a new task comes in, it can't be assigned to the worker
	// because the worker is busy.
	out, err = m.OnMessage(ctx, &NewTaskMessage{Input: input})
	assert.ErrorIs(t, err, ErrNoWorkerAvailable)
	assert.Empty(t, out)

	// The worker is done processing the file assigned to it.
	out, err = m.OnMessage(ctx, &MapTasksCompletedMessage{
		WorkerAddr: "127.0.0.1:8080",
		Tasks: []contracts.CompletedTask{
			{
				TaskID: inProgressTask.Task.ID,
				OutputFiles: []contracts.OutputFile{
					{
						FileID:    inProgressTask.Task.FileID,
						FilePath:  "todo",
						SizeBytes: 10,
					}},
			},
		},
	},
	)
	require.NoError(t, err)
	assert.Empty(t, out)
}

func TestCleanUpFailedWorkers(t *testing.T) {
	t.Parallel()

	m, err := New(Config{NumberOfMapWorkers: 10, WorkspaceFolder: "todo", MaxWorkerHeartbeatInterval: 10 * time.Millisecond}, nil, nil, clock.New())
	require.NoError(t, err)

	ctx := context.Background()

	workerA := "127.0.0.1:8080"
	workerB := "127.0.0.1:8081"

	out, err := m.OnMessage(ctx, &HeartbeatMessage{WorkerState: contracts.WorkerStateIdle, WorkerAddr: workerA})
	require.NoError(t, err)
	assert.Empty(t, out)

	assert.Contains(t, m.workers, workerA)

	// Pretend MaxWorkerHeartbeatInterval is 10ms and worker A went 15ms without sending a heartbeat.
	time.Sleep(15 * time.Millisecond)

	// Worker B sends a heartbeat.
	out, err = m.OnMessage(ctx, &HeartbeatMessage{WorkerState: contracts.WorkerStateIdle, WorkerAddr: workerB})
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

func TestTryAssignTasks(t *testing.T) {
	t.Parallel()

	m, err := New(Config{
		NumberOfMapWorkers:         10,
		WorkspaceFolder:            "./tmp",
		MaxWorkerHeartbeatInterval: 10 * time.Millisecond,
	},
		partitioning.NewLinePartitioner(),
		inmemory.NewMessageBus(),
		clock.New(),
	)
	require.NoError(t, err)

	ctx := context.Background()

	out, err := m.OnMessage(ctx, &HeartbeatMessage{WorkerState: contracts.WorkerStateIdle, WorkerAddr: "127.0.0.1:8080"})
	require.NoError(t, err)
	assert.Empty(t, out)

	inputFile := newTempInputFile(t, 1)
	defer inputFile.Close()

	input, err := NewValidatedInput(contracts.Input{
		File:                inputFile.Name(),
		NumberOfMapTasks:    3,
		NumberOfReduceTasks: 3,
		NumberOfPartitions:  3,
	})
	require.NoError(t, err)
	out, err = m.onNewTask(input)
	require.NoError(t, err)
	assert.Empty(t, out)

	fmt.Printf("\n\naaaaaaa m.workers %+v\n\n", m.workers)
	// assignment, err := m.tryAssignTask(&task{id: 1, files: map[contracts.FileID]pendingFile{
	// 	1: {fileID: 1},
	// 	2: {fileID: 2},
	// 	3: {fileID: 3}},
	// },
	// )
	require.NoError(t, err)
}
