package master

import (
	"context"
	"testing"
	"time"

	"github.com/poorlydefinedbehaviour/map_reduce_go/src/constants"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTryRemoveDeadWorkers(t *testing.T) {
	t.Parallel()

	m, err := New(Config{NumberOfMapWorkers: 10, WorkspaceFolder: "todo", MaxWorkerHeartbeatInterval: 10 * time.Millisecond}, nil, nil)
	require.NoError(t, err)

	ctx := context.Background()

	workerA := "127.0.0.1:8080"
	workerB := "127.0.0.1:8081"

	require.NoError(t, m.HeartbeatReceived(ctx, constants.WorkerStateIdle, workerA))

	assert.Contains(t, m.idleWorkers, workerA)

	// Pretend MaxWorkerHeartbeatInterval is 10ms and worker A went 15ms without sending a heartbeat.
	time.Sleep(15 * time.Millisecond)

	// Worker B sends a heartbeat.
	require.NoError(t, m.HeartbeatReceived(ctx, constants.WorkerStateIdle, workerB))

	m.tryToRemoveDeadWorkers()

	// Only worker A should have been removed.
	assert.NotContains(t, m.idleWorkers, workerA)
	assert.Contains(t, m.idleWorkers, workerB)

	// Pretend worker B didn't send a heartbeat in time.
	time.Sleep(15 * time.Millisecond)

	m.tryToRemoveDeadWorkers()

	// Worker B should have been removed.
	assert.Empty(t, m.idleWorkers)
}
