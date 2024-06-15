package master

import (
	"context"
	"fmt"

	"github.com/poorlydefinedbehaviour/map_reduce_go/src/proto"
)

// Represents a worker node.
// Makes it easier to interact with the worker by wrapping the grpc client.
type Worker struct {
	ID     WorkerID
	Client proto.MapReduceNodeClient
}

func NewWorker() *Worker {
	panic("todo")
}

func (worker *Worker) AssignMapTask(ctx context.Context, filePath string) error {
	_, err := worker.Client.AssignMapTask(ctx, &proto.AssignMapTaskRequest{
		FilePath: filePath,
	})
	if err != nil {
		return fmt.Errorf("sending request to assign map task to worker: workerID=%d %w", worker.ID, err)
	}

	return nil
}
