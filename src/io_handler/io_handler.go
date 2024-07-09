package io

import (
	"context"
	"fmt"
	"sync"

	"github.com/poorlydefinedbehaviour/map_reduce_go/src/contracts"

	grpcclient "github.com/poorlydefinedbehaviour/map_reduce_go/src/grpc/clients"
	"github.com/poorlydefinedbehaviour/map_reduce_go/src/master"
)

type IOHandler struct {
	master  *master.Master
	mu      *sync.Mutex
	clock   contracts.Clock
	clients map[contracts.WorkerAddr]contracts.WorkerClient
}

func NewIOHandler(ctx context.Context, master *master.Master, clock contracts.Clock) *IOHandler {
	handler := &IOHandler{
		master:  master,
		mu:      &sync.Mutex{},
		clock:   clock,
		clients: make(map[string]contracts.WorkerClient),
	}
	go handler.controlLoop(ctx)
	return handler
}

func (handler *IOHandler) controlLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			handler.mu.Lock()
			out, err := handler.master.OnMessage(ctx, master.CleanFailedWorkersMessage{})
			handler.mu.Unlock()

			if err != nil {
				// This message should never result in an error.
				panic(fmt.Errorf("unexpected error handling CleanFailedWorkersMessage: %w", err))
			}
			if len(out) > 0 {
				// This message should never result in outgoing messages.
				panic(fmt.Sprintf("unexpected output messages handling CleanFailedWorkersMessage: %+v", out))
			}

			handler.clock.Sleep(handler.master.Config.MaxWorkerHeartbeatInterval * 2)
		}
	}
}

func (handler *IOHandler) getOrCreateClient(workerAddr contracts.WorkerAddr) (contracts.WorkerClient, error) {
	// TODO: remove failed workers
	client, ok := handler.clients[workerAddr]
	if ok {
		return client, nil
	}

	client, err := grpcclient.NewWorkerClient(grpcclient.WorkerClientConfig{Addr: workerAddr})
	if err != nil {
		return nil, fmt.Errorf("instantiating worker client: %w", err)
	}

	handler.clients[workerAddr] = client

	return client, nil
}

func (handler *IOHandler) ExecuteTask(ctx context.Context, input master.ValidatedInput) ([]byte, error) {
	if err := handler.OnMessage(ctx, &master.NewTaskMessage{Input: input}); err != nil {
		return nil, fmt.Errorf("handling new task: %w", err)
	}

	return []byte("map task assigned"), nil
}

func (handler *IOHandler) OnMessage(ctx context.Context, msg master.InputMessage) error {
	handler.mu.Lock()
	defer handler.mu.Unlock()

	assignments, err := handler.master.OnMessage(ctx, msg)
	if err != nil {
		return fmt.Errorf("handling message: %T %w", msg, err)
	}

	for _, assignment := range assignments {
		client, err := handler.getOrCreateClient(assignment.WorkerAddr)
		if err != nil {
			return fmt.Errorf("getting worker client: %w", err)
		}

		if err := client.AssignTask(ctx, assignment.Task); err != nil {
			return fmt.Errorf("assigning map task to worker: %w", err)
		}
	}

	return nil
}
