package grpc

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/poorlydefinedbehaviour/map_reduce_go/src/contracts"
	"google.golang.org/grpc/connectivity"
)

type MessageBus struct {
	mu            *sync.Mutex
	config        MessageBusConfig
	workerClients map[string]*WorkerClient
}

type MessageBusConfig struct {
	// How long to wait for between checking if any of the client connections have disconnected.
	MessageBusConnectionCleanupInterval time.Duration
}

func NewMessageBus(config MessageBusConfig) (*MessageBus, error) {
	if config.MessageBusConnectionCleanupInterval == 0 {
		return nil, fmt.Errorf("CleanupDisconnectedClientsInterval is required")
	}

	bus := &MessageBus{mu: &sync.Mutex{}, config: config, workerClients: make(map[string]*WorkerClient)}
	go bus.controlLoop(context.Background())
	return bus, nil
}

func (bus *MessageBus) controlLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			bus.mu.Lock()
			for workerAddr, client := range bus.workerClients {

				if client.conn.GetState() == connectivity.Shutdown {
					if err := client.Close(); err != nil {
						fmt.Printf("closing grpc client: %s\n", err)
					}
					delete(bus.workerClients, workerAddr)
				}
			}
			bus.mu.Unlock()
			time.Sleep(bus.config.MessageBusConnectionCleanupInterval)
		}
	}
}

func (bus *MessageBus) getOrCreateClient(workerAddr string) (*WorkerClient, error) {
	bus.mu.Lock()
	defer bus.mu.Unlock()

	client, ok := bus.workerClients[workerAddr]
	if ok {
		return client, nil
	}

	client, err := NewWorkerClient(WorkerClientConfig{Addr: workerAddr})
	if err != nil {
		return client, fmt.Errorf("connecting to worker: addr=%s %w", workerAddr, err)
	}

	bus.workerClients[workerAddr] = client

	return client, nil
}

func (bus *MessageBus) AssignMapTask(ctx context.Context, workerAddr string, taskID contracts.TaskID, script, filePath string) error {
	client, err := bus.getOrCreateClient(workerAddr)
	if err != nil {
		return fmt.Errorf("getting/creating client: workerAdr=%s %w", workerAddr, err)
	}

	if err := client.AssignMapTask(ctx, taskID, script, filePath); err != nil {
		return fmt.Errorf("sending request to assign Map task to worker: workerAddr=%s %w", workerAddr, err)
	}

	return nil
}
