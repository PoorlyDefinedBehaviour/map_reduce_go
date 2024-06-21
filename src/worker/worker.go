package worker

import (
	"context"
	"fmt"
	"time"

	"github.com/poorlydefinedbehaviour/map_reduce_go/src/contracts"
)

type Worker struct {
	config       Config
	masterClient contracts.MasterClient
}

type Config struct {
	// The worker address. Used to register the worker with the master.
	Addr string
	// How long to wait for between sending heartbeat requests.
	HeartbeatInterval time.Duration
	// How long to wait for a heartbeat request to complete.
	HeartbeatTimeout time.Duration
}

func New(config Config, masterClient contracts.MasterClient) (*Worker, error) {
	if config.Addr == "" {
		return nil, fmt.Errorf("addr is required")
	}
	if config.HeartbeatInterval == 0 {
		return nil, fmt.Errorf("heartbeat interval is required")
	}
	if config.HeartbeatTimeout == 0 {
		return nil, fmt.Errorf("heartbeat timeout is required")
	}
	return &Worker{
		config:       config,
		masterClient: masterClient,
	}, nil
}

func (worker *Worker) HeartbeatControlLoop(ctx context.Context) {
	for {
		fmt.Println("sending heartbeat")
		if err := worker.sendHeartbeat(ctx); err != nil {
			fmt.Printf("sending heartbeat to master, will sleep before retrying: %s\n", err)
		}
		time.Sleep(worker.config.HeartbeatInterval)
	}
}

func (worker *Worker) sendHeartbeat(ctx context.Context) error {
	timeoutCtx, cancel := context.WithTimeout(ctx, worker.config.HeartbeatTimeout)
	defer cancel()
	if err := worker.masterClient.Heartbeat(timeoutCtx, worker.config.Addr); err != nil {
		return fmt.Errorf("sending heartbeat to master: %w", err)
	}

	return nil
}
