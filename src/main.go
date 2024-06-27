package main

import (
	"context"
	"fmt"
	"os"

	"github.com/poorlydefinedbehaviour/map_reduce_go/src/config"
	"github.com/poorlydefinedbehaviour/map_reduce_go/src/filestorage"

	"github.com/poorlydefinedbehaviour/map_reduce_go/src/grpc"
	"github.com/poorlydefinedbehaviour/map_reduce_go/src/httpserver"
	"github.com/poorlydefinedbehaviour/map_reduce_go/src/master"
	"github.com/poorlydefinedbehaviour/map_reduce_go/src/partitioning"
	"github.com/poorlydefinedbehaviour/map_reduce_go/src/worker"
)

func main() {
	cfg, err := config.Parse(os.Args[1:])
	if err != nil {
		panic(err)
	}

	ctx := context.Background()

	if cfg.IsWorker() {
		fmt.Println("starting worker")

		masterClient, err := grpc.NewMasterClient(grpc.MasterClientConfig{Addr: cfg.MasterAddr})
		if err != nil {
			panic(fmt.Errorf("creating grpc MasterClient: %w", err))
		}

		fileStorage := filestorage.New()

		worker, err := worker.New(worker.Config{
			WorkspaceFolder:          cfg.WorkspaceFolder,
			MaxFileSizeBytes:         cfg.WorkerMaxFileSizeBytes,
			Addr:                     fmt.Sprintf("%s:%d", cfg.WorkerHost, cfg.GrpcServerPort),
			HeartbeatInterval:        cfg.WorkerHeartbeatInterval,
			HeartbeatTimeout:         cfg.WorkerHeartbeatTimeout,
			MapTasksCompletedTimeout: cfg.WorkerMapTasksCompletedTimeout,
		}, masterClient, fileStorage)
		if err != nil {
			panic(fmt.Errorf("instantiating worker: %w", err))
		}
		go worker.HeartbeatControlLoop(ctx)

		if err := grpc.NewWorkerServer(grpc.WorkerServerConfig{Port: uint16(cfg.GrpcServerPort)}, worker).Start(); err != nil {
			panic(fmt.Errorf("starting grpc server: %w", err))
		}
	} else {
		fmt.Println("starting master")

		messageBus, err := grpc.NewMessageBus(grpc.MessageBusConfig{
			MessageBusConnectionCleanupInterval: cfg.MessageBusConnectionCleanupInterval,
		})
		if err != nil {
			panic(fmt.Errorf("instantiating message bus: %w", err))
		}

		fmt.Printf("\n\naaaaaaa cfg.MaxWorkerHeartbeatInterval %+v\n\n", cfg.MaxWorkerHeartbeatInterval)
		partitioner := partitioning.NewLinePartitioner()
		master, err := master.New(master.Config{
			WorkspaceFolder:            cfg.WorkspaceFolder,
			NumberOfMapWorkers:         3,
			MaxWorkerHeartbeatInterval: cfg.MaxWorkerHeartbeatInterval,
		},
			partitioner,
			messageBus,
		)
		if err != nil {
			panic(fmt.Errorf("instantiating master: %w", err))
		}

		go master.ControlLoop(ctx)

		go func() {
			if err := grpc.NewMasterServer(grpc.MasterServerConfig{Port: 8001}, master).Start(); err != nil {
				panic(fmt.Errorf("starting grpc server: %w", err))
			}
		}()

		httpServer := httpserver.New(master)
		if err := httpServer.Start(":8002"); err != nil {
			panic(fmt.Errorf("starting http server: %w", err))
		}
	}
}
