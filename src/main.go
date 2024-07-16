package main

import (
	"context"
	"fmt"
	"os"

	"github.com/poorlydefinedbehaviour/map_reduce_go/src/clock"
	"github.com/poorlydefinedbehaviour/map_reduce_go/src/config"
	"github.com/poorlydefinedbehaviour/map_reduce_go/src/filestorage"
	grpcclient "github.com/poorlydefinedbehaviour/map_reduce_go/src/grpc/clients"
	io "github.com/poorlydefinedbehaviour/map_reduce_go/src/io_handler"
	"github.com/poorlydefinedbehaviour/map_reduce_go/src/sorter"
	"github.com/poorlydefinedbehaviour/map_reduce_go/src/tracing"

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
	clock := clock.New()
	partitioner := partitioning.NewLinePartitioner()

	if cfg.IsWorker() {
		tracing.Info(ctx, "starting worker")

		masterClient, err := grpcclient.NewMasterClient(grpcclient.MasterClientConfig{Addr: cfg.MasterAddr})
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
			MemoryAvailable:          cfg.WorkerMemoryAvailable,
		}, masterClient, fileStorage, clock, partitioner, sorter.NewLineSorter())
		if err != nil {
			panic(fmt.Errorf("instantiating worker: %w", err))
		}
		go worker.HeartbeatControlLoop(ctx)

		if err := grpc.NewWorkerServer(grpc.WorkerServerConfig{Port: uint16(cfg.GrpcServerPort)}, worker).Start(); err != nil {
			panic(fmt.Errorf("starting grpc server: %w", err))
		}
	} else {
		tracing.Info(ctx, "starting master")

		m, err := master.New(master.Config{
			WorkspaceFolder:            cfg.WorkspaceFolder,
			NumberOfMapWorkers:         3,
			MaxWorkerHeartbeatInterval: cfg.MaxWorkerHeartbeatInterval,
		},
			partitioner,
			clock,
		)
		if err != nil {
			panic(fmt.Errorf("instantiating master: %w", err))
		}

		masterIO := io.NewMasterIOHandler(ctx, m, clock)

		go func() {
			if err := grpc.NewMasterServer(grpc.MasterServerConfig{Port: 8001}, masterIO).Start(); err != nil {
				panic(fmt.Errorf("starting grpc server: %w", err))
			}
		}()

		httpServer := httpserver.New(masterIO)
		if err := httpServer.Start(":8002"); err != nil {
			panic(fmt.Errorf("starting http server: %w", err))
		}
	}
}
