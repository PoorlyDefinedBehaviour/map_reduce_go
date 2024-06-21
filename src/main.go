package main

import (
	"context"
	"fmt"
	"os"

	"github.com/poorlydefinedbehaviour/map_reduce_go/src/config"
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

	if cfg.IsWorker() {
		fmt.Println("starting worker")

		masterClient, err := grpc.NewMasterClient(grpc.MasterClientConfig{Addr: cfg.MasterAddr})
		if err != nil {
			panic(fmt.Errorf("creating grpc MasterClient: %w", err))
		}

		worker, err := worker.New(worker.Config{
			Addr:              fmt.Sprintf("%s/%d", cfg.WorkerHost, cfg.GrpcServerPort),
			HeartbeatInterval: cfg.WorkerHeartbeatInterval,
			HeartbeatTimeout:  cfg.WorkerHeartbeatTimeout,
		}, masterClient)
		if err != nil {
			panic(fmt.Errorf("instantiating worker: %w", err))
		}
		go worker.HeartbeatControlLoop(context.Background())

		if err := grpc.NewWorkerServer(grpc.WorkerServerConfig{Port: 8001}, worker).Start(); err != nil {
			panic(fmt.Errorf("starting grpc server: %w", err))
		}
	} else {
		fmt.Println("starting master")

		partitioner := partitioning.NewLinePartitioner()
		master, err := master.New(master.Config{NumberOfMapWorkers: 3}, partitioner)
		if err != nil {
			panic(fmt.Errorf("instantiating master: %w", err))
		}

		go func() {
			if err := grpc.NewMasterServer(grpc.MasterServerConfig{Port: 8001}).Start(); err != nil {
				panic(fmt.Errorf("starting grpc server: %w", err))
			}
		}()

		httpServer := httpserver.New(master)
		if err := httpServer.Start(":8002"); err != nil {
			panic(fmt.Errorf("starting http server: %w", err))
		}
	}
}
