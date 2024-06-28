package grpc

import (
	"context"
	"fmt"
	"net"

	"github.com/poorlydefinedbehaviour/map_reduce_go/src/contracts"
	"github.com/poorlydefinedbehaviour/map_reduce_go/src/proto"
	"github.com/poorlydefinedbehaviour/map_reduce_go/src/worker"

	"google.golang.org/grpc"
)

type WorkerServer struct {
	proto.UnimplementedWorkerServer
	config WorkerServerConfig
	worker *worker.Worker
}

type WorkerServerConfig struct {
	// The port the server will listen on.
	Port uint16
}

func NewWorkerServer(config WorkerServerConfig, worker *worker.Worker) *WorkerServer {
	return &WorkerServer{config: config, worker: worker}
}

func (s *WorkerServer) Start() error {
	fmt.Printf("starting grpc server: addr=:%d\n", s.config.Port)
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", s.config.Port))
	if err != nil {
		return fmt.Errorf("listening on port %d: %w", s.config.Port, err)
	}
	grpcServer := grpc.NewServer()
	proto.RegisterWorkerServer(grpcServer, s)
	if err := grpcServer.Serve(listener); err != nil {
		return fmt.Errorf("serving listener: %w", err)
	}
	return nil
}

func (s *WorkerServer) AssignMapTask(ctx context.Context, in *proto.AssignMapTaskRequest) (*proto.AssignMapTaskRequestReply, error) {
	if err := s.worker.OnMapTaskReceived(ctx, contracts.MapTask{
		ID:       contracts.TaskID(in.TaskID),
		Script:   in.Script,
		FileID:   contracts.FileID(in.FileID),
		FilePath: in.FilePath,
	}); err != nil {
		return &proto.AssignMapTaskRequestReply{}, fmt.Errorf("handling new task assignment: %w", err)
	}
	return &proto.AssignMapTaskRequestReply{}, nil
}
