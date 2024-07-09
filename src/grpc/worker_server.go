package grpc

import (
	"context"
	"fmt"
	"net"

	"github.com/poorlydefinedbehaviour/map_reduce_go/src/contracts"
	"github.com/poorlydefinedbehaviour/map_reduce_go/src/proto"
	"github.com/poorlydefinedbehaviour/map_reduce_go/src/tracing"
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

func (s *WorkerServer) AssignTask(ctx context.Context, in *proto.AssignTaskRequest) (*proto.AssignTaskRequestReply, error) {
	go func() {
		ctx := context.Background()

		if err := s.worker.OnMapTaskReceived(ctx, contracts.Task{
			ID:       contracts.TaskID(in.TaskID),
			TaskType: contracts.TaskType(in.TaskType),
			Script:   in.Script,
			FileID:   contracts.FileID(in.FileID),
			FilePath: in.FilePath,
		}); err != nil {
			tracing.Error(context.Background(), "handling new task assignment", "err", err)
		}
	}()
	return &proto.AssignTaskRequestReply{}, nil
}
