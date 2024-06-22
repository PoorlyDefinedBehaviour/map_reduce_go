package grpc

import (
	"context"
	"fmt"
	"net"

	"github.com/poorlydefinedbehaviour/map_reduce_go/src/contracts"
	"github.com/poorlydefinedbehaviour/map_reduce_go/src/master"
	"github.com/poorlydefinedbehaviour/map_reduce_go/src/proto"

	"google.golang.org/grpc"
)

type MasterServer struct {
	proto.UnimplementedMasterServer
	config MasterServerConfig
	master *master.Master
}

type MasterServerConfig struct {
	// The port the server will listen on.
	Port uint16
}

func NewMasterServer(config MasterServerConfig, master *master.Master) *MasterServer {
	return &MasterServer{config: config, master: master}
}

func (s *MasterServer) Start() error {
	fmt.Printf("starting grpc server: addr=:%d\n", s.config.Port)
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", s.config.Port))
	if err != nil {
		return fmt.Errorf("listening on port %d: %w", s.config.Port, err)
	}
	grpcServer := grpc.NewServer()
	proto.RegisterMasterServer(grpcServer, s)
	if err := grpcServer.Serve(listener); err != nil {
		return fmt.Errorf("serving listener: %w", err)
	}
	return nil
}

func (s *MasterServer) Heartbeat(ctx context.Context, in *proto.HeartbeatRequest) (*proto.HeartbeatReply, error) {
	if err := s.master.HeartbeatReceived(ctx, contracts.WorkerState(in.State), in.WorkerAddr); err != nil {
		return &proto.HeartbeatReply{}, fmt.Errorf("handling heartbeat: %w", err)
	}

	return &proto.HeartbeatReply{}, nil
}

func (s *MasterServer) MapTasksCompleted(ctx context.Context, in *proto.MapTasksCompletedRequest) (*proto.MapTasksCompletedReply, error) {
	tasks := make([]contracts.CompletedTask, 0, len(in.Tasks))

	for _, t := range in.Tasks {
		task := contracts.CompletedTask{TaskID: contracts.TaskID(t.TaskID)}

		for _, file := range t.OutputFiles {
			task.OutputFiles = append(task.OutputFiles, contracts.OutputFile{Path: file.Path, SizeBytes: file.SizeBytes})
		}
	}

	if err := s.master.MapTasksCompletedReceived(ctx, tasks); err != nil {
		return &proto.MapTasksCompletedReply{}, fmt.Errorf("handling MapTasksCompletedRequest: %w", err)
	}

	return &proto.MapTasksCompletedReply{}, nil
}
