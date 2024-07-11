package grpc

import (
	"context"
	"fmt"
	"net"

	"github.com/poorlydefinedbehaviour/map_reduce_go/src/contracts"
	io "github.com/poorlydefinedbehaviour/map_reduce_go/src/io_handler"
	"github.com/poorlydefinedbehaviour/map_reduce_go/src/master"
	"github.com/poorlydefinedbehaviour/map_reduce_go/src/proto"

	"google.golang.org/grpc"
)

type MasterServer struct {
	proto.UnimplementedMasterServer
	config MasterServerConfig
	master *io.MasterIOHandler
}

type MasterServerConfig struct {
	// The port the server will listen on.
	Port uint16
}

func NewMasterServer(config MasterServerConfig, master *io.MasterIOHandler) *MasterServer {
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

	if err := s.master.OnMessage(ctx, &master.HeartbeatMessage{
		WorkerAddr:      in.WorkerAddr,
		MemoryAvailable: in.MemoryAvailable,
	}); err != nil {
		return &proto.HeartbeatReply{}, fmt.Errorf("handling heartbeat: %w", err)
	}

	return &proto.HeartbeatReply{}, nil
}

func (s *MasterServer) MapTasksCompleted(ctx context.Context, in *proto.MapTasksCompletedRequest) (*proto.MapTasksCompletedReply, error) {
	tasks := make([]contracts.CompletedTask, 0, len(in.Tasks))

	for _, t := range in.Tasks {
		task := contracts.CompletedTask{TaskID: contracts.TaskID(t.TaskID)}

		for _, file := range t.OutputFiles {
			if file.FileID == 0 {
				return nil, fmt.Errorf("output file id is required")
			}
			if file.Path == "" {
				return nil, fmt.Errorf("output file path is required")
			}
			if file.SizeBytes == 0 {
				return nil, fmt.Errorf("output file size in bytes is required")
			}

			task.OutputFiles = append(task.OutputFiles, contracts.OutputFile{
				FileID:    contracts.FileID(file.FileID),
				FilePath:  file.Path,
				SizeBytes: file.SizeBytes,
			})
		}

		tasks = append(tasks, task)
	}

	if err := s.master.OnMapTasksCompletedReceived(in.WorkerAddr, tasks); err != nil {
		return &proto.MapTasksCompletedReply{}, fmt.Errorf("handling MapTasksCompletedRequest: %w", err)
	}

	return &proto.MapTasksCompletedReply{}, nil
}
