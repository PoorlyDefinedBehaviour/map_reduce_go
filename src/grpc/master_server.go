package grpc

import (
	"context"
	"fmt"
	"net"

	"github.com/poorlydefinedbehaviour/map_reduce_go/src/proto"

	"google.golang.org/grpc"
)

type MasterServer struct {
	proto.UnimplementedMasterServer
	config MasterServerConfig
}

type MasterServerConfig struct {
	// The port the server will listen on.
	Port uint16
}

func NewMasterServer(config MasterServerConfig) *MasterServer {
	return &MasterServer{config: config}
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
	master.HeartbeatReceive(ctx, in.WorkerAddr)
}
