package grpc

import (
	"context"
	"fmt"
	"net"

	"github.com/poorlydefinedbehaviour/map_reduce_go/src/proto"

	"google.golang.org/grpc"
)

type Server struct {
	proto.UnimplementedGreeterServer
	config ServerConfig
}

type ServerConfig struct {
	// The port the server will listen on.
	Port uint16
}

func NewServer(config ServerConfig) *Server {
	return &Server{config: config}
}

func (s *Server) Start() error {
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", s.config.Port))
	if err != nil {
		return fmt.Errorf("listening on port %d: %w", s.config.Port, err)
	}
	grpcServer := grpc.NewServer()
	proto.RegisterGreeterServer(grpcServer, s)
	if err := grpcServer.Serve(listener); err != nil {
		return fmt.Errorf("serving listener: %w", err)
	}
	return nil
}

func (s *Server) AssignMapTask(ctx context.Context, in *proto.AssignMapTaskRequest) (*proto.AssignMapTaskRequestReply, error) {
	return &proto.AssignMapTaskRequestReply{}, nil
}
