package grpc

import (
	"context"
	"fmt"

	"github.com/poorlydefinedbehaviour/map_reduce_go/src/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type WorkerClient struct {
	conn       *grpc.ClientConn
	grpcClient proto.WorkerClient
}

type WorkerClientConfig struct {
	// The address to connect to.
	Addr string
}

func NewClient(config WorkerClientConfig) (*WorkerClient, error) {
	// Set up a connection to the server.
	conn, err := grpc.NewClient(config.Addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("creating grpc client: %w", err)
	}
	c := proto.NewWorkerClient(conn)

	return &WorkerClient{
		conn:       conn,
		grpcClient: c,
	}, nil
}

func (client *WorkerClient) AssignMapTask(ctx context.Context, filePath string) error {
	if _, err := client.grpcClient.AssignMapTask(ctx, &proto.AssignMapTaskRequest{FilePath: filePath}); err != nil {
		return fmt.Errorf("sending AssignMapTask request: %w", err)
	}
	return nil
}

func (client *WorkerClient) Close() error {
	if err := client.conn.Close(); err != nil {
		return fmt.Errorf("closing grpc client connection: %w", err)
	}
	return nil
}
