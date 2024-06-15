package grpc

import (
	"context"
	"fmt"

	"github.com/poorlydefinedbehaviour/map_reduce_go/src/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Client struct {
	conn       *grpc.ClientConn
	grpcClient proto.MapReduceNodeClient
}

type ClientConfig struct {
	// The address to connect to.
	Addr string
}

func NewClient(config ClientConfig) (*Client, error) {
	// Set up a connection to the server.
	conn, err := grpc.NewClient(config.Addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("creating grpc client: %w", err)
	}
	defer conn.Close()
	c := proto.NewMapReduceNodeClient(conn)

	return &Client{
		conn:       conn,
		grpcClient: c,
	}, nil
}

func AssignMapTask(ctx context.Context, filePath string) error {
	panic("todo")
}

func (client *Client) Close() error {
	if err := client.conn.Close(); err != nil {
		return fmt.Errorf("closing grpc client connection: %w", err)
	}
	return nil
}
