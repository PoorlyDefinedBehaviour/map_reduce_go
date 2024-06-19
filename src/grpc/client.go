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
	c := proto.NewMapReduceNodeClient(conn)

	return &Client{
		conn:       conn,
		grpcClient: c,
	}, nil
}

func (client *Client) AssignMapTask(ctx context.Context, in *proto.AssignMapTaskRequest, opts ...grpc.CallOption) (*proto.AssignMapTaskRequestReply, error) {
	reply, err := client.grpcClient.AssignMapTask(ctx, in, opts...)
	if err != nil {
		return reply, fmt.Errorf("sending AssignMapTask request to worker: %w", err)
	}
	return reply, nil
}

func (client *Client) Close() error {
	if err := client.conn.Close(); err != nil {
		return fmt.Errorf("closing grpc client connection: %w", err)
	}
	return nil
}
