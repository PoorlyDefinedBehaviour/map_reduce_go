package grpc

import (
	"context"
	"fmt"

	"github.com/poorlydefinedbehaviour/map_reduce_go/src/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type MasterClient struct {
	conn       *grpc.ClientConn
	grpcClient proto.MasterClient
}

type MasterClientConfig struct {
	// The address to connect to.
	Addr string
}

func NewMasterClient(config MasterClientConfig) (*MasterClient, error) {
	// Set up a connection to the server.
	conn, err := grpc.NewClient(config.Addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("creating grpc client: %w", err)
	}
	c := proto.NewMasterClient(conn)

	return &MasterClient{
		conn:       conn,
		grpcClient: c,
	}, nil
}

func (client *MasterClient) Heartbeat(ctx context.Context, workerAddr string) error {
	if _, err := client.grpcClient.Heartbeat(ctx, &proto.HeartbeatRequest{WorkerAddr: workerAddr}); err != nil {
		return fmt.Errorf("sending HeartBeat request: %w", err)
	}
	return nil
}

func (client *MasterClient) Close() error {
	if err := client.conn.Close(); err != nil {
		return fmt.Errorf("closing grpc client connection: %w", err)
	}
	return nil
}
