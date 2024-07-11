package grpcclient

import (
	"context"
	"fmt"

	"github.com/poorlydefinedbehaviour/map_reduce_go/src/contracts"
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

func (client *MasterClient) Heartbeat(ctx context.Context, workerAddr string, memoryAvailable uint64) error {
	_, err := withReconnect(client.conn, func() (*proto.HeartbeatReply, error) {
		return client.grpcClient.Heartbeat(ctx, &proto.HeartbeatRequest{
			WorkerAddr:      workerAddr,
			MemoryAvailable: memoryAvailable,
		})
	})
	if err != nil {
		return fmt.Errorf("sending HeartBeat request: %w", err)
	}

	return nil
}

func (client *MasterClient) MapTasksCompleted(ctx context.Context, workerAddr string, tasks []contracts.CompletedTask) error {
	protoTasks := make([]*proto.Task, 0, len(tasks))
	for _, task := range tasks {
		protoTask := &proto.Task{TaskID: uint64(task.TaskID)}
		for _, outputFile := range task.OutputFiles {
			protoTask.OutputFiles = append(protoTask.OutputFiles, &proto.File{
				FileID:    uint64(outputFile.FileID),
				Path:      outputFile.FilePath,
				SizeBytes: outputFile.SizeBytes,
			})
		}
		protoTasks = append(protoTasks, protoTask)
	}

	_, err := withReconnect(client.conn, func() (*proto.MapTasksCompletedReply, error) {
		return client.grpcClient.MapTasksCompleted(ctx, &proto.MapTasksCompletedRequest{WorkerAddr: workerAddr, Tasks: protoTasks})
	})
	if err != nil {
		return fmt.Errorf("send MapTasksCompleted message to master: %w", err)
	}
	return nil
}

func (client *MasterClient) Close() error {
	if err := client.conn.Close(); err != nil {
		return fmt.Errorf("closing grpc client connection: %w", err)
	}
	return nil
}