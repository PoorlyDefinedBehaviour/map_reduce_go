package grpcclient

import (
	"context"
	"fmt"

	"github.com/poorlydefinedbehaviour/map_reduce_go/src/contracts"
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

func NewWorkerClient(config WorkerClientConfig) (*WorkerClient, error) {
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

func (client *WorkerClient) AssignMapTask(ctx context.Context, task contracts.MapTask) error {
	_, err := withReconnect(client.conn, func() (*proto.AssignMapTaskReply, error) {
		return client.grpcClient.AssignMapTask(ctx, &proto.AssignMapTaskRequest{
			TaskID:              uint64(task.ID),
			Script:              task.Script,
			FileID:              uint64(task.FileID),
			FilePath:            task.FilePath,
			NumberOfReduceTasks: task.NumberOfReduceTasks,
		})
	})
	if err != nil {
		return fmt.Errorf("sending AssignMapTask request: %w", err)
	}
	return nil
}

func (client *WorkerClient) AssignReduceTask(ctx context.Context, task contracts.ReduceTask) error {
	files := make([]*proto.File, 0, len(task.Files))
	for _, file := range task.Files {
		files = append(files, &proto.File{FileID: uint64(file.FileID), SizeBytes: file.SizeBytes, Path: file.Path})
	}
	_, err := withReconnect(client.conn, func() (*proto.AssignReduceTaskReply, error) {
		return client.grpcClient.AssignReduceTask(ctx, &proto.AssignReduceTaskRequest{
			TaskID: uint64(task.ID),
			Script: task.Script,
			Files:  files,
		})
	})
	if err != nil {
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
