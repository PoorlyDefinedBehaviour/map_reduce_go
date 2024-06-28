package worker

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/poorlydefinedbehaviour/map_reduce_go/src/contracts"
	"github.com/poorlydefinedbehaviour/map_reduce_go/src/interpreters/javascript"
)

type Worker struct {
	config       Config
	masterClient contracts.MasterClient
	fileStorage  contracts.FileStorage
	state        contracts.WorkerState
}

type Config struct {
	// The worker address. Used to register the worker with the master.
	Addr string
	// The folder to put task outputs.
	WorkspaceFolder string
	// The maximum number of bytes each output file can have.
	MaxFileSizeBytes uint64
	// How long to wait for between sending heartbeat requests.
	HeartbeatInterval time.Duration
	// How long to wait for a heartbeat request to complete.
	HeartbeatTimeout time.Duration
	// How long to wait for a request to let the master know which map tasks have been completed to complete.
	MapTasksCompletedTimeout time.Duration
}

func New(config Config, masterClient contracts.MasterClient, fileStorage contracts.FileStorage) (*Worker, error) {
	if config.Addr == "" {
		return nil, fmt.Errorf("addr is required")
	}
	if config.WorkspaceFolder == "" {
		return nil, fmt.Errorf("workspace folder is required")
	}
	if config.MaxFileSizeBytes == 0 {
		return nil, fmt.Errorf("max file size in bytes is required")
	}
	if config.HeartbeatInterval == 0 {
		return nil, fmt.Errorf("heartbeat interval is required")
	}
	if config.HeartbeatTimeout == 0 {
		return nil, fmt.Errorf("heartbeat timeout request is required")
	}
	if config.MapTasksCompletedTimeout == 0 {
		return nil, fmt.Errorf("map tasks completed request timeout is required")
	}
	return &Worker{
		config:       config,
		masterClient: masterClient,
		fileStorage:  fileStorage,
		state:        contracts.WorkerStateIdle,
	}, nil
}

func (worker *Worker) HeartbeatControlLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			fmt.Println("sending heartbeat")
			if err := worker.sendHeartbeat(ctx); err != nil {
				fmt.Printf("sending heartbeat to master, will sleep before retrying: %s\n", err)
			}
			time.Sleep(worker.config.HeartbeatInterval)
		}

	}
}

func (worker *Worker) OnMapTaskReceived(ctx context.Context, task contracts.MapTask) error {
	jsScript, err := javascript.Parse(task.Script)
	if err != nil {
		return fmt.Errorf("parsing script: script='%s' %w", task.Script, err)
	}
	defer jsScript.Close()

	reader, err := worker.fileStorage.Open(ctx, task.FilePath)
	if err != nil {
		return fmt.Errorf("opening file: path=%s %w", task.FilePath, err)
	}
	defer reader.Close()

	contents, err := io.ReadAll(reader)
	if err != nil {
		return fmt.Errorf("reading file contents: %w", err)
	}

	outputFolder := fmt.Sprintf("%s/%d", worker.config.WorkspaceFolder, task.ID)

	writer, err := worker.fileStorage.NewWriter(ctx, worker.config.MaxFileSizeBytes, outputFolder)
	if err != nil {
		return fmt.Errorf("creating file storage writer: %w", err)
	}
	defer writer.Close()

	if err := jsScript.Map(task.FilePath, string(contents), func(key, value string) error {
		fmt.Printf("\n\naaaaaaa Worker.OnMapTaskReceived emit called with: key %+v value %+v\n\n", key, value)
		if _, err := writer.Write([]byte(key)); err != nil {
			return fmt.Errorf("emit: writing key to writer: %w", err)
		}
		if _, err := writer.Write([]byte(",")); err != nil {
			return fmt.Errorf("emit: writing , to writer: %w", err)
		}
		if _, err := writer.Write([]byte(value)); err != nil {
			return fmt.Errorf("emit: writing key to writer: %w", err)
		}

		return nil
	}); err != nil {
		return fmt.Errorf("executing Map task script: %w", err)
	}

	if err := writer.Close(); err != nil {
		return fmt.Errorf("closing storage writer: %w", err)
	}

	outputFiles := writer.OutputFiles()

	completedTask := contracts.CompletedTask{TaskID: task.ID}
	for _, outputFile := range outputFiles {
		completedTask.OutputFiles = append(completedTask.OutputFiles, contracts.OutputFile{
			FileID:    task.FileID,
			FilePath:  outputFile.Path,
			SizeBytes: outputFile.SizeBytes,
		},
		)
	}

	timeoutCtx, cancel := context.WithTimeout(ctx, worker.config.MapTasksCompletedTimeout)
	defer cancel()
	if err := worker.masterClient.MapTasksCompleted(timeoutCtx, worker.config.Addr, []contracts.CompletedTask{completedTask}); err != nil {
		return fmt.Errorf("sending MapTasksCompleted message to master: %w", err)
	}

	return nil
}

func (worker *Worker) sendHeartbeat(ctx context.Context) error {
	timeoutCtx, cancel := context.WithTimeout(ctx, worker.config.HeartbeatTimeout)
	defer cancel()
	if err := worker.masterClient.Heartbeat(timeoutCtx, worker.state, worker.config.Addr); err != nil {
		return fmt.Errorf("sending heartbeat to master: %w", err)
	}

	return nil
}
