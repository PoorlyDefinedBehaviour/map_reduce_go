package config

import (
	"flag"
	"fmt"
	"time"
)

type Mode string

var (
	WorkerMode Mode = "worker"
	MasterMode Mode = "master"
)

type Config struct {
	WorkspaceFolder string
	MasterAddr      string
	WorkerHost      string

	WorkerHeartbeatInterval time.Duration
	WorkerHeartbeatTimeout  time.Duration
	WorkerMaxFileSizeBytes  uint64

	MessageBusConnectionCleanupInterval time.Duration
	GrpcServerPort                      int
	HttpServerPort                      int
}

// Returns true when the process is running as a worker.
func (cfg *Config) IsWorker() bool {
	return cfg.MasterAddr != ""
}

func Parse(args []string) (*Config, error) {
	var config Config

	flagSet := flag.NewFlagSet("config", 0)

	flagSet.StringVar(&config.WorkspaceFolder, "workspace-folder", "", "The folder to put task files")
	flagSet.StringVar(&config.MasterAddr, "master.addr", "", "The address the master is running at")
	flagSet.StringVar(&config.WorkerHost, "worker.host", "127.0.0.1", "The host where the worker is running at")
	flagSet.DurationVar(&config.WorkerHeartbeatInterval, "worker.heartbeat.interval", 5*time.Second, "How long to wait for between sending heartbeat requests")
	flagSet.DurationVar(&config.WorkerHeartbeatTimeout, "worker.heartbeat.timeout", 5*time.Second, "How long to wait for between sending heartbeat requests")
	flagSet.Uint64Var(&config.WorkerMaxFileSizeBytes, "worker.max-file-size-bytes", 67108864, "The maximum number of bytes each output file can have")
	flagSet.DurationVar(&config.MessageBusConnectionCleanupInterval, "message-bus.connection-clean-up-interval", 5*time.Second, "How long to wait for between checking if any of the client connections have disconnected")
	flagSet.IntVar(&config.GrpcServerPort, "grpc.port", 8002, "The port the grpc server will listen on")
	flagSet.IntVar(&config.HttpServerPort, "http.port", 8001, "The port the http server will listen on")

	if err := flagSet.Parse(args); err != nil {
		return nil, fmt.Errorf("parsing flags: %w", err)
	}

	if config.WorkspaceFolder == "" {
		return &config, fmt.Errorf("workspace-folder is required")
	}

	if config.WorkerMaxFileSizeBytes == 0 {
		return &config, fmt.Errorf("worker.max-file-size-bytes is required")
	}

	return &config, nil
}
