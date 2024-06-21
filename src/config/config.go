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
	MasterAddr              string
	WorkerHost              string
	WorkerHeartbeatInterval time.Duration
	WorkerHeartbeatTimeout  time.Duration
	GrpcServerPort          int
	HttpServerPort          int
}

// Returns true when the process is running as a worker.
func (cfg *Config) IsWorker() bool {
	return cfg.MasterAddr != ""
}

func Parse(args []string) (*Config, error) {
	var config Config

	flagSet := flag.NewFlagSet("config", 0)

	flagSet.StringVar(&config.MasterAddr, "master.addr", "", "The address the master is running at")
	flagSet.StringVar(&config.WorkerHost, "worker.host", "0.0.0.0", "The host where the worker is running at")
	flagSet.DurationVar(&config.WorkerHeartbeatInterval, "worker.heartbeat.interval", 5*time.Second, "How long to wait for between sending heartbeat requests")
	flagSet.DurationVar(&config.WorkerHeartbeatTimeout, "worker.heartbeat.timeout", 5*time.Second, "How long to wait for between sending heartbeat requests")
	flagSet.IntVar(&config.GrpcServerPort, "grpc.port", 8002, "The port the grpc server will listen on")
	flagSet.IntVar(&config.HttpServerPort, "http.port", 8001, "The port the http server will listen on")

	if err := flagSet.Parse(args); err != nil {
		return nil, fmt.Errorf("parsing flags: %w", err)
	}

	return &config, nil
}
