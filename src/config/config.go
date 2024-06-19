package config

import (
	"flag"
	"fmt"
)

type Mode string

var (
	WorkerMode Mode = "worker"
	MasterMode Mode = "master"
)

type Config struct {
	Mode Mode
}

func Parse(args []string) (*Config, error) {
	var config Config

	flagSet := flag.NewFlagSet("config", 0)

	var mode string
	flagSet.StringVar(&mode, "mode", "worker", "Set to worker to run binary as worker and master to run as master")

	if err := flagSet.Parse(args); err != nil {
		return nil, fmt.Errorf("parsing flags: %w", err)
	}

	if mode != "master" && mode != "worker" {
		return nil, fmt.Errorf("mode is required: mode=%s", mode)
	}

	config.Mode = Mode(mode)

	return &config, nil
}
