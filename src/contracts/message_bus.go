package contracts

import (
	"context"
)

type MessageBus interface {
	AssignMapTask(ctx context.Context, workerAddr string, task MapTask) error
}
