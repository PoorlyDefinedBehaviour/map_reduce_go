package contracts

import (
	"context"
)

type MessageBus interface {
	AssignMapTask(ctx context.Context, workerAddr string, taskID TaskID, script string, filePath string) error
}
