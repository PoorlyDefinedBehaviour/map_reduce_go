package inmemory

import (
	"context"

	"github.com/poorlydefinedbehaviour/map_reduce_go/src/contracts"
)

type MessageBus struct{}

func NewMessageBus() *MessageBus {
	return &MessageBus{}
}

func (bus *MessageBus) AssignMapTask(ctx context.Context, workerAddr string, task contracts.MapTask) error {
	return nil
}
