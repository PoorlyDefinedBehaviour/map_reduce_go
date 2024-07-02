package master

import (
	"context"
	"fmt"
)

type IOHandler struct {
	master *Master
}

func NewIOHandler(ctx context.Context, master *Master) *IOHandler {
	go master.controlLoop(ctx)
	return &IOHandler{master: master}
}

func (handler *IOHandler) ExecuteTask(ctx context.Context, input ValidatedInput) ([]byte, error) {
	_, err := handler.master.OnMessage(ctx, &NewTaskMessage{Input: input})
	if err != nil {
		return nil, fmt.Errorf("handling NewTaskMessage: %w", err)
	}
	return nil, nil
}

func (handler *IOHandler) OnMessage(ctx context.Context, msg InputMessage) error {
	_, err := handler.master.OnMessage(ctx, msg)
	if err != nil {
		return fmt.Errorf("handling message: %w", err)
	}
	return nil
}
