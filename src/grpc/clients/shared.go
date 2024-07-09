package grpcclient

import (
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func withReconnect[T any](conn *grpc.ClientConn, f func() (T, error)) (T, error) {
	result, err := f()
	if err == nil {
		return result, nil
	}

	// If the error is not because the server wasn't reached.
	if status.Code(err) != codes.Unavailable {
		return result, err
	}

	// Try to reconnect to the server.
	conn.Connect()

	// Execute the function after reconnecting.
	result, err = f()

	return result, err
}
