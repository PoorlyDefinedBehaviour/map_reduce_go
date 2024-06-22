package contracts

import (
	"context"
	"io"
)

type FileStorage interface {
	Open(ctx context.Context, filePath string) (io.ReadCloser, error)
	NewWriter(ctx context.Context, maxFileSizeBytes uint64, folder string) (io.WriteCloser, error)
}
