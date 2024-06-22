package contracts

import (
	"context"
	"io"
)

type StorageFileInfo struct {
	Path      string
	SizeBytes uint64
}

type FileWriter interface {
	io.WriteCloser
	OutputFiles() []StorageFileInfo
}

type FileStorage interface {
	Open(ctx context.Context, filePath string) (io.ReadCloser, error)
	NewWriter(ctx context.Context, maxFileSizeBytes uint64, folder string) (FileWriter, error)
}
