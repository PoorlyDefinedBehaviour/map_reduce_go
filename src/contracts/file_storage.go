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
	io.Closer
	WriteKeyValue(k, v string, region uint32) error
	OutputFiles() []StorageFileInfo
}

type FileStorage interface {
	NewWriter(ctx context.Context, folder string) (FileWriter, error)
	NewReader(ctx context.Context, filePath string) (io.ReadCloser, error)
}
