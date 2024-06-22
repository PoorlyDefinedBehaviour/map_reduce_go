package filestorage

import (
	"context"
	"fmt"
	"io"
	"os"
)

type FileStorage struct{}

func New() *FileStorage {
	return &FileStorage{}
}

type FlushOnCloseFile struct {
	inner *inner
}

type inner struct {
	file            *os.File
	fileSizeInBytes uint64
	folder          string
	fileNumber      uint32
	maxFilSizeBytes uint64
}

func (f FlushOnCloseFile) Write(p []byte) (n int, err error) {
	newFileSizeBytes := f.inner.fileSizeInBytes + uint64(len(p))

	if f.inner.file == nil || newFileSizeBytes > f.inner.maxFilSizeBytes {
		if f.inner.file != nil {
			if err := syncAndCloseFile(f.inner.file); err != nil {
				return 0, err
			}
		}
		filePath := fmt.Sprintf("%s/file_%d", f.inner.folder, f.inner.fileNumber)
		file, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
		if err != nil {
			return 0, fmt.Errorf("opening file: %w", err)
		}

		f.inner.file = file
		f.inner.fileNumber++
	}

	n, err = f.inner.file.Write(p)
	if err != nil {
		return n, fmt.Errorf("writing to file: %w", err)
	}

	f.inner.fileSizeInBytes += newFileSizeBytes

	return n, nil
}

func syncAndCloseFile(file *os.File) error {
	if err := file.Sync(); err != nil {
		return fmt.Errorf("syncing file: %w", err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("closing file: %w", err)
	}
	return nil
}

func (f FlushOnCloseFile) Close() error {
	return syncAndCloseFile(f.inner.file)
}

func (storage *FileStorage) Open(ctx context.Context, filePath string) (io.ReadCloser, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return file, fmt.Errorf("trying to open file on local disk: path=%s %w", filePath, err)
	}
	return file, nil
}

func (storage *FileStorage) NewWriter(ctx context.Context, maxFileSizeBytes uint64, folder string) (io.WriteCloser, error) {
	if err := os.MkdirAll(folder, 0750); err != nil {
		return nil, fmt.Errorf("creating file path: path=%s %w", folder, err)
	}

	wrapper := FlushOnCloseFile{inner: &inner{folder: folder, maxFilSizeBytes: maxFileSizeBytes}}

	return wrapper, nil
}
