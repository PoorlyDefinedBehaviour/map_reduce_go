package filestorage

import (
	"context"
	"fmt"
	"io"
	"os"

	"github.com/poorlydefinedbehaviour/map_reduce_go/src/contracts"
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
	outputFiles     []contracts.StorageFileInfo
}

func (f *FlushOnCloseFile) createFile(path string) error {
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("opening/creating file: %w", err)
	}
	f.inner.file = file
	f.inner.fileSizeInBytes = 0
	f.inner.fileNumber++
	return nil
}

func (f FlushOnCloseFile) Write(p []byte) (n int, err error) {

	if f.inner.file == nil || f.inner.fileSizeInBytes+uint64(len(p)) > f.inner.maxFilSizeBytes {
		if f.inner.file != nil {
			if err := f.syncAndCloseFile(f.inner.file); err != nil {
				return 0, err
			}
		}
		filePath := fmt.Sprintf("%s/file_%d", f.inner.folder, f.inner.fileNumber)
		if err := f.createFile(filePath); err != nil {
			return 0, fmt.Errorf("creating new partition file: %w", err)
		}
	}

	n, err = f.inner.file.Write(p)
	if err != nil {
		return n, fmt.Errorf("writing to file: %w", err)
	}

	f.inner.fileSizeInBytes += uint64(len(p))

	return n, nil
}

func (f FlushOnCloseFile) OutputFiles() []contracts.StorageFileInfo {
	return f.inner.outputFiles
}

func (f *FlushOnCloseFile) syncAndCloseFile(file *os.File) error {
	if err := file.Sync(); err != nil {
		return fmt.Errorf("syncing file: %w", err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("closing file: %w", err)
	}

	f.inner.outputFiles = append(f.inner.outputFiles, contracts.StorageFileInfo{Path: file.Name(), SizeBytes: f.inner.fileSizeInBytes})

	return nil
}

func (f FlushOnCloseFile) Close() error {
	return f.syncAndCloseFile(f.inner.file)
}

func (storage *FileStorage) Open(ctx context.Context, filePath string) (io.ReadCloser, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return file, fmt.Errorf("trying to open file on local disk: path=%s %w", filePath, err)
	}
	return file, nil
}

func (storage *FileStorage) NewWriter(ctx context.Context, maxFileSizeBytes uint64, folder string) (contracts.FileWriter, error) {
	if err := os.MkdirAll(folder, 0750); err != nil {
		return nil, fmt.Errorf("creating file path: path=%s %w", folder, err)
	}

	wrapper := FlushOnCloseFile{inner: &inner{folder: folder, maxFilSizeBytes: maxFileSizeBytes}}

	return wrapper, nil
}
