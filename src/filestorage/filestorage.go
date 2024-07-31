package filestorage

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"

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
	folder      string
	files       map[uint32]*os.File
	outputFiles []contracts.StorageFileInfo
}

func (f FlushOnCloseFile) getRegionFile(region uint32) (*os.File, error) {
	file, ok := f.inner.files[region]
	if ok {
		return file, nil
	}

	filePath := filepath.Join(f.inner.folder, fmt.Sprintf("region_%d", region))
	file, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return nil, fmt.Errorf("opening/creating file: %w", err)
	}

	f.inner.files[region] = file

	return file, nil
}

func (f FlushOnCloseFile) WriteKeyValue(k, v string, region uint32) error {
	file, err := f.getRegionFile(region)
	if err != nil {
		return fmt.Errorf("getting region file: %w", err)
	}

	info, err := file.Stat()
	if err != nil {
		return fmt.Errorf("getting file stats: %w", err)
	}

	if info.Size() > 0 {
		if _, err := file.WriteString("\n"); err != nil {
			return fmt.Errorf("writing new line: %w", err)
		}
	}

	if _, err := file.WriteString(k); err != nil {
		return fmt.Errorf("writing key: %w", err)
	}

	if _, err := file.WriteString(","); err != nil {
		return fmt.Errorf("writing ',': %w", err)
	}

	if _, err := file.WriteString(v); err != nil {
		return fmt.Errorf("writing value: %w", err)
	}

	return nil
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

	stat, err := os.Stat(file.Name())
	if err != nil {
		return fmt.Errorf("calling stat on file: %w", err)
	}

	f.inner.outputFiles = append(f.inner.outputFiles, contracts.StorageFileInfo{
		Path:      file.Name(),
		SizeBytes: uint64(stat.Size()),
	})

	return nil
}

func (f FlushOnCloseFile) Close() error {
	for _, file := range f.inner.files {
		if err := f.syncAndCloseFile(file); err != nil {
			return err
		}
	}

	return nil

}

func (storage *FileStorage) NewReader(ctx context.Context, filePath string) (io.ReadCloser, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return file, fmt.Errorf("trying to open file on local disk: path=%s %w", filePath, err)
	}
	return file, nil
}

func (storage *FileStorage) NewWriter(ctx context.Context, folder string) (contracts.FileWriter, error) {
	if err := os.MkdirAll(folder, 0750); err != nil {
		return nil, fmt.Errorf("creating file path: path=%s %w", folder, err)
	}

	wrapper := FlushOnCloseFile{
		inner: &inner{
			folder: folder,
			files:  make(map[uint32]*os.File),
		}}

	return wrapper, nil
}
