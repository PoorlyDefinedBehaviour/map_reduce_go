package testingext

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TempDir() string {
	dir := filepath.Join(os.TempDir(), uuid.NewString())
	if err := os.MkdirAll(dir, 0755); err != nil {
		panic(err)
	}
	return dir
}

type TempFileOption func(*TempFileConfig)

func WithDir(dir string) TempFileOption {
	return func(cfg *TempFileConfig) {
		cfg.Dir = dir
	}
}

func WithFileName(name string) TempFileOption {
	return func(cfg *TempFileConfig) {
		cfg.FileName = name
	}
}

type TempFileConfig struct {
	Dir      string
	FileName string
}

func TempFile(options ...TempFileOption) *os.File {
	config := TempFileConfig{}
	for _, option := range options {
		option(&config)
	}
	if config.Dir != "" {
		if err := os.MkdirAll(config.Dir, 0755); err != nil {
			panic(err)
		}
	} else {
		config.Dir = TempDir()
	}
	if config.FileName == "" {
		config.FileName = fmt.Sprintf("file_%s", uuid.NewString())
	}

	file, err := os.OpenFile(filepath.Join(config.Dir, uuid.NewString()), os.O_RDWR|os.O_CREATE, 0755)
	if err != nil {
		panic(err)
	}
	return file
}

func WriteFile(path string, data string) *os.File {
	file := TempFile(WithDir(filepath.Dir(path)), WithFileName(filepath.Base(path)))
	if _, err := file.WriteString(data); err != nil {
		panic(err)
	}
	return file
}

func MustReadFile(t *testing.T, path string) string {
	t.Helper()
	buffer, err := os.ReadFile(path)
	require.NoError(t, err)
	return string(buffer)
}
