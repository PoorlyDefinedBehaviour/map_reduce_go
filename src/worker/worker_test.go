package worker

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/poorlydefinedbehaviour/map_reduce_go/src/clock"
	"github.com/poorlydefinedbehaviour/map_reduce_go/src/contracts"
	"github.com/poorlydefinedbehaviour/map_reduce_go/src/filestorage"
	"github.com/poorlydefinedbehaviour/map_reduce_go/src/partitioning"
	"github.com/poorlydefinedbehaviour/map_reduce_go/src/sorter"
	"github.com/poorlydefinedbehaviour/map_reduce_go/src/testingext"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOnReduceTaskReceived(t *testing.T) {
	t.Parallel()

	dir := testingext.TempDir()
	fmt.Printf("\n\naaaaaaa dir %+v\n\n", dir)

	worker, err := New(
		Config{
			Addr:                       "addr",
			WorkspaceFolder:            dir,
			MaxFileSizeBytes:           1024,
			MemoryAvailable:            1024,
			HeartbeatInterval:          5 * time.Second,
			HeartbeatTimeout:           15 * time.Second,
			MapTasksCompletedTimeout:   5 * time.Second,
			MaxInflightFileDownloads:   10,
			ExternalSortMaxMemoryBytes: 1024,
		},
		nil,
		filestorage.New(),
		clock.New(),
		partitioning.NewLinePartitioner(),
		sorter.NewLineSorter(),
	)
	require.NoError(t, err)

	file1 := testingext.TempFile(testingext.WithDir(dir), testingext.WithFileName("input_1"))
	defer file1.Close()
	_, err = file1.WriteString("b 1\na 1\na 1")
	require.NoError(t, err)
	file1Info, err := file1.Stat()
	require.NoError(t, err)

	file2 := testingext.TempFile(testingext.WithDir(dir), testingext.WithFileName("input_2"))
	defer file2.Close()
	_, err = file2.WriteString("u 1\ni 1\na 2")
	require.NoError(t, err)
	file2Info, err := file2.Stat()
	require.NoError(t, err)

	err = worker.OnReduceTaskReceived(context.Background(), contracts.ReduceTask{
		ID: 1,
		Script: `const partition = (key, r) => {
  let hash = 0

  for (const char of key) {
    hash ^= char.charCodeAt(0)
  }

  return hash % r
}

const map = (filename, contents, emit) => {
  for (const word of contents.split(/\s+/)) {
    const trimmedWord = word.trim()
    if (!trimmedWord) {
      continue
    }

    emit(word, "1")
  }
}

const reduce = (word, nextValueIter, emit) => {
  let count = 0

  while (true) {
    const [value, done] = nextValueIter()
    if (done) {
      break
    }

    count += Number(value)
  }

  emit(word, count.toString())
}
`,
		Files: []contracts.File{
			{
				FileID:    1,
				SizeBytes: uint64(file1Info.Size()),
				Path:      file1.Name(),
			},
			{
				FileID:    2,
				SizeBytes: uint64(file2Info.Size()),
				Path:      file2.Name(),
			},
		},
	})
	require.NoError(t, err)
}

func TestSortReduceInputFiles(t *testing.T) {
	t.Parallel()

	dir := testingext.TempDir()

	worker, err := New(
		Config{
			Addr:                       "addr",
			WorkspaceFolder:            dir,
			MaxFileSizeBytes:           1024,
			MemoryAvailable:            1024,
			HeartbeatInterval:          5 * time.Second,
			HeartbeatTimeout:           15 * time.Second,
			MapTasksCompletedTimeout:   5 * time.Second,
			MaxInflightFileDownloads:   10,
			ExternalSortMaxMemoryBytes: 1024,
		},
		nil,
		filestorage.New(),
		clock.New(),
		partitioning.NewLinePartitioner(),
		sorter.NewLineSorter(),
	)
	require.NoError(t, err)

	t.Run("basic", func(t *testing.T) {
		t.Parallel()

		files := []*os.File{
			testingext.WriteFile(filepath.Join(dir, "TestSortReduceInputFiles", uuid.NewString()), "b 1\nf 1\nd 1"),
			testingext.WriteFile(filepath.Join(dir, "TestSortReduceInputFiles", uuid.NewString()), "a 2\nc 3\na 1"),
			testingext.WriteFile(filepath.Join(dir, "TestSortReduceInputFiles", uuid.NewString()), "z 2\nx 5\ny 10"),
		}

		paths := []string{}
		for _, file := range files {
			paths = append(paths, file.Name())
		}

		sortedFile, err := worker.sortReduceInputFiles(paths)
		require.NoError(t, err)

		buffer, err := io.ReadAll(sortedFile)
		require.NoError(t, err)

		assert.Equal(t, "a 1\na 2\nb 1\nc 3\nd 1\nf 1\nx 5\ny 10\nz 2", string(buffer))
	})

	t.Run("model", func(t *testing.T) {
		t.Parallel()

		panic("TODO")
	})
}
