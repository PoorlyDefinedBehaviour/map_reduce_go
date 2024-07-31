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

	writer, err := filestorage.New().NewWriter(context.Background(), dir)
	require.NoError(t, err)

	require.NoError(t, writer.WriteKeyValue("u", "1", 0))
	require.NoError(t, writer.WriteKeyValue("i", "1", 0))
	require.NoError(t, writer.WriteKeyValue("u", "2", 0))

	require.NoError(t, writer.WriteKeyValue("b", "1", 1))
	require.NoError(t, writer.WriteKeyValue("a", "1", 1))
	require.NoError(t, writer.WriteKeyValue("a", "1", 1))

	require.NoError(t, writer.Close())

	region0File, err := os.OpenFile(filepath.Join(dir, "region_0"), os.O_RDONLY, 0755)
	require.NoError(t, err)
	region0FileStat, err := region0File.Stat()
	require.NoError(t, err)

	region1File, err := os.OpenFile(filepath.Join(dir, "region_1"), os.O_RDONLY, 0755)
	require.NoError(t, err)
	region1FileStat, err := region1File.Stat()

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
				SizeBytes: uint64(region0FileStat.Size()),
				Path:      region0File.Name(),
			},
			{
				FileID:    2,
				SizeBytes: uint64(region1FileStat.Size()),
				Path:      region1File.Name(),
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
