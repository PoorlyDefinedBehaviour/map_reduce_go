package partitioning

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func createTempFile(fileName string) (*os.File, error) {
	directory, err := os.MkdirTemp("", "line_partitioner_*")
	if err != nil {
		return nil, fmt.Errorf("creating temporary directory: %w", err)
	}
	file, err := os.CreateTemp(directory, fileName)
	if err != nil {
		return file, fmt.Errorf("creating temporary file: %w", err)
	}

	return file, nil
}

func mustReadFile(filePath string) string {
	file, err := os.Open(filePath)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	contents, err := io.ReadAll(file)
	if err != nil {
		panic(err)
	}

	return string(contents)
}

func TestPartition(t *testing.T) {
	t.Parallel()

	t.Run("empty file", func(t *testing.T) {
		t.Parallel()

		file, err := createTempFile("line_partitioner")
		require.NoError(t, err)
		directory := filepath.Dir(file.Name())

		maxNumberOfPartitions := 10

		partitioner := NewLinePartitioner()

		partitionFilePaths, err := partitioner.Partition(file.Name(), directory, uint32(maxNumberOfPartitions))
		require.NoError(t, err)
		assert.Empty(t, partitionFilePaths)
	})

	t.Run("file has 1 line without new line at the end", func(t *testing.T) {
		t.Parallel()

		file, err := createTempFile("line_partitioner")
		require.NoError(t, err)

		_, err = file.WriteString("a b c")
		require.NoError(t, err)

		directory := filepath.Dir(file.Name())

		maxNumberOfPartitions := 10

		partitioner := NewLinePartitioner()
		partitionFilePaths, err := partitioner.Partition(file.Name(), directory, uint32(maxNumberOfPartitions))
		require.NoError(t, err)

		assert.Equal(t, 1, len(partitionFilePaths))
		assert.Equal(t, "a b c\n", mustReadFile(partitionFilePaths[0]))
	})

	t.Run("file has 1 line with new line at the end", func(t *testing.T) {
		t.Parallel()

		file, err := createTempFile("line_partitioner")
		require.NoError(t, err)

		_, err = file.WriteString("a b c\n")
		require.NoError(t, err)

		directory := filepath.Dir(file.Name())

		maxNumberOfPartitions := 10

		partitioner := NewLinePartitioner()
		partitionFilePaths, err := partitioner.Partition(file.Name(), directory, uint32(maxNumberOfPartitions))
		require.NoError(t, err)

		assert.Equal(t, 1, len(partitionFilePaths))
		assert.Equal(t, "a b c\n", mustReadFile(partitionFilePaths[0]))
	})

	t.Run("the number of partitions is greater than the number of lines in the file", func(t *testing.T) {
		t.Parallel()

		file, err := createTempFile("line_partitioner")
		require.NoError(t, err)
		directory := filepath.Dir(file.Name())

		numberOfFileLines := 3
		maxNumberOfPartitions := 5

		for i := range numberOfFileLines {
			_, err := file.WriteString(fmt.Sprintf("line_%d\n", i))
			require.NoError(t, err)
		}

		require.NoError(t, file.Sync())
		_, err = file.Seek(0, 0)
		require.NoError(t, err)

		partitioner := NewLinePartitioner()
		partitionFilePaths, err := partitioner.Partition(file.Name(), directory, uint32(maxNumberOfPartitions))
		require.NoError(t, err)

		assert.Len(t, partitionFilePaths, 3)

		assert.Equal(t, "line_0\n", mustReadFile(partitionFilePaths[0]))
		assert.Equal(t, "line_1\n", mustReadFile(partitionFilePaths[1]))
		assert.Equal(t, "line_2\n", mustReadFile(partitionFilePaths[2]))
	})
}
