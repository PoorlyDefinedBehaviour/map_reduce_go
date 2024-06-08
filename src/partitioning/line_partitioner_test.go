package partitioning

import (
	"fmt"
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
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

func TestPartition(t *testing.T) {
	t.Parallel()

	file, err := createTempFile("line_partitioner")
	assert.NoError(t, err)

	numberOfFileLines := 10
	maxNumberOfPartitions := 3

	for i := 0; i < int(numberOfFileLines); i++ {
		_, err := file.WriteString(fmt.Sprintf("line_%d\n", i))
		assert.NoError(t, err)
	}

	assert.NoError(t, file.Sync())
	_, err = file.Seek(0, 0)
	assert.NoError(t, err)

	partitioner := NewLinePartitioner()
	partitionFilePaths, err := partitioner.Partition(file.Name(), uint32(maxNumberOfPartitions))
	assert.NoError(t, err)
	fmt.Printf("\n\naaaaaaa partitionFilePaths %+v\n\n", partitionFilePaths)

	for _, filepath := range partitionFilePaths {

		file, err := os.Open(filepath)
		assert.NoError(t, err)

		contents, err := io.ReadAll(file)
		assert.NoError(t, err)

		fmt.Printf("\n\naaaaaaa string(contents)\n%+v\n\n", string(contents))

	}
}
