package partitioning

import (
	"fmt"
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPartition(t *testing.T) {
	t.Parallel()

	file, err := os.CreateTemp("", "line_partitioner")
	assert.NoError(t, err)

	for i := 0; i < 10; i++ {

		_, err := file.WriteString(fmt.Sprintf("partition %d\n", i))
		assert.NoError(t, err)
	}

	assert.NoError(t, file.Sync())
	_, err = file.Seek(0, 0)
	assert.NoError(t, err)

	partitioner := NewLinePartitioner()
	partitionFilePaths, err := partitioner.Partition(file.Name(), 10)
	assert.NoError(t, err)
	fmt.Printf("\n\naaaaaaa partitionFilePaths %+v\n\n", partitionFilePaths)

	for i, filepath := range partitionFilePaths {
		file, err := os.Open(filepath)
		assert.NoError(t, err)

		contents, err := io.ReadAll(file)
		assert.NoError(t, err)

		assert.Equal(t, fmt.Sprintf("partition %d", i), string(contents))
	}
}
