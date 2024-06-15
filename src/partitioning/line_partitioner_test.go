package partitioning

import (
	"bytes"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"
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

	t.Run("the number of partitions is greater than the number of lines in the file", func(t *testing.T) {
		t.Parallel()

		file, err := createTempFile("line_partitioner")
		require.NoError(t, err)
		directory := filepath.Dir(file.Name())
		fmt.Printf("\n\naaaaaaa file.Name() %+v\n\n", file.Name())

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

		assert.Len(t, partitionFilePaths, 4)

		assert.Equal(t, "line_0\n", mustReadFile(partitionFilePaths[0]))
		assert.Equal(t, "line_1\n", mustReadFile(partitionFilePaths[1]))
		assert.Equal(t, "line_2\n", mustReadFile(partitionFilePaths[2]))
		assert.Equal(t, "", mustReadFile(partitionFilePaths[3]))
	})

	t.Run("property", func(t *testing.T) {
		t.Parallel()

		rapid.Check(t, func(t *rapid.T) {
			file, err := createTempFile("line_partitioner")
			require.NoError(t, err)
			directory := filepath.Dir(file.Name())

			numberOfFileLines := rapid.Uint8Range(1, 255).Draw(t, "numberOfFileLines")
			maxNumberOfPartitions := rapid.Uint8Range(1, 255).Draw(t, "maxNumberOfPartitions")
			maxLinesPerPartition := int64(math.Ceil(float64(numberOfFileLines) / float64(maxNumberOfPartitions)))

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

			for i, filePath := range partitionFilePaths {
				file, err := os.Open(filePath)
				require.NoError(t, err)

				contents, err := io.ReadAll(file)
				require.NoError(t, err)

				startingLine := i * int(maxLinesPerPartition)
				endingLine := int(math.Min(float64(numberOfFileLines), float64(startingLine+int(maxLinesPerPartition))))

				expectedFileContents := generateExpectedFileContents(startingLine, endingLine)

				fmt.Printf("numberOfFileLines=%d maxNumberOfPartitions=%d maxLinesPerPartition=%d expected:\n%+v\n\ngot:\n%+v\n",
					numberOfFileLines,
					maxNumberOfPartitions,
					maxLinesPerPartition,
					expectedFileContents, string(contents),
				)
				assert.Equal(t, expectedFileContents, string(contents))
			}
		})
	})
}

func generateExpectedFileContents(startingLine, endingLine int) string {
	buffer := bytes.NewBufferString("")

	for i := startingLine; i < endingLine; i++ {
		_, err := buffer.Write([]byte(fmt.Sprintf("line_%d\n", i)))
		if err != nil {
			panic(err)
		}
	}

	return buffer.String()
}
