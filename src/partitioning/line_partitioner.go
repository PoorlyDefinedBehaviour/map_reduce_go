package partitioning

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path"
)

type LinePartitioner struct{}

func NewLinePartitioner() LinePartitioner {
	return LinePartitioner{}
}

func (partitioner *LinePartitioner) Partition(filepath string, maxNumberOfPartitions uint32) (partitionFilePaths []string, err error) {
	file, err := os.Open(filepath)
	if err != nil {
		return nil, fmt.Errorf("opening input file: filepath=%s %w", filepath, err)
	}
	defer func() {
		if closeErr := file.Close(); closeErr != nil {
			err = errors.Join(err, fmt.Errorf("closing input file: filepath=%s %w", filepath, closeErr))
		}
	}()

	numberOfLines, err := countLinesInFile(bufio.NewReader(file))
	if err != nil {
		return nil, fmt.Errorf("counting number of lines in the file: %w", err)
	}
	// Counting the lines moves the internal file pointer. Let's go back to the start because we want to read every line.
	if _, err := file.Seek(0, 0); err != nil {
		return nil, fmt.Errorf("seeking to the start of the input file after counting lines: %w", err)
	}

	maxLinesPerPartition := int64(math.Ceil(float64(numberOfLines) / float64(maxNumberOfPartitions)))

	partitionFilePaths = make([]string, 0, maxNumberOfPartitions)

	scanner := bufio.NewScanner(file)

	directory, _ := path.Split(filepath)

	for partitionNumber := range maxNumberOfPartitions {
		partitionFilePath := fmt.Sprintf("%s_input_%d", directory, partitionNumber)

		partitionFile, err := os.OpenFile(partitionFilePath, os.O_RDWR|os.O_CREATE, 0644)
		if err != nil {
			return nil, fmt.Errorf("creating/opening partition file: path=%s %w", partitionFilePath, err)
		}
		defer func() {
			if closeErr := partitionFile.Close(); closeErr != nil {
				err = errors.Join(err, fmt.Errorf("closing partition file: path=%s %w", partitionFilePath, closeErr))
			}
		}()

		for range maxLinesPerPartition {
			// Reached the end of the file.
			if !scanner.Scan() {
				if err := partitionFile.Sync(); err != nil {
					return nil, fmt.Errorf("syncing partition file: path=%s %w", partitionFilePath, err)
				}

				partitionFilePaths = append(partitionFilePaths, partitionFilePath)

				return partitionFilePaths, nil
			}

			if _, err := partitionFile.Write(scanner.Bytes()); err != nil {
				return nil, fmt.Errorf("writing to partition file: %w", err)
			}
			if _, err := partitionFile.Write([]byte{'\n'}); err != nil {
				return nil, fmt.Errorf("writing \\n to partition file: %w", err)
			}
		}

		if err := partitionFile.Sync(); err != nil {
			return nil, fmt.Errorf("syncing partition file: path=%s %w", partitionFilePath, err)
		}

		partitionFilePaths = append(partitionFilePaths, partitionFilePath)
	}

	return partitionFilePaths, nil
}

// Returns the number of lines in the reader by counting the number of times \n appears.
func countLinesInFile(reader io.Reader) (int, error) {
	const bufferSizeInBytes = 32 * 1024
	buffer := make([]byte, bufferSizeInBytes)

	count := 0

	lineSeparator := []byte{'\n'}

	for {
		bytesRead, err := reader.Read(buffer)
		count += bytes.Count(buffer[:bytesRead], lineSeparator)

		if errors.Is(err, io.EOF) {
			return count, nil
		}

		if err != nil {
			return count, fmt.Errorf("reading file contents: %w", err)
		}
	}
}
