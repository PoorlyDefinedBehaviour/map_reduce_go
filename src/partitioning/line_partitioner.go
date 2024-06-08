package partitioning

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
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
			err = errors.Join(err, fmt.Errorf("closing input file: filepath=%s %w", filepath, err))
		}
	}()
	fileInfo, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("getting file stats: %w", err)
	}

	// partition per lines per file
	// create n partition files
	// write lines to files in round robin order

	partitionSizeInBytes := int64(math.Ceil(float64(fileInfo.Size()+int64(maxNumberOfPartitions)) / float64(maxNumberOfPartitions)))
	partitionFilePaths = make([]string, 0, maxNumberOfPartitions)

	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, partitionSizeInBytes), int(partitionSizeInBytes))

	directory, _ := path.Split(filepath)

	partitionBuffer := bytes.NewBuffer(make([]byte, 0, partitionSizeInBytes))
	partitionNumber := 0

	for scanner.Scan() {
		line := scanner.Text()

		// Partition buffer is full, write to disk.
		// + 1 for the new line that will be added at the end.
		if partitionBuffer.Len()+len(line)+1 > int(partitionSizeInBytes) && partitionNumber < int(maxNumberOfPartitions) {
			partitionFilePath, err := writePartitionFile(directory, partitionNumber, partitionBuffer.Bytes())
			if err != nil {
				return partitionFilePaths, fmt.Errorf("writing partition file to disk: %w", err)
			}
			partitionBuffer.Reset()
			partitionFilePaths = append(partitionFilePaths, partitionFilePath)
			partitionNumber++
		}

		if _, err := partitionBuffer.WriteString(line); err != nil {
			return nil, fmt.Errorf("writing to parititon buffer: %w", err)
		}
		if err := partitionBuffer.WriteByte('\n'); err != nil {
			return nil, fmt.Errorf("writing new line to partition buffer: %w", err)
		}
	}

	// There's some data in the partition buffer, write to disk.
	if partitionBuffer.Len() > 0 {
		partitionFilePath, err := writePartitionFile(directory, partitionNumber, partitionBuffer.Bytes())
		if err != nil {
			return partitionFilePaths, fmt.Errorf("writing partition file to disk: %w", err)
		}
		partitionBuffer.Reset()
		partitionFilePaths = append(partitionFilePaths, partitionFilePath)
	}

	return partitionFilePaths, nil
}

func writePartitionFile(directory string, partitionNumber int, buffer []byte) (partitionFilePath string, err error) {
	partitionFilePath = fmt.Sprintf("%s_input_%d", directory, partitionNumber)

	partitionFile, err := os.OpenFile(partitionFilePath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return partitionFilePath, fmt.Errorf("creating/opening partition file: filepath=%s %w", partitionFilePath, err)
	}
	defer func() {
		if closeErr := partitionFile.Close(); closeErr != nil {
			err = errors.Join(err, fmt.Errorf("closing partiiton file: %w", closeErr))
		}
	}()
	bytesWritten, err := partitionFile.Write(buffer)
	if err != nil {
		return partitionFilePath, fmt.Errorf("writing to input partition file: bytesWritten=%d %w", bytesWritten, err)
	}
	if err := partitionFile.Sync(); err != nil {
		return partitionFilePath, fmt.Errorf("syncing partition file: filepath=%s %w", partitionFilePath, err)
	}

	return partitionFilePath, nil
}
