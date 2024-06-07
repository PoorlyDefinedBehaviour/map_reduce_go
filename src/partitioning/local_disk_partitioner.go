package partitioning

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path"
)

type LocalDiskPartitioner struct{}

func (partitioner *LocalDiskPartitioner) Partition(filepath string, maxPartitionSizeInBytes uint64, maxNumberOfPartitions uint32) error {
	file, err := os.Open(filepath)
	if err != nil {
		return fmt.Errorf("opening input file: filepath=%s %w", filepath, err)
	}

	directory, _ := path.Split(filepath)

	for i := 0; i < int(maxNumberOfPartitions); i++ {
		buffer := make([]byte, maxPartitionSizeInBytes)
		bytesRead, err := file.Read(buffer)
		buffer = buffer[:bytesRead]

		if err == nil || errors.Is(err, io.EOF) && len(buffer) > 0 {
			partitionFilePath := fmt.Sprintf("%s_input_%d", directory, i)
			partitionFile, err := os.OpenFile(partitionFilePath, os.O_RDWR|os.O_CREATE, 0644)
			if err != nil {
				return fmt.Errorf("creating/opening partition file: filepath=%s %w", partitionFilePath, err)
			}
			bytesWritten, err := partitionFile.Write(buffer)
			if err != nil {
				return fmt.Errorf("writing to input partition file: bytesWritten=%d %w", bytesWritten, err)
			}
		}

		if err != nil {
			return fmt.Errorf("reading from file into buffer: bytesRead=%d %w", bytesRead, err)
		}
	}

	return nil
}
