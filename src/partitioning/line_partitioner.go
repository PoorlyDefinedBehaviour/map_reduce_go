package partitioning

import (
	"bufio"
	"bytes"
	"fmt"
	"os"
	"path"
)

type LinePartitioner struct{}

func NewLinePartitioner() LinePartitioner {
	return LinePartitioner{}
}

func (partitioner *LinePartitioner) Partition(filepath string, maxNumberOfPartitions uint32) ([]string, error) {
	file, err := os.Open(filepath)
	if err != nil {
		return nil, fmt.Errorf("opening input file: filepath=%s %w", filepath, err)
	}
	fileInfo, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("getting file stats: %w", err)
	}

	partitionSize := fileInfo.Size() / int64(maxNumberOfPartitions)
	partitionFilePaths := make([]string, 0, maxNumberOfPartitions)

	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, partitionSize), int(partitionSize))

	directory, _ := path.Split(filepath)

	for i := 0; i < int(maxNumberOfPartitions); i++ {
		partitionBuffer := bytes.NewBuffer(make([]byte, 0, partitionSize))

		for scanner.Scan() {
			line := scanner.Text()

			if partitionBuffer.Len()+len(line) > int(partitionSize) {
				// TOOD: the last scanned `line` is being lost
				break
			}
			if partitionBuffer.Len()+len(line) <= int(partitionSize) {
				_, err := partitionBuffer.WriteString(line)
				if err != nil {
					return nil, fmt.Errorf("writing to parititon buffer: %w", err)
				}
			}
		}

		if partitionBuffer.Len() > 0 {
			partitionFilePath := fmt.Sprintf("%s_input_%d", directory, i)
			partitionFilePaths = append(partitionFilePaths, partitionFilePath)
			partitionFile, err := os.OpenFile(partitionFilePath, os.O_RDWR|os.O_CREATE, 0644)
			if err != nil {
				return nil, fmt.Errorf("creating/opening partition file: filepath=%s %w", partitionFilePath, err)
			}
			bytesWritten, err := partitionFile.Write(partitionBuffer.Bytes())
			if err != nil {
				return nil, fmt.Errorf("writing to input partition file: bytesWritten=%d %w", bytesWritten, err)
			}
		}
	}

	return partitionFilePaths, nil
}
