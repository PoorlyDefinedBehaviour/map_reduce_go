package partitioning

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path"
)

type LinePartitioner struct{}

func NewLinePartitioner() *LinePartitioner {
	return &LinePartitioner{}
}

func (partitioner *LinePartitioner) Partition(filePath string, outputFolder string, maxNumberOfPartitions uint32) (partitionFilePaths []string, err error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("opening input file: filepath=%s %w", filePath, err)
	}
	defer func() {
		if closeErr := file.Close(); closeErr != nil {
			err = errors.Join(err, fmt.Errorf("closing input file: filepath=%s %w", filePath, closeErr))
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
	if numberOfLines == 0 {
		return nil, nil
	}

	if _, err = os.Stat(outputFolder); err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			return nil, fmt.Errorf("calling stat on the output folder: %w", err)
		}

		if err := os.MkdirAll(outputFolder, 0755); err != nil {
			return nil, fmt.Errorf("creating folder for intermediary files: %w", err)
		}
		folder, err := os.Open(outputFolder)
		if err != nil {
			return nil, fmt.Errorf("opening output folder: %w", err)
		}
		if err := folder.Sync(); err != nil {
			return nil, fmt.Errorf("syncinf output folder: %w", err)
		}
	}

	maxLinesPerPartition := int64(math.Ceil(float64(numberOfLines) / float64(maxNumberOfPartitions)))

	partitionFilePaths = make([]string, 0, maxNumberOfPartitions)

	scanner := bufio.NewScanner(file)

	for partitionNumber := range maxNumberOfPartitions {
		var partitionFile *os.File
		partitionFilePath := path.Join(outputFolder, fmt.Sprintf("input_%d", partitionNumber))

		for range maxLinesPerPartition {
			// Reached the end of the file.
			if !scanner.Scan() {
				return partitionFilePaths, nil
			}

			if partitionFile == nil {

				partitionFile, err = os.OpenFile(partitionFilePath, os.O_RDWR|os.O_CREATE, 0644)
				if err != nil {
					return nil, fmt.Errorf("creating/opening partition file: path=%s %w", partitionFilePath, err)
				}
				defer func() {
					if syncErr := partitionFile.Sync(); syncErr != nil {
						err = errors.Join(err, fmt.Errorf("syncing partition file: path=%s %w", partitionFilePath, syncErr))
					}

					if closeErr := partitionFile.Close(); closeErr != nil {
						err = errors.Join(err, fmt.Errorf("closing partition file: path=%s %w", partitionFilePath, closeErr))
					}
				}()
			}

			if _, err := partitionFile.Write(scanner.Bytes()); err != nil {
				return nil, fmt.Errorf("writing to partition file: %w", err)
			}
			if _, err := partitionFile.Write([]byte{'\n'}); err != nil {
				return nil, fmt.Errorf("writing \\n to partition file: %w", err)
			}
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
	state := 0

	for {
		bytesRead, err := reader.Read(buffer)

		for _, b := range buffer[:bytesRead] {

			switch state {
			// Looking for a value that's not a new line.
			case 0:
				if b != '\n' {
					count++
					state = 1
				}

			// Looking for new line.
			case 1:
				if b == '\n' {
					state = 0
				}

			default:
				panic(fmt.Sprintf("unknown state: %d", state))
			}
		}

		if err != nil {
			if errors.Is(err, io.EOF) {
				return count, nil
			}
			return count, fmt.Errorf("reading file contents: %w", err)
		}
	}
}
