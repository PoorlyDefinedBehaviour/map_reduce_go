package sorter

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"slices"
	"strings"

	"github.com/poorlydefinedbehaviour/map_reduce_go/src/iterator"
)

type LineSorter struct{}

func NewLineSorter() LineSorter {
	return LineSorter{}
}

func (s LineSorter) Sort(in io.Reader) (io.Reader, error) {
	parts := make([]string, 0)
	scanner := bufio.NewScanner(in)

	for scanner.Scan() {
		parts = append(parts, scanner.Text())
	}

	slices.SortFunc(parts, func(a, b string) int {
		if a < b {
			return -1
		}
		if a > b {
			return 1
		}

		return 0
	})

	return strings.NewReader(strings.Join(parts, "\n")), nil
}

func (s LineSorter) SortAndMerge(a, b io.Reader, out io.Writer) error {
	iterA := iterator.NewLineIterator(a)
	iterB := iterator.NewLineIterator(b)

	for {
		nextA, doneA := iterA.Peek()

		nextB, doneB := iterB.Peek()

		var value []byte

		if !doneA && !doneB {
			if bytes.Compare(nextA, nextB) <= 0 {
				_, _ = iterA.Next()
				value = nextA
			} else {
				_, _ = iterB.Next()
				value = nextB
			}
		} else if !doneA {
			_, _ = iterA.Next()
			value = nextA
		} else if !doneB {
			_, _ = iterB.Next()
			value = nextB
		} else {
			break
		}

		bytesWritten, err := out.Write(value)
		if err != nil {
			return fmt.Errorf("writing value to writer: %w", err)
		}
		if bytesWritten != len(value) {
			return fmt.Errorf("unable to write all bytes: bytesWritten=%d expectedBytesWritten=%d", bytesWritten, len(value))
		}

		_, doneA = iterA.Peek()
		_, doneB = iterB.Peek()

		// If there's more data to write, write a new line.
		if !doneA || !doneB {
			buffer := []byte("\n")
			bytesWritten, err = out.Write(buffer)
			if err != nil {
				return fmt.Errorf("writing value to writer: %w", err)
			}
			if bytesWritten != len(buffer) {
				return fmt.Errorf("unable to write all bytes: bytesWritten=%d expectedBytesWritten=%d", bytesWritten, len(buffer))
			}
		}
	}

	return nil
}
