package sorter

import (
	"bufio"
	"io"
	"slices"
	"strings"
)

type LineSorter struct{}

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
