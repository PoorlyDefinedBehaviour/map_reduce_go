package memory

import (
	"fmt"
	"strconv"
	"strings"
)

const (
	mib uint64 = 1048576
)

func ToBytes(memory string) (uint64, error) {
	if strings.HasSuffix(memory, "Mi") {
		n, err := strconv.ParseUint(strings.TrimSuffix(memory, "Mi"), 10, 64)

		if err != nil {
			return n, fmt.Errorf("parsing memory: %w", err)
		}
		return n * mib, nil
	}

	return 0, fmt.Errorf("unknown memory format: %s", memory)
}
