package memory

import (
	"fmt"
	"strconv"
	"strings"
)

const (
	Mib uint64 = 1048576
)

func FromStringToBytes(memory string) (uint64, error) {
	if memory == "" {
		return 0, fmt.Errorf("memory is required")
	}

	if strings.HasSuffix(memory, "Mi") {
		n, err := strconv.ParseUint(strings.TrimSuffix(memory, "Mi"), 10, 64)

		if err != nil {
			return n, fmt.Errorf("parsing memory: %w", err)
		}
		return n * Mib, nil
	}

	return 0, fmt.Errorf("unknown memory format: '%s'", memory)
}
