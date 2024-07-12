package contracts

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestOutputFile_region(t *testing.T) {
	tests := []struct {
		filePath string
		expected uint32
	}{
		{
			filePath: "/contracts/region_1",
			expected: 1,
		},
		{
			filePath: "/contracts/region_10",
			expected: 10,
		},
	}
	for _, tt := range tests {
		f := &OutputFile{
			FileID:    1,
			FilePath:  tt.filePath,
			SizeBytes: 10,
		}

		assert.Equal(t, tt.expected, f.Region())
	}
}
