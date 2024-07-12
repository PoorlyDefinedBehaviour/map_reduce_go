package sorter

import (
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSort(t *testing.T) {
	t.Parallel()

	s := LineSorter{}

	cases := []struct {
		input    string
		expected string
	}{
		{
			input:    "",
			expected: "",
		},
		{
			input:    "1",
			expected: "1",
		},
		{
			input:    "b a",
			expected: "b a",
		},
		{
			input:    "c 1\nb 1\na 1",
			expected: "a 1\nb 1\nc 1",
		},
	}

	for _, tt := range cases {
		reader, err := s.Sort(strings.NewReader(tt.input))
		require.NoError(t, err)

		data, err := io.ReadAll(reader)
		require.NoError(t, err)

		assert.Equal(t, tt.expected, string(data))
	}
}
