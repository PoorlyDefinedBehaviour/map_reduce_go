package memory

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestToBytes(t *testing.T) {
	t.Parallel()

	cases := []struct {
		input          string
		expectedOutput uint64
		expectedErr    string
	}{
		{
			input:          "1Mi",
			expectedOutput: 1048576,
		},
		{
			input:          "2Mi",
			expectedOutput: 2097152,
		},
		{
			input:          "3Mi",
			expectedOutput: 3145728,
		},
		{
			input:          "4Mi",
			expectedOutput: 4194304,
		},
		{
			input:          "5Mi",
			expectedOutput: 5242880,
		},
		{
			input:          "6Mi",
			expectedOutput: 6291456,
		},
		{
			input:          "7Mi",
			expectedOutput: 7340032,
		},
		{
			input:          "8Mi",
			expectedOutput: 8388608,
		},
		{
			input:          "9Mi",
			expectedOutput: 9437184,
		},
		{
			input:          "10Mi",
			expectedOutput: 10485760,
		},
		{
			input:       "10Gi",
			expectedErr: "unknown memory format: 10Gi",
		},
	}

	for _, tt := range cases {
		t.Run(tt.input, func(t *testing.T) {
			n, err := ToBytes(tt.input)

			if tt.expectedErr == "" {
				assert.Equal(t, tt.expectedOutput, n)
				require.NoError(t, err)
			} else {
				assert.Equal(t, tt.expectedErr, err.Error())
			}
		})
	}
}
