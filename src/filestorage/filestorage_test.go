package filestorage

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/poorlydefinedbehaviour/map_reduce_go/src/testingext"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewWriter(t *testing.T) {
	t.Parallel()

	dir := testingext.TempDir()

	writer, err := New().NewWriter(context.Background(), dir)
	require.NoError(t, err)

	data := []struct {
		key    string
		value  string
		region uint32
	}{
		{
			key:    "a",
			value:  "1",
			region: 0,
		},
		{
			key:    "b",
			value:  "2",
			region: 0,
		},
		{
			key:    "a",
			value:  "1",
			region: 1,
		},
		{
			key:    "a",
			value:  "1",
			region: 2,
		},
		{
			key:    "b",
			value:  "2",
			region: 2,
		},
		{
			key:    "c",
			value:  "3",
			region: 2,
		},
	}

	for _, input := range data {
		require.NoError(t, writer.WriteKeyValue(input.key, input.value, input.region))
	}

	require.NoError(t, writer.Close())

	assert.Equal(t, "a,1\nb,2", testingext.MustReadFile(t, filepath.Join(dir, "region_0")))
	assert.Equal(t, "a,1", testingext.MustReadFile(t, filepath.Join(dir, "region_1")))
	assert.Equal(t, "a,1\nb,2\nc,3", testingext.MustReadFile(t, filepath.Join(dir, "region_2")))
}
