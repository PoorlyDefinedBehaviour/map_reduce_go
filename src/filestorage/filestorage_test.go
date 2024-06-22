package filestorage

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewWriter(t *testing.T) {
	t.Parallel()

	storage := New()

	dir := fmt.Sprintf("%s/%s", os.TempDir(), uuid.NewString())

	// Given a max file size of 5 bytes.
	maxFileSizeBytes := uint64(5)
	writer, err := storage.NewWriter(context.Background(), maxFileSizeBytes, dir)
	require.NoError(t, err)

	// When 15 bytes are written.
	_, err = writer.Write([]byte("11111"))
	require.NoError(t, err)

	_, err = writer.Write([]byte("22222"))
	require.NoError(t, err)

	_, err = writer.Write([]byte("33333"))
	require.NoError(t, err)

	require.NoError(t, writer.Close())

	// 3 files should be created to store the data.
	contents, err := os.ReadFile(fmt.Sprintf("%s/file_0", dir))
	require.NoError(t, err)
	assert.Equal(t, "11111", string(contents))

	contents, err = os.ReadFile(fmt.Sprintf("%s/file_1", dir))
	require.NoError(t, err)
	assert.Equal(t, "22222", string(contents))

	contents, err = os.ReadFile(fmt.Sprintf("%s/file_2", dir))
	require.NoError(t, err)
	assert.Equal(t, "33333", string(contents))
}
