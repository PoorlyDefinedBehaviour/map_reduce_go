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

	_, err = writer.Write([]byte("11111"))
	require.NoError(t, err)

	_, err = writer.Write([]byte("222"))
	require.NoError(t, err)

	_, err = writer.Write([]byte("333333"))
	require.NoError(t, err)

	require.NoError(t, writer.Close())

	// 3 files should be created to store the data.
	contents, err := os.ReadFile(fmt.Sprintf("%s/file_0", dir))
	require.NoError(t, err)
	assert.Equal(t, "11111", string(contents))

	contents, err = os.ReadFile(fmt.Sprintf("%s/file_1", dir))
	require.NoError(t, err)
	assert.Equal(t, "222", string(contents))

	contents, err = os.ReadFile(fmt.Sprintf("%s/file_2", dir))
	require.NoError(t, err)
	assert.Equal(t, "333333", string(contents))

	outputFiles := writer.OutputFiles()

	// Each file can have at most 5 bytes.
	// There are 13 bytes being written.
	// So 3 files are needed to store all the bytes.
	assert.Equal(t, 3, len(outputFiles))

	// "11111" has 5 bytes.
	assert.EqualValues(t, 5, outputFiles[0].SizeBytes)
	// "222" has 5 bytes.
	assert.EqualValues(t, 3, outputFiles[1].SizeBytes)
	// "333333" has 6 bytes.
	assert.EqualValues(t, 6, outputFiles[2].SizeBytes)

}
