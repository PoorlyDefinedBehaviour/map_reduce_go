package iterator

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLineIterator(t *testing.T) {
	t.Parallel()

	t.Run("basic", func(t *testing.T) {
		t.Parallel()

		it := NewLineIterator(strings.NewReader("a\nb\nc"))

		buffer, done := it.Peek()
		assert.False(t, done)
		assert.Equal(t, "a", string(buffer))

		// Peeking does not advance the reader so it should return the same value as before.
		buffer, done = it.Peek()
		assert.False(t, done)
		assert.Equal(t, "a", string(buffer))

		// Get the value and advance the reader.
		buffer, done = it.Next()
		assert.False(t, done)
		assert.Equal(t, "a", string(buffer))

		// Peeking should return the next value now.
		buffer, done = it.Peek()
		assert.False(t, done)
		assert.Equal(t, "b", string(buffer))

		// Get the next values.
		buffer, done = it.Next()
		assert.False(t, done)
		assert.Equal(t, "b", string(buffer))

		buffer, done = it.Next()
		assert.False(t, done)
		assert.Equal(t, "c", string(buffer))

		// EOF reached.
		buffer, done = it.Peek()
		assert.Empty(t, buffer)
		assert.True(t, done)

		buffer, done = it.Next()
		assert.Empty(t, buffer)
		assert.True(t, done)
	})

	t.Run("model", func(t *testing.T) {
		t.Parallel()

		panic("TODO")
	})
}
