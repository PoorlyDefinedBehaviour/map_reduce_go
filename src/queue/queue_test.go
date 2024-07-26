package queue

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestQueue(t *testing.T) {
	t.Parallel()

	t.Run("basic", func(t *testing.T) {
		q := New[int]()
		_, ok := q.Pop()
		assert.False(t, ok)

		q.Add(3)

		v, ok := q.Pop()
		assert.Equal(t, 3, v)
		assert.True(t, ok)

		_, ok = q.Pop()
		assert.False(t, ok)

		q.Add(3)
		q.Add(2)
		q.Add(1)

		v, ok = q.Pop()
		assert.Equal(t, 3, v)
		assert.True(t, ok)

		v, ok = q.Pop()
		assert.Equal(t, 2, v)
		assert.True(t, ok)

		v, ok = q.Pop()
		assert.Equal(t, 1, v)
		assert.True(t, ok)

		_, ok = q.Pop()
		assert.False(t, ok)
	})

	t.Run("Len", func(t *testing.T) {
		t.Parallel()

		panic("TODO")
	})

	t.Run("model", func(t *testing.T) {
		t.Parallel()

		panic("TODO")
	})
}
