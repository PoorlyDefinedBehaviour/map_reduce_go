package assert

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestEqual(t *testing.T) {
	t.Parallel()

	t.Run("fails", func(t *testing.T) {
		t.Parallel()

		assert.PanicsWithValue(t, "expected '2' to equal '1': hello world", func() {
			Equal(1, 2, "hello world")
		})

		assert.PanicsWithValue(t, "expected 'b' to equal 'a'", func() {
			Equal("a", "b")
		})
	})

	t.Run("pass", func(t *testing.T) {
		t.Parallel()

		assert.NotPanics(t, func() {
			Equal(1, 1)
			Equal(1, 1, "custom message")
		})
	})
}

func TestTrue(t *testing.T) {
	t.Parallel()

	t.Run("fails", func(t *testing.T) {
		t.Parallel()

		assert.PanicsWithValue(t, "expected 'false' to equal 'true': hello world", func() {
			True(1 == 2, "hello world")
		})

		assert.PanicsWithValue(t, "expected 'false' to equal 'true'", func() {
			True("a" == "b")
		})
	})

	t.Run("pass", func(t *testing.T) {
		t.Parallel()

		assert.NotPanics(t, func() {
			True(true)
			True(true, "custom message")
		})
	})
}
