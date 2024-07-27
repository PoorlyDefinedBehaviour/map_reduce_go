package slicesext

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"pgregory.net/rapid"
)

func TestGroupBy(t *testing.T) {
	t.Parallel()

	rapid.Check(t, func(t *rapid.T) {
		// Create N groups of integers:
		// Ex:
		// 1: [1 1 1 1]
		// 2: [2 2 2 2 2 2 2]
		// 3: [3]
		groups := make(map[int][]int, 0)

		for i := range rapid.IntRange(0, 10).Draw(t, "n") {
			groups[i] = rapid.SliceOfN(rapid.Just(i), 1, 10).Draw(t, "group")
		}

		// Put all the values in a slice
		xs := make([]int, 0)
		for _, values := range groups {
			xs = append(xs, values...)
		}

		// Group the values, expect the initial groups:
		// 1: [1 1 1 1]
		// 2: [2 2 2 2 2 2 2]
		// 3: [3]
		result := GroupBy(xs, func(x int) int { return x })

		assert.Equal(t, len(groups), len(result))

		// Ensure the expected groups were returned.
		for k, values := range groups {
			assert.Equal(t, values, result[k])
		}
	})
}

func TestMap(t *testing.T) {
	t.Parallel()

	panic("TODO")
}

func TestFind(t *testing.T) {
	t.Parallel()

	panic("TODO")
}
