package javascript

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestClose(t *testing.T) {
	t.Parallel()

	script, err := newScript("const map = (filename, contents, emit) => {}")
	require.NoError(t, err)

	assert.NotPanics(t, func() { script.Close() })
}

func TestPartition(t *testing.T) {
	t.Parallel()

	jsScript := `
const partition = (key, r) => {
  let hash = 0

  for (const char of key) {
    hash ^= char.charCodeAt(0)
  }

  return hash % r
}
	`

	script, err := newScript(jsScript)
	require.NoError(t, err)

	partition, err := script.Partition("key", 5)
	require.NoError(t, err)
	assert.True(t, partition >= 0 && partition < 5)
}

func TestMap(t *testing.T) {
	t.Parallel()

	jsScript := `
function map(filename, contents, emit) {
	emit(filename, contents)
}
	`

	script, err := newScript(jsScript)
	require.NoError(t, err)

	var (
		key   string
		value string
	)

	filename := "file"
	contents := "contents"

	require.NoError(t, script.Map(filename, contents, func(k, v string) error {
		key = k
		value = v
		return nil
	}))

	assert.Equal(t, filename, key)
	assert.Equal(t, contents, value)
}

func TestReduce(t *testing.T) {
	t.Parallel()

	jsScript := `
const reduce = (word, nextValueIter, emit) => {
  let count = 0

  while (true) {
    const [value, done] = nextValueIter()
    if (done) {
      break
    }

    count += Number(value)
  }

  emit(word, count.toString())
}
	`

	script, err := newScript(jsScript)
	require.NoError(t, err)

	var (
		key   string
		value string
	)

	iterator := newIterator([]string{"1", "1", "1"})

	require.NoError(t, script.Reduce("word", iterator.next, func(k, v string) error {
		key = k
		value = v
		return nil
	}))

	assert.Equal(t, "word", key)
	assert.Equal(t, "3", value)
}

type iterator struct {
	i      int
	values []string
}

func newIterator(values []string) iterator {
	return iterator{i: 0, values: values}
}

func (iterator *iterator) next() (string, bool) {
	if iterator.i < len(iterator.values) {
		value := iterator.values[iterator.i]
		iterator.i++
		return value, false
	}
	return "", true
}
