package javascript

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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

	require.NoError(t, script.Map(filename, contents, func(k, v string) {
		key = k
		value = v
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

	require.NoError(t, script.Reduce("word", iterator.next, func(k, v string) {
		key = k
		value = v
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
