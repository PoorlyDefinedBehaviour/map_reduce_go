const partition = () => {
  // TODO: use this function to partition map output into R regions.
}

const map = (filename, contents, emit) => {
  for (const word of contents.split(/\s+/)) {
    const trimmedWord = word.trim()
    if (!trimmedWord) {
      continue
    }

    emit(word, "1")
  }
}

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

