const config = {
  file: "./dev/input_word_count.txt",
  folder: "/tmp",
  numberOfPartitions: 3,
  numberOfMapTasks: 3,
  numberOfReduceTasks: 1,
}

const map = (filename, contents, emit) => {
  for (const word of contents.split(" ")) {
    const trimmedWord = word.trim()
    if (!trimmedWord) {
      continue
    }

    emit(word, "1")
  }
}

const reduce = (word, valuesIter, emit) => {
  let count = 0

  while (true) {
    const { done, value } = valuesIter.next()
    if (done) {
      break
    }

    count += Number(value)
  }

  emit(word, count.toString())
}

