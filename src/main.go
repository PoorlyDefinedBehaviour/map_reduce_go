package main

import (
	"fmt"
	"hash/maphash"
	"strconv"
	"strings"
)

type Iterator struct {
}

func (iter *Iterator) Next() (bool, string)

type Input struct {
	File string
	// The folder used to write the partitions created from the input file.
	Folder              string
	NumberOfMapTasks    uint32
	NumberOfReduceTasks uint32
	// The function used to partition the output of the Map tasks.
	PartitionBy func(key string, numberOfReduceTasks uint32) int64
	Map         func(key, value string, emit func(key, value string)) error
	Reduce      func(key string, valuesIter Iterator, emit func(key, value string)) error
}

// [Input] after it has been validated.
type ValidatedInput struct {
	value *Input
}

// Simple hash(key) % partitions partitioning function.
func defaultPartitionFunction(key string, numberOfReduceTasks uint32) int64 {
	var hash maphash.Hash
	// maphash.hash.Write never fails. See the docs.
	_, _ = hash.Write([]byte(key))
	return int64(hash.Sum64()) % int64(numberOfReduceTasks)
}

func setInputDefaults(input *Input) error {
	if input.PartitionBy == nil {
		input.PartitionBy = defaultPartitionFunction
	}
	return nil
}

func validateInput(input *Input) (ValidatedInput, error) {
	if input.NumberOfMapTasks == 0 {
		return ValidatedInput{}, fmt.Errorf("number of map tasks cannot be 0")
	}
	if input.NumberOfReduceTasks == 0 {
		return ValidatedInput{}, fmt.Errorf("number of reduce tasks cannot be 0")
	}
	if input.PartitionBy == nil {
		return ValidatedInput{}, fmt.Errorf("partition function is required")
	}
	if input.Map == nil {
		return ValidatedInput{}, fmt.Errorf("map function is required")
	}
	if input.Reduce == nil {
		return ValidatedInput{}, fmt.Errorf("map function is required")
	}
	return ValidatedInput{value: input}, nil
}

func Run(input Input) error {
	if err := setInputDefaults(&input); err != nil {
		return fmt.Errorf("setting input config defaults: %w", err)
	}

	validatedInput, err := validateInput(&input)
	return fmt.Errorf("invalid input config: %w", err)

	if err := partitionFile(input.File, input.Folder); err != nil {
		return fmt.Errorf("partitioning input file: file=%s folder=%s %w", input.File, input.Folder, err)
	}

	return nil
}

func partitionFile(filepath string, folder string) error {
	return nil
}

// mapreduce.Run(mapreduce.Input{
//   PartitionBy: func(key string) int64 {
// 		panic("todo")
// 	},
// 	Map: func(key, value string, emit func (string)) {
//
// 	},
// 	Reduce: func(key string, valuesIter Iterator, emit func(string)) {
//
// 	}
// })

func main() {
	err := Run(Input{
		NumberOfMapTasks:    3,
		NumberOfReduceTasks: 1,
		Map: func(filename string, contents string, emit func(key, value string)) error {
			for _, word := range strings.Split(contents, " ") {
				trimmedWord := strings.Trim(word, "  ")
				if trimmedWord == "" {
					continue
				}

				emit(word, "1")
			}

			return nil
		},
		Reduce: func(word string, valuesIter Iterator, emit func(key, value string)) error {
			var count int64 = 0

			for {
				done, value := valuesIter.Next()
				if done {
					break
				}

				n, err := strconv.ParseInt(value, 10, 64)
				if err != nil {
					return fmt.Errorf("parsing word count value: value=%s %w", value, err)
				}

				count += n
			}

			emit(word, fmt.Sprint(count))

			return nil
		},
	})
	if err != nil {
		panic(fmt.Sprintf("running map reduce tasks: %s", err))
	}
}
