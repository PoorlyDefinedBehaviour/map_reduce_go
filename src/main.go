package main

import (
	"fmt"
	"hash/maphash"

	"github.com/poorlydefinedbehaviour/map_reduce_go/src/httpserver"
	"github.com/poorlydefinedbehaviour/map_reduce_go/src/master"
	"github.com/poorlydefinedbehaviour/map_reduce_go/src/partitioning"
)

type Iterator struct {
}

func (iter *Iterator) Next() (bool, string) {
	panic("todo")
}

type Input struct {
	File string
	// The folder used to write the partitions created from the input file.
	Folder              string
	NumberOfMapTasks    uint32
	NumberOfReduceTasks uint32
	NumberOfPartitions  uint32
	Map                 func(key, value string, emit func(key, value string)) error
	Reduce              func(key string, valuesIter Iterator, emit func(key, value string)) error
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

func validateInput(input *Input) (ValidatedInput, error) {
	if input.File == "" {
		return ValidatedInput{}, fmt.Errorf("input file path is required")
	}
	if input.Folder == "" {
		return ValidatedInput{}, fmt.Errorf("folder to store intermediary files is required")
	}
	if input.NumberOfMapTasks == 0 {
		return ValidatedInput{}, fmt.Errorf("number of map tasks cannot be 0")
	}
	if input.NumberOfReduceTasks == 0 {
		return ValidatedInput{}, fmt.Errorf("number of reduce tasks cannot be 0")
	}
	if input.Map == nil {
		return ValidatedInput{}, fmt.Errorf("map function is required")
	}
	if input.Reduce == nil {
		return ValidatedInput{}, fmt.Errorf("map function is required")
	}
	return ValidatedInput{value: input}, nil
}

func Run(input Input, master *master.Master, partitioner partitioning.Partitioner) error {
	validatedInput, err := validateInput(&input)
	if err != nil {
		return fmt.Errorf("invalid input config: %w", err)
	}

	partitionFilePaths, err := partitioner.Partition(validatedInput.value.File, input.Folder, validatedInput.value.NumberOfPartitions)
	if err != nil {
		return fmt.Errorf("partitioning input file: %w", err)
	}
	fmt.Printf("\n\naaaaaaa partitionFilePaths %+v\n\n", partitionFilePaths)

	// for _, filePath := range partitionFilePaths {
	// 	if err := master.Add(filePath); err != nil {
	// 		return fmt.Errorf("assigning Map tasks to workers: %w", err)
	// 	}
	// }

	// TODO: partition by user defined partition function after Map.

	return nil
}

func main() {
	// if err := grpc.NewServer(grpc.ServerConfig{Port: 8001}).Start(); err != nil {
	// 	panic(err)
	// }

	httpServer := httpserver.New(nil)
	if err := httpServer.Start(":8002"); err != nil {
		panic(err)
	}

	ctx := v8.NewContext()                                  // creates a new V8 context with a new Isolate aka VM
	ctx.RunScript("const add = (a, b) => a + b", "math.js") // executes a script on the global context
	ctx.RunScript("const result = add(3, 4)", "main.js")    // any functions previously added to the context can be called
	val, _ := ctx.RunScript("result", "value.js")           // return a value in JavaScript back to Go
	fmt.Printf("addition result: %s", val.String())

	/*
		master, err := master.New(master.Config{NumberOfMapWorkers: 3, Workers: []master.WorkerID{1, 2, 3}})
		if err != nil {
			panic(fmt.Errorf("instantiating master: %w", err))
		}

		err = Run(Input{
			File:                "./dev/input_word_count.txt",
			Folder:              "./tmp",
			NumberOfPartitions:  3,
			NumberOfMapTasks:    3,
			NumberOfReduceTasks: 1,
			Map: func(filename string, contents string, emit func(key, value string)) error {
				fmt.Printf("\n\naaaaaaa Map: filename %+v\n\n", filename)
				for _, word := range strings.Split(contents, " ") {
					trimmedWord := strings.Trim(word, " ")
					if trimmedWord == "" {
						continue
					}

					emit(word, "1")
				}

				return nil
			},
			Reduce: func(word string, valuesIter Iterator, emit func(key, value string)) error {
				var count int64

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
		},
			master,
			partitioning.NewLinePartitioner(),
		)

		if err != nil {
			panic(fmt.Sprintf("running map reduce tasks: %s", err))
		}
	*/
}
