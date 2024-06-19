package main

import (
	"fmt"
	"os"

	"github.com/poorlydefinedbehaviour/map_reduce_go/src/config"
	"github.com/poorlydefinedbehaviour/map_reduce_go/src/grpc"
	"github.com/poorlydefinedbehaviour/map_reduce_go/src/httpserver"
	"github.com/poorlydefinedbehaviour/map_reduce_go/src/master"
	"github.com/poorlydefinedbehaviour/map_reduce_go/src/partitioning"
)

// func Run(input Input, master *master.Master, partitioner partitioning.Partitioner) error {
// 	validatedInput, err := validateInput(&input)
// 	if err != nil {
// 		return fmt.Errorf("invalid input config: %w", err)
// 	}

// 	partitionFilePaths, err := partitioner.Partition(validatedInput.value.File, input.Folder, validatedInput.value.NumberOfPartitions)
// 	if err != nil {
// 		return fmt.Errorf("partitioning input file: %w", err)
// 	}
// 	fmt.Printf("\n\naaaaaaa partitionFilePaths %+v\n\n", partitionFilePaths)

// 	// for _, filePath := range partitionFilePaths {
// 	// 	if err := master.Add(filePath); err != nil {
// 	// 		return fmt.Errorf("assigning Map tasks to workers: %w", err)
// 	// 	}
// 	// }

// 	// TODO: partition by user defined partition function after Map.

// 	return nil
// }

func main() {
	// if err := grpc.NewServer(grpc.ServerConfig{Port: 8001}).Start(); err != nil {
	// 	panic(err)
	// }

	cfg, err := config.Parse(os.Args)
	if err != nil {
		panic(err)
	}

	switch cfg.Mode {
	case config.WorkerMode:
	case config.MasterMode:
	default:
		panic(fmt.Sprintf("unexpected mode: %+v", cfg.Mode))
	}

	partitioner := partitioning.NewLinePartitioner()

	workers := make([]master.Worker, 0)
	for i := 1; i <= 3; i++ {
		client, err := grpc.NewClient(grpc.ClientConfig{Addr: "todo"})
		if err != nil {
			panic(fmt.Errorf("instantiating grpc client: %w", err))
		}
		workers = append(workers, master.Worker{ID: uint64(i), Client: client})
	}

	master, err := master.New(master.Config{NumberOfMapWorkers: 3, Workers: workers}, partitioner)
	if err != nil {
		panic(fmt.Errorf("instantiating master: %w", err))
	}

	httpServer := httpserver.New(master)
	fmt.Println("starting http server")
	if err := httpServer.Start(":8002"); err != nil {
		panic(fmt.Errorf("starting http server: %w", err))
	}

	// ctx := v8.NewContext()                                  // creates a new V8 context with a new Isolate aka VM
	// ctx.RunScript("const add = (a, b) => a + b", "math.js") // executes a script on the global context
	// ctx.RunScript("const result = add(3, 4)", "main.js")    // any functions previously added to the context can be called
	// val, _ := ctx.RunScript("result", "value.js")           // return a value in JavaScript back to Go
	// fmt.Printf("addition result: %s", val.String())

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
