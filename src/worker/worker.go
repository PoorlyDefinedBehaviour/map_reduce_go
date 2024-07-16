package worker

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path"
	"path/filepath"
	"sync"
	"time"

	"github.com/poorlydefinedbehaviour/map_reduce_go/src/contracts"
	"github.com/poorlydefinedbehaviour/map_reduce_go/src/interpreters/javascript"
	"github.com/poorlydefinedbehaviour/map_reduce_go/src/iterator"
	"github.com/poorlydefinedbehaviour/map_reduce_go/src/tracing"
	"golang.org/x/sync/errgroup"
)

type Worker struct {
	mu           *sync.Mutex
	config       Config
	masterClient contracts.MasterClient
	fileStorage  contracts.FileStorage
	clock        contracts.Clock
	partitioner  contracts.Partitioner
	sorter       contracts.Sorter
}

type Config struct {
	// The worker address. Used to register the worker with the master.
	Addr string
	// The folder to put task outputs.
	WorkspaceFolder string
	// The maximum number of bytes each output file can have.
	MaxFileSizeBytes uint64
	// The amount of memory this worker has available
	MemoryAvailable uint64
	// How long to wait for between sending heartbeat requests.
	HeartbeatInterval time.Duration
	// How long to wait for a heartbeat request to complete.
	HeartbeatTimeout time.Duration
	// How long to wait for a request to let the master know which map tasks have been completed to complete.
	MapTasksCompletedTimeout time.Duration
	// The max number of files that can be downloaded at the same time.
	MaxInflightFileDownloads uint32
	// The max number of bytes that can be loaded in into memory when sorting the input files of a reduce task.
	ExternalSortMaxMemoryBytes uint64
}

func New(
	config Config,
	masterClient contracts.MasterClient,
	fileStorage contracts.FileStorage,
	clock contracts.Clock,
	partitioner contracts.Partitioner,
	sorter contracts.Sorter) (*Worker, error) {
	if config.Addr == "" {
		return nil, fmt.Errorf("addr is required")
	}
	if config.WorkspaceFolder == "" {
		return nil, fmt.Errorf("workspace folder is required")
	}
	if config.MaxFileSizeBytes == 0 {
		return nil, fmt.Errorf("max file size in bytes is required")
	}
	if config.HeartbeatInterval == 0 {
		return nil, fmt.Errorf("heartbeat interval is required")
	}
	if config.HeartbeatTimeout == 0 {
		return nil, fmt.Errorf("heartbeat timeout request is required")
	}
	if config.MapTasksCompletedTimeout == 0 {
		return nil, fmt.Errorf("map tasks completed request timeout is required")
	}
	if config.MaxInflightFileDownloads == 0 {
		return nil, fmt.Errorf("max inflight file downloads is required")
	}
	if config.ExternalSortMaxMemoryBytes == 0 {
		return nil, fmt.Errorf("external sort max memory bytes is required")
	}
	return &Worker{
		mu:           &sync.Mutex{},
		config:       config,
		masterClient: masterClient,
		fileStorage:  fileStorage,
		clock:        clock,
		partitioner:  partitioner,
		sorter:       sorter,
	}, nil
}

// TODO: move to IO driver
func (worker *Worker) HeartbeatControlLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			if err := worker.sendHeartbeat(ctx); err != nil {
				tracing.Error(ctx, "sending heartbeat to master, will sleep before retrying", "err", err)
			}
			worker.clock.Sleep(worker.config.HeartbeatInterval)
		}
	}
}

func (worker *Worker) OnReduceTaskReceived(ctx context.Context, task contracts.ReduceTask) error {
	worker.mu.Lock()
	defer worker.mu.Unlock()

	// The folder where the input files will be downloaded to.
	folder := path.Join(worker.config.WorkspaceFolder, fmt.Sprint(task.ID), "reduce")
	unsortedFolder := path.Join(folder, "unsorted")
	if err := os.MkdirAll(unsortedFolder, 0755); err != nil {
		return fmt.Errorf("creating directory to store unsorted reduce input files: %w", err)
	}

	var wg errgroup.Group
	wg.SetLimit(int(worker.config.MaxInflightFileDownloads))
	downloadedFiles := make([]contracts.File, len(task.Files))

	for i, file := range task.Files {
		i := i
		file := file
		wg.Go(func() error {
			file, err := worker.downloadFile(ctx, unsortedFolder, file)
			if err != nil {
				return fmt.Errorf("downloading file: path=%s %w", file.Path, err)
			}
			downloadedFiles[i] = file
			return nil
		})
	}

	// Wait for the files to be downloaded
	if err := wg.Wait(); err != nil {
		return fmt.Errorf("downloading reduce task files: %w", err)
	}

	sortedFolder := path.Join(folder, "sorted")
	if err := os.MkdirAll(sortedFolder, 0755); err != nil {
		return fmt.Errorf("creating directory to store sorted reduce input files: %w", err)
	}

	filePaths, err := worker.splitFiles(sortedFolder, downloadedFiles)
	if err != nil {
		return fmt.Errorf("splitting files: %w", err)
	}

	if err := worker.sortReduceInputFiles(filePaths); err != nil {
		return fmt.Errorf("soring reduce input files: %w", err)
	}

	return nil
}
func (worker *Worker) downloadFile(ctx context.Context, folder string, file contracts.File) (newFile contracts.File, err error) {
	downloadedFilePath := path.Join(folder, fmt.Sprintf("input_%d", file.FileID))
	f, err := os.OpenFile(downloadedFilePath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return newFile, fmt.Errorf("creating/opening file: path=%s %w", file.Path, err)
	}

	defer func() {
		if syncErr := f.Sync(); syncErr != nil {
			err = errors.Join(err, fmt.Errorf("syncing file: %w", syncErr))
		}
		if closeErr := f.Close(); closeErr != nil {
			err = errors.Join(err, fmt.Errorf("closing file: %w", closeErr))
		}
	}()
	reader, err := worker.fileStorage.NewReader(ctx, file.Path)
	if err != nil {
		return newFile, fmt.Errorf("reading file: %w", err)
	}
	defer func() {
		if closeErr := reader.Close(); closeErr != nil {
			err = errors.Join(err, fmt.Errorf("closing reader: %w", closeErr))
		}
	}()

	writer := bufio.NewWriter(f)
	defer func() {
		if closeErr := writer.Flush(); closeErr != nil {
			err = errors.Join(err, fmt.Errorf("flushing writer: %w", closeErr))
		}
	}()

	if _, err := io.Copy(writer, reader); err != nil {
		return newFile, fmt.Errorf("copying bytes to file: %w", err)
	}

	newFile = file
	newFile.Path = downloadedFilePath

	return newFile, nil
}

func (worker *Worker) sortReduceInputFiles(filePaths []string) error {
	wg := errgroup.Group{}

	files := make([]*os.File, 0, len(filePaths))

	// Sort every file by themselves.
	for _, filePath := range filePaths {
		filePath := filePath
		wg.Go(func() (err error) {

			file, err := os.Open(filePath)
			if err != nil {
				return fmt.Errorf("opening file: path=%s %w", filePath, err)
			}
			defer func() {
				if closeErr := file.Close(); closeErr != nil {
					err = errors.Join(err, fmt.Errorf("closing file: %w", closeErr))
				}
			}()

			reader, err := worker.sorter.Sort(file)
			if err != nil {
				return fmt.Errorf("sorting file: %w", err)
			}

			sortedFile, err := os.OpenFile(filepath.Join(filepath.Dir(filePath), fmt.Sprintf("sorted_temp_%s", filepath.Base(file.Name()))), os.O_RDWR|os.O_CREATE, 0644)
			if err != nil {
				return fmt.Errorf("creating sorted file: %w", err)
			}

			defer func() {
				if closeErr := sortedFile.Close(); closeErr != nil {
					err = errors.Join(err, fmt.Errorf("closing file: %w", closeErr))
				}
			}()

			if _, err := io.Copy(sortedFile, reader); err != nil {
				return fmt.Errorf("writing sorted data to file: %w", err)
			}

			if err := sortedFile.Sync(); err != nil {
				return fmt.Errorf("syncing file: %w", err)
			}

			if err := os.Rename(sortedFile.Name(), file.Name()); err != nil {
				return fmt.Errorf("renaming sorted file: %w", err)
			}

			files = append(files, sortedFile)

			return nil
		})
	}

	// Merge sorted files.
	for i := 0; i < len(files); i += 2 {
		a := files[i]
		b := files[i+1]

		sortedFile, err := os.OpenFile(filepath.Join(filepath.Dir(a.Name()), fmt.Sprintf("sorted_temp_%s", filepath.Base(a.Name()))), os.O_RDWR|os.O_CREATE, 0644)
		if err != nil {
			return fmt.Errorf("creating sorted file: %w", err)
		}

		defer func() {
			if closeErr := sortedFile.Close(); closeErr != nil {
				err = errors.Join(err, fmt.Errorf("closing file: %w", closeErr))
			}
		}()

		iterA := iterator.NewLineIterator(a)
		iterB := iterator.NewLineIterator(b)

		for {
			nextA, doneA := iterA.Peek()
			if err != nil {
				return fmt.Errorf("iterating file: path=%s %w", a.Name(), err)
			}

			nextB, doneB := iterB.Peek()
			if err != nil {
				return fmt.Errorf("iterating file: path=%s %w", b.Name(), err)
			}

			var value []byte

			if !doneA && !doneB {
				if bytes.Compare(nextA, nextB) == -1 {
					_, _ = iterA.Next()
					value = nextA
				} else {
					_, _ = iterB.Next()
					value = nextB
				}
			} else if !doneA {
				_, _ = iterA.Next()
				value = nextA
			} else if !doneB {
				_, _ = iterB.Next()
				value = nextB
			} else {
				break
			}

			if _, err := sortedFile.Write(value); err != nil {
				return fmt.Errorf("writing to sorted file: %w", err)
			}
			if _, err := sortedFile.Write([]byte("\n")); err != nil {
				return fmt.Errorf("writing to \n sorted file: %w", err)
			}
		}

		if err := sortedFile.Sync(); err != nil {
			return fmt.Errorf("syncing file: %w", err)
		}

		if err := os.Rename(sortedFile.Name(), a.Name()); err != nil {
			return fmt.Errorf("renaming sorted file: %w", err)
		}

		if err := os.Remove(b.Name()); err != nil {
			return fmt.Errorf("removing file: path=%s %w", b.Name(), err)
		}

	}

	// TODO: handle odd number of files

	return nil
}

func (worker *Worker) splitFiles(folder string, files []contracts.File) ([]string, error) {
	filePaths := make([]string, 0, len(files))

	for _, file := range files {
		// How many files the file will be broken into.
		splits := uint32(math.Ceil(float64(file.SizeBytes) / float64(worker.config.ExternalSortMaxMemoryBytes)))

		filePartitionsFolder := filepath.Join(folder, fmt.Sprint(file.FileID))
		if err := os.MkdirAll(filePartitionsFolder, 0755); err != nil {
			return nil, fmt.Errorf("creating folder for partitioned files: %w", err)
		}
		newFilePaths, err := worker.partitioner.Partition(file.Path, filePartitionsFolder, splits)
		if err != nil {
			return nil, fmt.Errorf("splitting file: %w", err)
		}
		filePaths = append(filePaths, newFilePaths...)
	}

	return filePaths, nil
}

func (worker *Worker) OnMapTaskReceived(ctx context.Context, task contracts.MapTask) error {
	worker.mu.Lock()
	defer worker.mu.Unlock()
	if task.ID == 0 {
		return fmt.Errorf("map task id is required")
	}
	if task.Script == "" {
		return fmt.Errorf("map task script is required")
	}
	if task.FileID == 0 {
		return fmt.Errorf("map task file id is required")
	}
	if task.FilePath == "" {
		return fmt.Errorf("map task file path is required")
	}

	jsScript, err := javascript.Parse(task.Script)
	if err != nil {
		return fmt.Errorf("parsing script: script='%s' %w", task.Script, err)
	}
	defer jsScript.Close()

	reader, err := worker.fileStorage.NewReader(ctx, task.FilePath)
	if err != nil {
		return fmt.Errorf("opening file: path=%s %w", task.FilePath, err)
	}
	defer reader.Close()

	contents, err := io.ReadAll(reader)
	if err != nil {
		return fmt.Errorf("reading file contents: %w", err)
	}

	outputFolder := fmt.Sprintf("%s/%d/%d", worker.config.WorkspaceFolder, task.ID, task.FileID)

	writer, err := worker.fileStorage.NewWriter(ctx, outputFolder)
	if err != nil {
		return fmt.Errorf("creating file storage writer: %w", err)
	}
	defer writer.Close()

	if err := jsScript.Map(task.FilePath, string(contents), func(key, value string) error {

		region, err := jsScript.Partition(key, task.NumberOfReduceTasks)
		if err != nil {
			return fmt.Errorf("calling user provided partition function: %w", err)
		}

		if err := writer.WriteKeyValue(key, fmt.Sprintf("%s\n", value), region); err != nil {
			return fmt.Errorf("writing key value: %w", err)
		}

		return nil
	}); err != nil {
		return fmt.Errorf("executing Map task script: %w", err)
	}

	if err := writer.Close(); err != nil {
		return fmt.Errorf("closing storage writer: %w", err)
	}

	outputFiles := writer.OutputFiles()

	completedTask := contracts.CompletedTask{TaskID: task.ID}
	for _, outputFile := range outputFiles {
		completedTask.OutputFiles = append(completedTask.OutputFiles, contracts.OutputFile{
			FileID:    task.FileID,
			FilePath:  outputFile.Path,
			SizeBytes: outputFile.SizeBytes,
		},
		)
	}

	timeoutCtx, cancel := context.WithTimeout(ctx, worker.config.MapTasksCompletedTimeout)
	defer cancel()
	if err := worker.masterClient.MapTasksCompleted(timeoutCtx, worker.config.Addr, []contracts.CompletedTask{completedTask}); err != nil {
		return fmt.Errorf("sending MapTasksCompleted message to master: %w", err)
	}

	return nil
}

func (worker *Worker) sendHeartbeat(ctx context.Context) error {
	timeoutCtx, cancel := context.WithTimeout(ctx, worker.config.HeartbeatTimeout)
	defer cancel()
	if err := worker.masterClient.Heartbeat(timeoutCtx, worker.config.Addr, worker.config.MemoryAvailable); err != nil {
		return fmt.Errorf("sending heartbeat to master: %w", err)
	}

	return nil
}
