package worker

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/poorlydefinedbehaviour/map_reduce_go/src/assert"
	"github.com/poorlydefinedbehaviour/map_reduce_go/src/contracts"
	"github.com/poorlydefinedbehaviour/map_reduce_go/src/interpreters/javascript"
	"github.com/poorlydefinedbehaviour/map_reduce_go/src/iterator"
	"github.com/poorlydefinedbehaviour/map_reduce_go/src/queue"
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

func (worker *Worker) OnReduceTaskReceived(ctx context.Context, task contracts.ReduceTask) (returnErr error) {
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

	sortedFile, err := worker.sortReduceInputFiles(filePaths)
	if err != nil {
		return fmt.Errorf("soring reduce input files: %w", err)
	}

	it := iterator.NewLineIterator(sortedFile)

	jsScript, err := javascript.Parse(task.Script)
	if err != nil {
		return fmt.Errorf("parsing reduce script: %w", err)
	}
	defer jsScript.Close()

	outputFolder := filepath.Join(worker.config.WorkspaceFolder, fmt.Sprint(task.ID), "outputs")
	if err := os.MkdirAll(outputFolder, 0755); err != nil {
		return fmt.Errorf("creating output folder: %w", err)
	}
	// TODO: should write output to replicated storage instead of local disk
	outputFilePath := filepath.Join(outputFolder, "output")
	f, err := os.OpenFile(outputFilePath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return fmt.Errorf("creating output file: %w", err)
	}
	defer func() {
		if err := f.Close(); err != nil {
			returnErr = errors.Join(returnErr, fmt.Errorf("closing ouput file: %w", err))
		}
	}()
	if err := iterateReduceInput(it, jsScript, f); err != nil {
		return fmt.Errorf("iterating reduce input file: %w", err)
	}
	if err := f.Sync(); err != nil {
		return fmt.Errorf("syncing output file: %w", err)
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

func (worker *Worker) sortReduceInputFiles(filePaths []string) (*os.File, error) {
	wg := errgroup.Group{}

	files := make([]*os.File, len(filePaths))

	// Sort every file by themselves.
	for i, filePath := range filePaths {
		i := i
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

			if _, err := io.Copy(sortedFile, reader); err != nil {
				return fmt.Errorf("writing sorted data to file: %w", err)
			}

			if err := os.Remove(file.Name()); err != nil {
				return fmt.Errorf("renaming sorted file: %w", err)
			}

			files[i] = sortedFile

			return nil
		})
	}

	if err := wg.Wait(); err != nil {
		return nil, fmt.Errorf("sorting reduce input files individually: %w", err)
	}

	// The queue of files to sort.
	sortQueue := queue.New[*os.File]()
	for _, file := range files {
		sortQueue.Add(file)
	}

	for sortQueue.Len() > 1 {
		// Take the first from files from the queue and sort them.
		a, _ := sortQueue.Pop()
		b, _ := sortQueue.Pop()

		if a != nil {
			if _, err := a.Seek(0, 0); err != nil {
				return nil, fmt.Errorf("seeking to beginning of file: %w", err)
			}
		}
		if b != nil {
			if _, err := b.Seek(0, 0); err != nil {
				return nil, fmt.Errorf("seeking to beginning of file: %w", err)
			}
		}

		// The sorted file is the file contaning the contents of the two files being sorted in order.
		sortedFile, err := os.OpenFile(filepath.Join(filepath.Dir(a.Name()), fmt.Sprintf("sorted_temp_%s", filepath.Base(a.Name()))), os.O_RDWR|os.O_CREATE, 0644)
		if err != nil {
			return nil, fmt.Errorf("creating sorted file: %w", err)
		}

		defer func() {
			if a != nil {
				if closeErr := a.Close(); closeErr != nil {
					err = errors.Join(err, fmt.Errorf("closing file: %w", closeErr))
				}
			}
			if b != nil {
				if closeErr := b.Close(); closeErr != nil {
					err = errors.Join(err, fmt.Errorf("closing file: %w", closeErr))
				}
			}
		}()

		if err := worker.sorter.SortAndMerge(a, b, sortedFile); err != nil {
			return nil, fmt.Errorf("sorting files: %w", err)
		}

		sortQueue.Add(sortedFile)
	}

	assert.Equal(1, sortQueue.Len())
	sortedFile, _ := sortQueue.Pop()
	if _, err := sortedFile.Seek(0, 0); err != nil {
		return nil, fmt.Errorf("seeking to beginning of sorted file: %w", err)
	}

	return sortedFile, nil
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
	if task.NumberOfReduceTasks == 0 {
		return fmt.Errorf("map task number of reduce tasks is required")
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

		if err := writer.WriteKeyValue(key, value, region); err != nil {
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

func iterateReduceInput(it *iterator.LineIterator, jsScript *javascript.Script, out io.Writer) error {
	for {
		buffer, done := it.Peek()
		if done {
			return nil
		}

		key, _, found := strings.Cut(string(buffer), ",")
		assert.True(found, "file should contain key value pairs separated by ,")

		var previousKey string

		if err := jsScript.Reduce(
			string(key),
			func() (string, bool) {
				buffer, done := it.Peek()
				if done {
					return "", true
				}
				key, value, found := strings.Cut(string(buffer), ",")
				assert.True(found, "file should contain key value pairs separated by ,")
				if previousKey != "" && key != string(previousKey) {
					return "", true
				}
				_, _ = it.Next()
				previousKey = key
				return string(value), false
			},
			func(key, value string) error {
				if _, err := out.Write([]byte(fmt.Sprintf("%s %s\n", key, value))); err != nil {
					return fmt.Errorf("writing output: %w", err)
				}
				return nil
			}); err != nil {
			return fmt.Errorf("calling user provided reduce function: %w", err)
		}
	}
}

func (worker *Worker) sendHeartbeat(ctx context.Context) error {
	timeoutCtx, cancel := context.WithTimeout(ctx, worker.config.HeartbeatTimeout)
	defer cancel()
	if err := worker.masterClient.Heartbeat(timeoutCtx, worker.config.Addr, worker.config.MemoryAvailable); err != nil {
		return fmt.Errorf("sending heartbeat to master: %w", err)
	}

	return nil
}
