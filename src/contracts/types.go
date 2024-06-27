package contracts

type TaskID uint64
type FileID uint64
type WorkerState int32

type MapTask struct {
	ID       TaskID
	Script   string
	FileID   FileID
	FilePath string
}
