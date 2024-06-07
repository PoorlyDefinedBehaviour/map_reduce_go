package partitioning

// Partitioner partitions an input file into several files of the specified size.
type Partitioner interface {
	// Partitions the file contents into at most `maxNumberOfPartitions`` where each file has at most `maxPartitionSizeInBytes` bytes.
	Partition(filepath string, outputFolder string, maxPartitionSizeInBytes uint64, maxNumberOfPartitions uint32) error
}
