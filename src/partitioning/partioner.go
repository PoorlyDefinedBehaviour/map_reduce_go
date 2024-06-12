package partitioning

// Partitioner partitions an input file into several files of the specified size.
type Partitioner interface {
	// Partitions the file contents into at most `maxNumberOfPartitions`.
	Partition(filepath string, outputFolder string, maxNumberOfPartitions uint32) ([]string, error)
}
