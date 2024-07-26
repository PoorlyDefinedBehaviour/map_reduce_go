package contracts

import "io"

type Sorter interface {
	Sort(in io.Reader) (io.Reader, error)
	SortAndMerge(a, b io.Reader, out io.Writer) error
}
