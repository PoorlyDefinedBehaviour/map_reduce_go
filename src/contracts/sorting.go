package contracts

import "io"

type Sorter interface {
	Sort(in io.Reader) (io.Reader, error)
}
