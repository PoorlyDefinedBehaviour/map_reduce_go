package iterator

import (
	"bufio"
	"io"
)

type LineIterator struct {
	scanner *bufio.Scanner
	// Buffer used to hold the value returned by Peek().
	// The value is reset and returned on the next Next() call.
	peekBuffer []byte
}

func NewLineIterator(reader io.Reader) *LineIterator {
	return &LineIterator{scanner: bufio.NewScanner(reader)}
}

func (it *LineIterator) Peek() ([]byte, bool) {
	if !it.scanner.Scan() {
		return nil, true
	}

	buffer := it.scanner.Bytes()

	it.peekBuffer = buffer

	return buffer, false
}

func (it *LineIterator) Next() ([]byte, bool) {
	if it.peekBuffer != nil {
		buffer := it.peekBuffer
		it.peekBuffer = nil
		return buffer, false
	}

	if !it.scanner.Scan() {
		return nil, true
	}

	return it.scanner.Bytes(), false
}
