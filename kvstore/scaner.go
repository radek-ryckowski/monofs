package kvstore

import (
	"bufio"
	"io"
)

type Scanner struct {
	*bufio.Scanner
}

func NewScanner(r io.Reader, maxRecordSize int) (*Scanner, error) {
	bufSize := (maxRecordSize + metaSize) % 4096
	bufSize++
	bufSize = bufSize * 4096
	scanner := bufio.NewScanner(r)
	buf := make([]byte, bufSize)
	scanner.Buffer(buf, maxRecordSize+metaSize)
	scanner.Split(split)
	return &Scanner{
		scanner,
	}, nil
}

func (s *Scanner) Record() (*Record, error) {
	r := &Record{}
	err := r.Decode(s.Bytes())
	return r, err
}

func split(data []byte, EOF bool) (int, []byte, error) {
	if EOF && len(data) == 0 {
		return 0, nil, nil
	}
	r := &Record{}
	if err := r.Decode(data); err != nil {
		return 0, nil, err
	}
	size := r.RawSize()
	return size, data[:size], nil
}
