package filepointer

import (
	"bufio"
	"compress/gzip"
	"io"
	"os"
	"path/filepath"

	"github.com/pkg/errors"
)

type reader struct {
	fd       *os.File
	zr       *gzip.Reader
	reader   *bufio.Reader
	rowNum   int
	mode     string
	filename string
	e        error
	currText string
}

func newReader(filename string) (*reader, error) {
	var fd *os.File
	var err error
	if filename == "" {
		fd = os.Stdin
	} else {
		fd, err = os.OpenFile(filename, os.O_RDONLY, 0644)
		if err != nil {
			return nil, errors.WithStack(err)
		}
	}

	lr := new(reader)
	lr.fd = fd

	ext := filepath.Ext(filename)
	if ext == ".gz" || ext == ".gzip" {
		lr.mode = "gzip"
	} else {
		lr.mode = "plain"
	}

	if lr.mode == "gzip" {
		zr, err := gzip.NewReader(fd)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		lr.reader = bufio.NewReader(zr)
		lr.zr = zr
	} else {
		lr.reader = bufio.NewReader(fd)
	}
	//lr.scanner.Split(bufio.ScanBytes)
	lr.filename = filename
	return lr, nil
}

func (lr *reader) next() bool {
	var b []byte
	lr.e = nil
	isEOF := false
	for {
		line, isPrefix, err := lr.reader.ReadLine()
		if err != nil {
			if err == io.EOF {
				isEOF = true
			} else {
				lr.e = err
			}
			break
		}
		b = append(b, line...)
		if !isPrefix {
			break
		}
	}
	lr.currText = string(b)

	if lr.e == nil && !isEOF {
		lr.rowNum++
		return true
	}
	return false
}

func (lr *reader) err() error {
	return lr.e
}

func (lr *reader) text() string {
	return lr.currText
}

func (lr *reader) close() {
	if lr.mode == "gzip" {
		if lr.zr != nil {
			lr.zr.Close()
		}
	}
	if lr.fd != nil {
		lr.fd.Close()
	}
}

func (lr *reader) isOpen() bool {
	if lr.mode == "gzip" {
		if lr.zr == nil {
			return false
		}
	}
	if lr.fd == nil {
		return false
	}
	return true
}
