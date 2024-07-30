package csvdb

import (
	"compress/gzip"
	"encoding/csv"
	"os"
	"path/filepath"

	"github.com/pkg/errors"
)

type Writer struct {
	fw     *os.File
	zw     *gzip.Writer
	writer *csv.Writer
	path   string
	mode   string
}

func newWriter(path, writeMode string) (*Writer, error) {
	ext := filepath.Ext(path)
	var fw *os.File
	var zw *gzip.Writer
	var writer *csv.Writer
	mode := ""

	flags := 0
	switch writeMode {
	case CWriteModeWrite:
		flags = os.O_WRONLY | os.O_CREATE | os.O_TRUNC
	default:
		flags = os.O_WRONLY | os.O_CREATE | os.O_APPEND
	}

	fw, err := os.OpenFile(path, flags, 0644)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	if ext == ".gz" || ext == ".gzip" {
		zw = gzip.NewWriter(fw)
		writer = csv.NewWriter(zw)
		mode = cRModeGZip
	} else {
		writer = csv.NewWriter(fw)
		mode = cRModePlain
	}

	c := new(Writer)
	c.path = path
	c.writer = writer
	c.fw = fw
	c.zw = zw
	c.mode = mode

	return c, nil
}

func (c *Writer) write(record []string) error {
	return c.writer.Write(record)
}

func (c *Writer) flush() {
	c.writer.Flush()
}

func (c *Writer) close() {
	if c.zw != nil {
		c.zw.Close()
	}

	if c.fw != nil {
		c.fw.Close()
	}
}
