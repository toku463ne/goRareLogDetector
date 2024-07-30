package csvdb

import (
	"compress/gzip"
	"encoding/csv"
	"goRareLogDetector/pkg/utils"
	"io"
	"os"
	"path/filepath"

	"github.com/pkg/errors"
)

type Reader struct {
	fr       *os.File
	zr       *gzip.Reader
	reader   *csv.Reader
	values   []string
	err      error
	filename string
	mode     string
	readBuff *readBuff
}

func newReader(filename string, buffSize int) (*Reader, error) {
	c := new(Reader)
	c.filename = filename
	if err := c.open(); err != nil {
		return nil, err
	}

	if buffSize > 0 {
		buff := newReadBuffer(filename, buffSize)
		if c.reader == nil {
			c.readBuff = buff
			return c, nil
		}
		for {
			values, err := c.reader.Read()
			if err == io.EOF {
				break
			}
			if err != nil {
				return nil, err
			}
			if err := buff.append(values); err != nil {
				return nil, err
			}
		}
		c.readBuff = buff
		c.close()
	}

	return c, nil
}

func (c *Reader) open() error {
	if c.readBuff != nil {
		c.initPos()
		return nil
	}
	ext := filepath.Ext(c.filename)
	var fr *os.File
	var zr *gzip.Reader
	var r *csv.Reader
	var err error
	mode := ""

	if !utils.PathExist(c.filename) {
		return nil
	}

	fr, err = os.Open(c.filename)
	if err != nil {
		return errors.WithStack(err)
	}

	if ext == ".gz" || ext == ".gzip" {
		zr, err = gzip.NewReader(fr)
		if err != nil {
			return errors.WithStack(err)
		}
		r = csv.NewReader(zr)
		mode = cRModeGZip
	} else {
		r = csv.NewReader(fr)
		mode = cRModePlain
	}

	c.fr = fr
	c.zr = zr
	c.reader = r
	c.mode = mode
	return nil
}

func (c *Reader) initPos() error {
	if c.readBuff == nil {
		return errors.New("initPos() can use only when readBuff is enabled")
	}
	c.readBuff.initReadPos()
	return nil
}

func (c *Reader) next() bool {
	var values []string
	var err error
	if c.readBuff == nil {
		if c.reader == nil {
			err = io.EOF
		} else {
			values, err = c.reader.Read()
		}
	} else {
		if c.readBuff.next() {
			values = c.readBuff.values
		} else {
			err = io.EOF
		}
	}
	c.err = err
	if err == io.EOF {
		return false
	}
	if err != nil {
		return false
	}
	c.values = values
	c.err = nil
	return true
}

func (c *Reader) close() {
	if c.zr != nil {
		c.zr.Close()
		c.fr = nil
	}
	if c.fr != nil {
		c.fr.Close()
		c.fr = nil
	}
}

func (c *Reader) append(row []string) error {
	if err := c.readBuff.append(row); err != nil {
		return err
	}
	return nil
}

func (c *Reader) hasRows() bool {
	if c.readBuff != nil && c.readBuff.pos >= 0 {
		return true
	}
	if c.readBuff == nil && utils.PathExist(c.filename) {
		return true
	}
	return false
}
