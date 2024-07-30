package filepointer

import (
	"goRareLogDetector/pkg/utils"
	"io"

	"github.com/pkg/errors"
)

type FilePointer struct {
	files    []string
	epochs   []int64
	r        *reader
	lastRow  int
	pos      int
	e        error
	currErr  error
	currText string
	currRow  int
	currPos  int
	IsEOF    bool
}

func NewFilePointer(pathRegex string,
	lastEpoch int64, lastRow int) (*FilePointer, error) {
	fp := new(FilePointer)
	var targetFiles []string
	var targetEpochs []int64
	if pathRegex == "" {
		targetFiles = []string{""}
		targetEpochs = []int64{0}
	} else {
		epochs, files, err := utils.GetSortedGlob(pathRegex)
		if err != nil {
			fp.currErr = err
			return nil, err
		}
		for i, f := range files {
			epoch := epochs[i]
			if (epoch == lastEpoch && lastRow != -1) || epoch > lastEpoch {
				targetFiles = append(targetFiles, f)
				targetEpochs = append(targetEpochs, epoch)
			}
		}
	}

	fp.files = targetFiles
	fp.epochs = targetEpochs
	fp.lastRow = lastRow
	fp.pos = 0
	fp.IsEOF = false
	fp.currPos = 0
	return fp, nil
}

func (fp *FilePointer) CurrFileEpoch() int64 {
	return fp.epochs[fp.pos]
}

func (fp *FilePointer) Err() error {
	return fp.currErr
}

func (fp *FilePointer) Open() error {
	if fp.r != nil {
		fp.Close()
	}
	if fp.files == nil {
		return errors.New("no files to open")
	}
	fp.pos = 0
	currRow := fp.lastRow
	r, err := newReader(fp.files[0])
	if err != nil {
		return errors.WithStack(err)
	}

	if !r.next() {
		if err := r.err(); err != nil {
			return err
		}
	}

	row := 0
	if currRow > 0 {
		for r.next() {
			row++
			if row >= currRow {
				break
			}
		}
		if err := r.err(); err != nil {
			return err
		}
	}
	fp.r = r
	return nil
}

// this function is critical for performance
// need to keep as simple as possible
func (fp *FilePointer) Next() bool {
	// don't consider the case fp.r is nil
	// case it is nil, it means open() has not been done which is considered as a bug

	err := fp.e
	fp.currErr = err
	if err == io.EOF {
		return false
	}
	if err != nil {
		fp.currText = ""
		fp.currRow = -1
		return false
	}
	fp.currText = fp.r.text()
	fp.currRow = fp.r.rowNum
	fp.currPos = fp.pos

	ok := fp.r.next()
	if ok {
		fp.IsEOF = false
		return true
	}

	err = fp.r.err()
	if err != nil && err != io.EOF {
		fp.e = err
	}
	fp.IsEOF = true

	if fp.pos+1 >= len(fp.files) {
		fp.e = io.EOF
		return true
	}
	if fp.r != nil {
		fp.r.close()
		fp.r = nil
	}

	fp.pos++
	r, err := newReader(fp.files[fp.pos])
	if err != nil {
		fp.e = errors.WithStack(err)
		return true
	}
	fp.r = r
	fp.e = nil

	fp.r.next()
	return true
}

func (fp *FilePointer) IsLastFile() bool {
	return fp.currPos+1 >= len(fp.files)
}

func (fp *FilePointer) Text() string {
	//return fp.r.text()
	return fp.currText
}

func (fp *FilePointer) Row() int {
	//return fp.r.rowNum
	return fp.currRow
}

func (fp *FilePointer) Close() {
	if fp.r != nil {
		fp.r.close()
		fp.r = nil
	}
	fp.pos = 0
}

func (fp *FilePointer) IsOpen() bool {
	if fp.r == nil || !fp.r.isOpen() {
		return false
	}
	return true
}

func (fp *FilePointer) CountNFiles(totalFileCount int, pathRegex string) (int, int, error) {
	cnt := 0
	fileCnt := 0
	for i, filename := range fp.files {
		if i >= totalFileCount {
			break
		}
		fileCnt++
		r, err := newReader(filename)
		if err != nil {
			return -1, fileCnt, err
		}
		for {
			_, _, err := r.reader.ReadLine()
			if err != nil {
				if err != io.EOF {
					return -1, fileCnt, err
				}
				break
			}
			cnt++
		}
	}
	fp.Close()
	return cnt, fileCnt, nil
}
