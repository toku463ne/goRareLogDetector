package csvdb

import (
	"fmt"
	"io"
	"sort"

	"github.com/pkg/errors"
)

type Rows struct {
	reader             *Reader
	selectedColIndexes []int
	tableCols          []string
	conditionCheckFunc func([]string) bool
	orderbyBuff        orderBuffRows
	orderbyBuffPos     int
	orderbyExecuted    bool
	orderbyErr         error
}

func newRows(conditionCheckFunc func([]string) bool,
	path string, tableCols, selectedCols []string, reader *Reader) (*Rows, error) {
	//reader, err := newCsvReader(path, 0)
	if err := reader.open(); err != nil {
		return nil, err
	}
	r := new(Rows)
	r.reader = reader
	r.conditionCheckFunc = conditionCheckFunc
	r.tableCols = tableCols
	r.orderbyExecuted = false

	colIndexes := make([]int, len(selectedCols))
	for i, cols := range selectedCols {
		ok := false
		for j, colt := range tableCols {
			if colt == cols {
				colIndexes[i] = j
				ok = true
				break
			}
		}
		if !ok {
			return nil, errors.New(fmt.Sprintf("col %s is not in the table", cols))
		}
	}
	r.selectedColIndexes = colIndexes
	return r, nil
}

func (r *Rows) Next() bool {
	if r.orderbyExecuted {
		if r.orderbyBuffPos+1 >= r.orderbyBuff.Len() {
			return false
		}
		for r.orderbyBuffPos < r.orderbyBuff.Len() {
			r.orderbyBuffPos++

			return true
			/*
				if r.conditionCheckFunc == nil || r.conditionCheckFunc(r.orderbyBuff[r.orderbyBuffPos].v) {
					return true
				}
			*/
		}
		r.orderbyErr = io.EOF
	} else {
		for r.reader.next() {
			if r.conditionCheckFunc == nil || r.conditionCheckFunc(r.reader.values) {
				return true
			}
		}
	}
	return false
}

func (r *Rows) Err() error {
	if r.orderbyExecuted {
		return r.orderbyErr
	}
	return r.reader.err
}

func (r *Rows) Scan(args ...interface{}) error {
	var v []string
	if r.orderbyExecuted {
		v = r.orderbyBuff[r.orderbyBuffPos].v
	} else {
		v = r.reader.values //r.tableCols[i]
	}
	if r.selectedColIndexes == nil || len(r.selectedColIndexes) == 0 {
		if len(args) != len(r.tableCols) {
			return errors.New(fmt.Sprintf("Got %d args while expected %d",
				len(args), len(r.tableCols)))
		}
		for i, _ := range r.tableCols {
			src := v[i]
			dst := args[i]
			if err := convFromString(src, dst); err != nil {
				return err
			}
		}
	} else {
		if len(args) != len(r.selectedColIndexes) {
			return errors.New(fmt.Sprintf("Got %d args while expected %d",
				len(args), len(r.selectedColIndexes)))
		}
		for argidx, colidx := range r.selectedColIndexes {
			src := v[colidx]
			dst := args[argidx]
			if err := convFromString(src, dst); err != nil {
				return err
			}
		}
	}
	return nil
}

/*
fieldTypes:
int, int8, int32, int64,
uint, uint8, uint16, uint32, uint64
float32, float64,
bool

direction:
asc, desc
*/
func (r *Rows) OrderBy(fields []string, fieldTypes []string, direction int) error {
	if len(fields) != len(fieldTypes) {
		return errors.Errorf("length of fields=%d does not match that of fieldTypes=%d",
			len(fields), len(fieldTypes))
	}
	fieldIdxs := make([]int, len(fields))
	for i, f := range fields {
		ok := false
		for j, colt := range r.tableCols {
			if colt == f {
				fieldIdxs[i] = j
				ok = true
				break
			}
		}
		if !ok {
			return errors.New(fmt.Sprintf("col %s is not in the table", f))
		}
	}
	ov := make(orderBuffRows, 0)
	for r.reader.next() {
		if r.conditionCheckFunc == nil || r.conditionCheckFunc(r.reader.values) {
			or := new(orderBuffRow)
			or.v = r.reader.values
			or.orderFieldTypes = fieldTypes
			or.direction = direction
			or.orderFieldIdxs = fieldIdxs
			ov = append(ov, *or)
		}
	}
	sort.Sort(ov)
	r.orderbyExecuted = true
	r.orderbyBuff = ov
	r.orderbyBuffPos = -1
	return nil
}
