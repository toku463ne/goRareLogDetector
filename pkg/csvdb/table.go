package csvdb

import (
	"fmt"
	"goRareLogDetector/pkg/utils"
	"io"
	"os"
	"strconv"

	"github.com/pkg/errors"
)

type Table struct {
	*TableDef
	columns        []string
	colMap         map[string]int
	useGzip        bool
	bufferSize     int
	readBufferSize int
	path           string
	iBuff          *insertBuff
	reader         *Reader
}

func newTable(groupName, tableName, path string,
	columns []string, useGzip bool,
	bufferSize, readBufferSize int) (*Table, error) {
	t := new(Table)
	t.TableDef = new(TableDef)
	t.groupName = groupName
	t.tableName = tableName
	t.path = path
	t.columns = columns
	colMap := make(map[string]int)
	for i, col := range columns {
		colMap[col] = i
	}
	t.colMap = colMap
	t.useGzip = useGzip
	t.bufferSize = bufferSize
	t.iBuff = newInsertBuffer(bufferSize)
	t.readBufferSize = readBufferSize
	var err error
	t.reader, err = newReader(path, readBufferSize)
	if err != nil {
		return nil, err
	}

	return t, nil
}

func (t *Table) Close() {
	t.iBuff = nil
}

func (t *Table) Drop() error {
	if utils.PathExist(t.path) {
		return os.Remove(t.path)
	}
	return nil
}

func (t *Table) Count(conditionCheckFunc func([]string) bool) int {
	//if !pathExist(t.path) {
	//	return 0
	//}

	//reader, err := newCsvReader(t.path)
	//if err != nil {
	//	return -1
	//}
	cnt := 0
	reader := t.reader
	if err := reader.open(); err != nil {
		return -1
	}
	defer reader.close()
	for reader.next() {
		v := reader.values
		if conditionCheckFunc == nil {
			cnt++
		} else if conditionCheckFunc(v) {
			cnt++
		}
	}
	if reader.err != nil && reader.err != io.EOF {
		return -1
	}
	return cnt
}

func (t *Table) Sum(conditionCheckFunc func([]string) bool,
	column string, s interface{}) error {
	//if !pathExist(t.path) {
	//	convFromString("0", s)
	//	return nil
	//}

	idx, ok := t.colMap[column]
	if !ok {
		return errors.New(fmt.Sprintf("Column %s does not exist", column))
	}

	//reader, err := newCsvReader(t.path)
	//if err != nil {
	//	return err
	//}
	reader := t.reader
	if err := reader.open(); err != nil {
		return err
	}
	res := 0.0
	defer reader.close()
	for reader.next() {
		vs := reader.values
		v, err := strconv.ParseFloat(vs[idx], 64)
		if err != nil {
			return err
		}

		if conditionCheckFunc == nil {
			res += v
		} else if conditionCheckFunc(vs) {
			res += v
		}
	}
	if reader.err != nil && reader.err != io.EOF {
		return reader.err
	}
	if err := convFromString(asString(res), s); err != nil {
		return err
	}
	return nil
}

func (t *Table) SelectRows(conditionCheckFunc func([]string) bool,
	colNames []string) (*Rows, error) {
	//if !pathExist(t.path) {
	//	return nil, errors.New(cErrPathNotExists)
	//}
	return newRows(conditionCheckFunc,
		t.path, t.columns, colNames, t.reader)
}

func (t *Table) Select1Row(conditionCheckFunc func([]string) bool,
	colNames []string, args ...interface{}) error {
	//if !pathExist(t.path) {
	//	return errors.New(cErrPathNotExists)
	//}
	r, err := t.SelectRows(conditionCheckFunc, colNames)
	if err != nil {
		return err
	}
	for r.Next() {
		return r.Scan(args...)
	}
	return errors.New("No record found")
}

func (t *Table) readRows(conditionCheckFunc func([]string) bool) ([][]string, error) {
	//if !pathExist(t.path) {
	//	return nil, errors.New(cErrPathNotExists)
	//}

	//reader, err := newCsvReader(t.path)
	//if err != nil {
	//	return nil, err
	//}
	reader := t.reader
	if err := reader.open(); err != nil {
		return nil, err
	}
	found := [][]string{}
	defer reader.close()
	for reader.next() {
		v := reader.values
		if conditionCheckFunc == nil {
			found = append(found, v)
		} else if conditionCheckFunc(v) {
			found = append(found, v)
		}
	}
	if reader.err != nil {
		return nil, reader.err
	}
	return found, nil
}

func (t *Table) InsertRow(columns []string, args ...interface{}) error {
	if columns == nil && len(args) != len(t.columns) {
		return errors.New("len of args do not match to table columns")
	}
	if columns != nil && len(columns) != len(args) {
		return errors.New("len of columns and args do not match")
	}

	row := make([]string, len(t.columns))
	if columns == nil {
		for i, v := range args {
			row[i] = asString(v)
		}
	} else {
		for i, col := range columns {
			j, ok := t.colMap[col]
			if !ok {
				return errors.New(fmt.Sprintf("column %s does not exist", col))
			}
			row[j] = asString(args[i])
		}
	}

	if t.iBuff.register(row) {
		t.Flush()
	}

	if t.readBufferSize > 0 {
		if err := t.reader.append(row); err != nil {
			return err
		}
	}

	return nil
}

func (t *Table) Flush() error {
	return t.flush(CWriteModeAppend)
}

func (t *Table) FlushOverwrite() error {
	return t.flush(CWriteModeWrite)
}

func (t *Table) flush(wmode string) error {
	if t.iBuff.pos < 0 {
		return nil
	}
	writer, err := t.openW(wmode)
	if err != nil {
		return err
	}
	defer writer.close()
	for i, row := range t.iBuff.rows {
		if err := writer.write(row); err != nil {
			t.iBuff.init()
			return err
		}
		if i >= t.iBuff.pos {
			break
		}
	}
	t.iBuff.init()
	writer.flush()
	return nil
}

func (t *Table) openW(writeMode string) (*Writer, error) {
	writer, err := newWriter(t.path, writeMode)
	if err != nil {
		return nil, err
	}
	return writer, nil
}

func (t *Table) Max(conditionCheckFunc func([]string) bool,
	field string, v interface{}) error {
	return t.minmax(conditionCheckFunc, true, field, v)
}

func (t *Table) Min(conditionCheckFunc func([]string) bool,
	field string, v interface{}) error {
	return t.minmax(conditionCheckFunc, false, field, v)
}

func (t *Table) minmax(conditionCheckFunc func([]string) bool,
	isMax bool, field string, v interface{}) error {
	r, err := t.SelectRows(conditionCheckFunc, []string{field})

	if err != nil {
		return err
	}
	var a float64
	m := 1.0
	if !isMax {
		m = -1.0
	}
	res := 0.0
	i := 0
	for r.Next() {
		if err := r.Scan(&a); err != nil {
			return err
		}
		if conditionCheckFunc != nil && !conditionCheckFunc(r.reader.values) {
			continue
		}
		if i == 0 || m*res < m*a {
			res = a
		}
		i++
	}

	convFromString(asString(res), v)
	return nil
}

func (t *Table) GetColIdx(colName string) int {
	i, ok := t.colMap[colName]
	if ok {
		return i
	}
	return -1
}

func (t *Table) Delete(conditionCheckFunc func([]string) bool) error {
	return t.update(conditionCheckFunc, nil, false)
}

func (t *Table) Upsert(conditionCheckFunc func([]string) bool,
	updates map[string]interface{}) error {
	return t.update(conditionCheckFunc, updates, true)
}

func (t *Table) Update(conditionCheckFunc func([]string) bool,
	updates map[string]interface{}) error {
	return t.update(conditionCheckFunc, updates, false)
}

func (t *Table) Truncate() error {
	if t.reader.readBuff != nil {
		t.reader.readBuff.init()
	} else {
		writer, err := t.openW(CWriteModeWrite)
		if err != nil {
			return err
		}
		defer writer.close()
		writer.flush()
	}
	return nil
}

func (t *Table) update(conditionCheckFunc func([]string) bool,
	updates map[string]interface{}, isUpsert bool) error {
	if conditionCheckFunc == nil && updates == nil {
		return t.Truncate()
	}

	if !t.reader.hasRows() && !isUpsert {
		return nil
	}

	//var reader *CsvReader
	reader := t.reader
	if err := reader.open(); err != nil {
		return err
	}
	defer reader.close()

	rows := make([][]string, 0)
	isUpdated := false
	cnt := 0
	for reader != nil && reader.next() {
		cnt++
		v := reader.values

		if updates == nil {
			if conditionCheckFunc != nil && !conditionCheckFunc(v) {
				rows = append(rows, v)
			}
		} else {
			if conditionCheckFunc == nil || conditionCheckFunc(v) {
				for col, updv := range updates {
					v[t.colMap[col]] = asString(updv)
				}
				isUpdated = true
			}
			rows = append(rows, v)
		}
	}
	if reader != nil {
		reader.close()
		reader = nil
	}

	if len(rows) < cnt {
		isUpdated = true
	}

	if isUpdated {
		buff := newInsertBuffer(len(rows))
		buff.setBuff(rows)
		t.iBuff = buff

		if err := t.flush(CWriteModeWrite); err != nil {
			return err
		}
		if t.reader.readBuff != nil {
			//t.reader.readBuff.rows = rows
			t.reader.readBuff.init()
			for _, row := range rows {
				t.reader.readBuff.append(row)
			}
		}
	} else if isUpsert {
		columns := make([]string, len(updates))
		args := make([]interface{}, len(updates))
		i := 0
		for col, val := range updates {
			columns[i] = col
			args[i] = val
			i++
		}
		if err := t.InsertRow(columns, args...); err != nil {
			return err
		}
		if t.iBuff.pos != -1 {
			t.flush(CWriteModeAppend)
		}
	}
	return nil
}
