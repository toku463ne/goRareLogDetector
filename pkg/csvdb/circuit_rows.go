package csvdb

import (
	"io"
)

type circuitRows struct {
	groupName  string
	tableNames []string
	rows       *Rows
	pos        int
	err        error
	*CsvDB
	columns            []string
	conditionCheckFunc func([]string) bool
	BlockCompleted     bool
	statusTable        *Table
}

func (r *circuitRows) Next() bool {
	if r.pos >= len(r.tableNames) {
		r.err = io.EOF
		r.rows = nil
		return false
	}

	if r.rows == nil {
		completed := true
		if err := r.statusTable.Select1Row(func(v []string) bool {
			return v[ColBlockId] == r.tableNames[r.pos]
		}, []string{"completed"}, &completed); err != nil {
			r.err = err
			return false
		}

		g, err := r.GetGroup(r.groupName)
		if err != nil {
			r.err = err
			return false
		}

		t, err := g.GetTable(r.tableNames[r.pos])
		if err != nil {
			r.err = err
			return false
		}
		rows, err := t.SelectRows(r.conditionCheckFunc, r.columns)
		if err != nil {
			r.err = err
			return false
		}
		r.rows = rows
		r.BlockCompleted = completed

	}

	r.rows.Next()
	err := r.rows.Err()
	r.err = err

	if err != nil && err.Error() == "EOF" {
		r.pos++
		r.rows = nil
		return r.Next()
	} else if err != nil {
		r.err = err
		return false
	}
	return true
}

func (r *circuitRows) Scan(a ...interface{}) error {
	return r.rows.Scan(a...)
}
