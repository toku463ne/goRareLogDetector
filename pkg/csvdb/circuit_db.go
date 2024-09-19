package csvdb

import (
	"fmt"
	"goRareLogDetector/pkg/utils"
	"strconv"

	"github.com/pkg/errors"
)

type CircuitDB struct {
	*CsvDB
	DataDir     string
	RowNo       int
	Name        string
	maxBlocks   int
	blockSize   int
	blockNo     int
	retention   int64
	statusTable *Table
	lastIndex   int64
	lastEpoch   int64
	currTable   *Table
	writeMode   string
	unitsecs    int64
}

var (
	CircuitColumns = []string{"lastIndex", "blockNo", "blockID", "rowNo", "lastEpoch", "completed"}
	ColIndex       = 0
	ColBlockNo     = 1
	ColBlockId     = 2
	ColRowNo       = 3
	ColLastEpoch   = 4
	ColCompleted   = 5
)

func NewCircuitDB(rootDir, name string, columns []string,
	maxBlocks, blockSize int, retention int64, frequency string, useGzip bool) (*CircuitDB, error) {
	cdb := new(CircuitDB)
	cdb.Name = name
	cdb.blockSize = blockSize
	cdb.maxBlocks = maxBlocks
	cdb.retention = retention

	if rootDir == "" {
		return cdb, nil
	}
	cdb.DataDir = fmt.Sprintf("%s/%s", rootDir, name)

	db, err := NewCsvDB(cdb.DataDir)
	if err != nil {
		return nil, err
	}
	_, err = db.CreateGroup(name, columns, useGzip, blockSize, 0)
	if err != nil {
		return nil, err
	}

	cdb.writeMode = CWriteModeAppend

	st, err := db.CreateTableIfNotExists("CircuitDBStatus",
		CircuitColumns, false, maxBlocks, maxBlocks)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	cdb.statusTable = st

	cdb.CsvDB = db

	cdb.unitsecs = utils.GetUnitsecs(frequency)

	t, err := cdb.GetBlockTable(cdb.blockNo)
	if err != nil {
		return nil, err
	}
	cdb.currTable = t

	return cdb, nil
}

func (cdb *CircuitDB) GetBlockTable(blockNo int) (*Table, error) {
	blockID := cdb.getBlockTableName(blockNo)
	return cdb.Groups[cdb.Name].GetTable(blockID)
}

func (cdb *CircuitDB) SetMaxBlocks(maxBlocks int) {
	cdb.maxBlocks = maxBlocks
}
func (cdb *CircuitDB) SetBlockSize(blockSize int) {
	cdb.blockSize = blockSize
}

func (cdb *CircuitDB) LoadCircuitDBStatus() error {
	if cdb.DataDir == "" {
		return nil
	}

	var lastIndex, lastEpoch int64
	var blockNo, RowNo int
	var completed bool

	t := cdb.statusTable
	if err := t.Max(nil, "lastIndex", &lastIndex); err != nil {
		return errors.WithStack(err)
	}

	lastIndexStr := strconv.Itoa(int(lastIndex))
	if err := t.Select1Row(func(v []string) bool {
		return v[ColIndex] == lastIndexStr
	}, []string{"blockNo", "rowNo", "lastEpoch", "completed"},
		&blockNo, &RowNo, &lastEpoch, &completed); err != nil {
		return errors.WithStack(err)
	}

	cdb.blockNo = blockNo
	cdb.lastIndex = lastIndex
	cdb.lastEpoch = lastEpoch
	//cdb.RowNo = RowNo // no need to load RowNo because it will be incremented when inserting rows
	cdb.RowNo = 0
	cdb.writeMode = CWriteModeAppend

	if completed {
		if err := cdb.NextBlock(lastEpoch); err != nil {
			return err
		}
	}

	t, err := cdb.GetBlockTable(cdb.blockNo)
	if err != nil {
		return err
	}
	cdb.currTable = t

	return nil
}

func (cdb *CircuitDB) NextBlock(lastEpoch int64) error {
	cdb.lastEpoch = lastEpoch

	if err := cdb.UpdateBlockStatus(true); err != nil {
		return err
	}

	cdb.RowNo = 0
	cdb.blockNo++
	if cdb.blockNo >= cdb.maxBlocks {
		cdb.blockNo = 0
	}
	cdb.lastIndex++

	// if don't save to file
	if cdb.DataDir == "" {
		return nil
	}

	cdb.writeMode = "w"

	cdb.currTable.Close()

	t, err := cdb.GetBlockTable(cdb.blockNo)
	if err != nil {
		return err
	}
	cdb.currTable = t
	//rt.rows = make([][]interface{}, rt.blockSize)
	return nil
}

func (cdb *CircuitDB) getBlockID(blockNo int) string {
	return fmt.Sprintf("%0"+strconv.Itoa(cMaxBlockDitigs)+"d", blockNo)
}

func (cdb *CircuitDB) getBlockTableName(blockNo int) string {
	return fmt.Sprintf("BLK%s", cdb.getBlockID(blockNo))
}

func (cdb *CircuitDB) InsertRow(columns []string, row ...interface{}) error {
	if cdb.DataDir == "" {
		return nil
	}
	if cdb.writeMode == CWriteModeWrite {
		if err := cdb.currTable.Delete(nil); err != nil {
			return errors.WithStack(err)
		}
		cdb.writeMode = CWriteModeAppend
		cdb.RowNo = 0
	}

	if err := cdb.currTable.InsertRow(columns, row...); err != nil {
		return errors.WithStack(err)
	}
	cdb.RowNo++
	cdb.writeMode = CWriteModeAppend
	return nil
}

func (cdb *CircuitDB) deleteOldBlocks() error {
	if cdb.DataDir == "" {
		return nil
	}

	if cdb.retention == 0 {
		return nil
	}

	//oldEpoch := utils.AddDaysToEpoch(cdb.lastEpoch, -cdb.retention) + 1
	oldEpoch := cdb.lastEpoch - cdb.retention*cdb.unitsecs + 1

	selectOldBlocks := func(v []string) bool {
		lastEpoch := utils.StringToInt64(v[ColLastEpoch])
		return lastEpoch < oldEpoch
	}

	rows, err := cdb.SelectFromStatusTable(selectOldBlocks, []string{"blockNo"})

	if err != nil {
		return err
	}

	var blockNo int
	for rows.Next() {
		if err := rows.Scan(&blockNo); err != nil {
			return err
		}
		if t, err := cdb.GetBlockTable(blockNo); err != nil {
			return err
		} else {
			if err := t.Delete(nil); err != nil {
				return err
			}
		}
	}

	if err := cdb.statusTable.Delete(selectOldBlocks); err != nil {
		return err
	}

	return nil
}

func (cdb *CircuitDB) UpdateBlockStatus(completed bool) error {
	if cdb.DataDir == "" {
		return nil
	}
	blockID := cdb.getBlockTableName(cdb.blockNo)

	if err := cdb.statusTable.Upsert(func(v []string) bool {
		return v[ColBlockNo] == strconv.Itoa(cdb.blockNo)
	}, map[string]interface{}{
		"lastIndex": cdb.lastIndex,
		"blockNo":   cdb.blockNo,
		"blockID":   blockID,
		"rowNo":     cdb.RowNo,
		"lastEpoch": cdb.lastEpoch,
		"completed": completed,
	}); err != nil {
		return errors.WithStack(err)
	}

	if err := cdb.deleteOldBlocks(); err != nil {
		return nil
	}

	return nil
}

func (cdb *CircuitDB) FlushOverwriteCurrentTable() error {
	if err := cdb.currTable.FlushOverwrite(); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func (cdb *CircuitDB) FlushCurrentTable() error {
	if err := cdb.currTable.Flush(); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func (cdb *CircuitDB) SelectFromCurrentTable(conditionCheckFunc func([]string) bool,
	colNames []string) (*Rows, error) {
	return cdb.currTable.SelectRows(conditionCheckFunc, colNames)
}

func (cdb *CircuitDB) Commit() error {
	if cdb.DataDir == "" {
		return nil
	}
	if err := cdb.currTable.Flush(); err != nil {
		return errors.WithStack(err)
	}
	if err := cdb.UpdateBlockStatus(false); err != nil {
		return err
	}
	return nil
}

func (cdb *CircuitDB) CountFromStatusTable(conditionCheckFunc func([]string) bool) int {
	return cdb.statusTable.Count(conditionCheckFunc)
}

func (cdb *CircuitDB) SelectFromStatusTable(conditionCheckFunc func([]string) bool,
	colNames []string) (*Rows, error) {
	return cdb.statusTable.SelectRows(conditionCheckFunc, colNames)
}

func (cdb *CircuitDB) Select1RowFromStatusTable(conditionCheckFunc func([]string) bool,
	colNames []string, args ...interface{}) error {
	return cdb.statusTable.Select1Row(conditionCheckFunc, colNames, args...)
}

func (cdb *CircuitDB) getBlockNos() ([]int, error) {
	cnt := cdb.statusTable.Count(nil)
	if cnt <= 0 {
		return nil, nil
	}
	blockNos := make([]int, cnt)
	rows, err := cdb.statusTable.SelectRows(nil, []string{"blockNo"})
	if err != nil {
		return nil, err
	}

	if rows == nil {
		return nil, nil
	}

	i := 0
	for rows.Next() {
		if err := rows.Scan(&blockNos[i]); err != nil {
			return nil, err
		}
		i++
	}
	return blockNos, nil
}

func (cdb *CircuitDB) SelectRows(conditionCheckFunc func([]string) bool,
	blockNos []int, columns []string) (*circuitRows, error) {
	var err error
	if blockNos == nil {
		blockNos, err = cdb.getBlockNos()
		if err != nil {
			return nil, err
		}
	}

	r := new(circuitRows)
	r.groupName = cdb.Name
	r.tableNames = make([]string, len(blockNos))
	pos := 0
	for _, blockNo := range blockNos {
		r.tableNames[pos] = cdb.getBlockTableName(blockNo)
		pos++
	}

	r.statusTable = cdb.statusTable
	r.CsvDB = cdb.CsvDB
	r.conditionCheckFunc = conditionCheckFunc
	r.columns = columns

	r.pos = 0
	return r, nil
}

func (cdb *CircuitDB) CountAll(conditionCheckFunc func([]string) bool) int {
	blockNos, err := cdb.getBlockNos()
	if err != nil {
		return -1
	}
	cnt := 0
	for _, blockNo := range blockNos {
		t, err := cdb.Groups[cdb.Name].GetTable(cdb.getBlockTableName(blockNo))
		if err != nil {
			return -1
		}
		tcnt := t.Count(conditionCheckFunc)
		if tcnt > 0 {
			cnt += tcnt
		}
	}
	return cnt
}
