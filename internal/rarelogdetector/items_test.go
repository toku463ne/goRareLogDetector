package rarelogdetector

import (
	"fmt"
	"goRareLogDetector/pkg/csvdb"
	"goRareLogDetector/pkg/utils"
	"strconv"
	"testing"
	"time"

	"github.com/pkg/errors"
)

func Test_Items(t *testing.T) {
	now := time.Now().Unix()

	//blockNoIdx := getColIdx("circuitDBStatus", "blockNo")
	itemIdx := getColIdx("items", "item")

	registerTran := func(it *items, itemCount int, a ...string) error {
		for _, item := range a {
			if itemID := it.register(item, itemCount, now, "", true); itemID < 0 {
				return errors.New("Failed to register the item " + item)
			}
		}
		return nil
	}
	registerTrans := func(it *items, a [][]string, goNextBlock bool) error {
		for _, tran := range a {
			if err := registerTran(it, 1, tran...); err != nil {
				return err
			}
		}
		if goNextBlock {
			if err := it.next(); err != nil {
				return err
			}
		}
		return nil
	}

	blockExists := func(it *items, blockNo int) bool {
		cnt := it.CountFromStatusTable(func(v []string) bool {
			return v[csvdb.ColBlockNo] == strconv.Itoa(blockNo)
		})
		return cnt > 0
	}

	checkCircuitDBStatus := func(it *items, blockNo int, expected []interface{}) error {
		if !blockExists(it, blockNo) {
			return errors.New("blockNo must be uniq")
		}

		var lastIndex int
		var blockID string
		var rowNo int
		var completed bool
		if err := it.Select1RowFromStatusTable(func(v []string) bool {
			return v[csvdb.ColBlockNo] == strconv.Itoa(blockNo)
		}, []string{"lastIndex", "blockID", "rowNo", "completed"},
			&lastIndex, &blockID, &rowNo, &completed); err != nil {
			return err
		}

		if err := utils.GetGotExpErr("lastIndex", lastIndex, expected[0]); err != nil {
			return err
		}
		if err := utils.GetGotExpErr("blockID", blockID, expected[1]); err != nil {
			return err
		}
		if err := utils.GetGotExpErr("rowNo", rowNo, expected[2]); err != nil {
			return err
		}
		if err := utils.GetGotExpErr("completed", completed, expected[3]); err != nil {
			return err
		}

		return nil
	}

	getItemCountInBlock := func(it *items, blockNo int, item string) int {
		t, _ := it.GetBlockTable(blockNo)
		if !blockExists(it, blockNo) {
			return 0
		}

		itemCount := 0
		if err := t.Sum(func(v []string) bool {
			return v[itemIdx] == item
		}, "count", &itemCount); err != nil {
			return -1
		}

		return itemCount
	}

	checkItemCountInBlock := func(it *items, blockNo int, item string, expCnt int) error {
		cnt := getItemCountInBlock(it, blockNo, item)
		if cnt != expCnt {
			if err := utils.GetGotExpErr(item, cnt, expCnt); err != nil {
				return err
			}
		}
		return nil
	}

	checkItemCount := func(it *items, item string, expCnt int) error {
		itemID := it.getItemID(item)
		if itemID == -1 {
			return errors.New(fmt.Sprintf("item %s is not registered.", item))
		}
		cnt := it.getCount(itemID)
		if cnt != expCnt {
			if err := utils.GetGotExpErr(item, cnt, expCnt); err != nil {
				return err
			}
		}
		return nil
	}

	dataDir, err := utils.InitTestDir("itemsTest")
	if err != nil {
		t.Errorf("%v", err)
		return
	}

	maxBlocks := 3
	//tl, err := newTableLogRecords(dataDir, maxBlocks, maxRowsInBlock)
	it, err := newItems(dataDir, "items", maxBlocks, false)
	if err != nil {
		t.Errorf("%v", err)
		return
	}
	it.DropAll()
	it, err = newItems(dataDir, "items", maxBlocks, false)
	if err != nil {
		t.Errorf("%v", err)
		return
	}

	inRows := [][]string{
		{"test100", "test200", "test301"},
		{"test100", "test200", "test302"},
		{"test100", "test200", "test303"},
	}

	if err := registerTrans(it, inRows, true); err != nil {
		t.Errorf("%v", err)
		return
	}

	if err := checkItemCount(it, "test100", 3); err != nil {
		t.Errorf("%v", err)
		return
	}

	inRows = [][]string{
		{"test100", "test200", "test304"},
		{"test100", "test200", "test305"},
		{"test100", "test201", "test306"},
	}

	if err := registerTrans(it, inRows, true); err != nil {
		t.Errorf("%v", err)
		return
	}

	if err := checkItemCount(it, "test100", 6); err != nil {
		t.Errorf("%v", err)
		return
	}
	if err := checkItemCount(it, "test200", 5); err != nil {
		t.Errorf("%v", err)
		return
	}

	if err := checkItemCountInBlock(it, 0, "test100", 3); err != nil {
		t.Errorf("%v", err)
		return
	}
	if err := checkItemCountInBlock(it, 0, "test200", 3); err != nil {
		t.Errorf("%v", err)
		return
	}

	if err := checkCircuitDBStatus(it, 0,
		[]interface{}{0, "BLK0000000000", 5, true}); err != nil {
		t.Errorf("%v", err)
		return
	}

	inRows = [][]string{
		{"test100", "test201", "test307"},
		{"test100", "test201", "test308"},
		{"test100", "test201", "test309"},
		{"test100", "test201", "test310"},
		{"test100", "test202", "test311"},
	}

	if err := registerTrans(it, inRows, true); err != nil {
		t.Errorf("%v", err)
		return
	}

	inRows = [][]string{
		{"test100", "test202", "test312"},
		{"test100", "test202", "test313"},
		{"test100", "test202", "test314"},
		{"test100", "test202", "test315"},
		{"test100", "test203", "test316"},
	}

	if err := registerTrans(it, inRows, true); err != nil {
		t.Errorf("%v", err)
		return
	}

	/*
			Data at this point.
			We executed next() after registering test316
			currentTable switched to BLK0000000001
			We remove the old data on BLK0000000001 from RAM because BLK0000000001 is going to be overwritten
		BLK0000000000.csv
		1,1722341583,test315
		1,1722341583,test312
		1,1722341583,test314
		4,1722341583,test202
		1,1722341583,test316
		1,1722341583,test313
		5,1722341583,test100
		1,1722341583,test203

		BLK0000000001.csv
		1,1722341583,test201
		1,1722341583,test306
		1,1722341583,test304
		2,1722341583,test200
		1,1722341583,test305
		3,1722341583,test100

		BLK0000000002.csv
		1,1722341583,test310
		1,1722341583,test308
		1,1722341583,test202
		5,1722341583,test100
		1,1722341583,test309
		1,1722341583,test307
		4,1722341583,test201
		1,1722341583,test311
	*/
	if err := checkItemCount(it, "test100", 10); err != nil {
		t.Errorf("%v", err)
		return
	}
	if err := checkItemCount(it, "test200", 0); err != nil {
		t.Errorf("%v", err)
		return
	}

	if err := checkCircuitDBStatus(it, 0,
		[]interface{}{3, "BLK0000000000", 8, true}); err != nil {
		t.Errorf("%v", err)
		return
	}
	if err := checkCircuitDBStatus(it, 1,
		[]interface{}{1, "BLK0000000001", 6, true}); err != nil {
		t.Errorf("%v", err)
		return
	}
	if err := checkCircuitDBStatus(it, 2,
		[]interface{}{2, "BLK0000000002", 8, true}); err != nil {
		t.Errorf("%v", err)
		return
	}

	inRows = [][]string{
		{"test100", "test203", "test317"},
		{"test100", "test203", "test318"},
		{"test100", "test203", "test319"},
	}

	if err := registerTrans(it, inRows, false); err != nil {
		t.Errorf("%v", err)
		return
	}

	err = it.commit(false)
	if err != nil {
		t.Errorf("%v", err)
		return
	}

	if err := checkItemCount(it, "test100", 13); err != nil {
		t.Errorf("%v", err)
		return
	}
	if err := checkItemCount(it, "test200", 0); err != nil {
		t.Errorf("%v", err)
		return
	}
	if err := checkItemCount(it, "test301", 0); err != nil {
		t.Errorf("%v", err)
		return
	}

	if err := checkCircuitDBStatus(it, 1,
		[]interface{}{4, "BLK0000000001", 5, false}); err != nil {
		t.Errorf("%v", err)
		return
	}

	if err := checkItemCountInBlock(it, 1, "test100", 3); err != nil {
		t.Errorf("%v", err)
		return
	}

	it = nil

	it, err = newItems(dataDir, "items", maxBlocks, false)
	if err != nil {
		t.Errorf("%v", err)
		return
	}
	if err := it.load(); err != nil {
		t.Errorf("%v", err)
		return
	}

	if err := checkItemCount(it, "test100", 13); err != nil {
		t.Errorf("%v", err)
		return
	}
	if err := checkItemCount(it, "test203", 4); err != nil {
		t.Errorf("%v", err)
		return
	}

	inRows = [][]string{
		{"test100", "test203", "test317"},
		{"test100", "test203", "test318"},
		{"test100", "test203", "test319"},
		{"test100", "test203", "test320"},
		{"test100", "test204", "test321"},
	}

	if err := registerTrans(it, inRows, true); err != nil {
		t.Errorf("%v", err)
		return
	}

	if err := checkItemCount(it, "test100", 13); err != nil {
		t.Errorf("%v", err)
		return
	}
	if err := checkItemCount(it, "test203", 8); err != nil {
		t.Errorf("%v", err)
		return
	}

	if err := checkCircuitDBStatus(it, 1,
		[]interface{}{4, "BLK0000000001", 8, true}); err != nil {
		t.Errorf("%v", err)
		return
	}

	if err := checkItemCountInBlock(it, 1, "test100", 8); err != nil {
		t.Errorf("%v", err)
		return
	}
	if err := checkItemCountInBlock(it, 1, "test203", 7); err != nil {
		t.Errorf("%v", err)
		return
	}
	if err := checkItemCountInBlock(it, 1, "test204", 1); err != nil {
		t.Errorf("%v", err)
		return
	}

	it = nil
}
