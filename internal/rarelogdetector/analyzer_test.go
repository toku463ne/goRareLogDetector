package rarelogdetector

import (
	"fmt"
	"goRareLogDetector/pkg/utils"
	"strings"
	"testing"
)

func Test_Analyzer_Run(t *testing.T) {
	testDir, err := utils.InitTestDir("Test_Analyzer_Run")
	if err != nil {
		t.Errorf("%v", err)
		return
	}

	logPathRegex := fmt.Sprintf("%s/sample.log*", testDir)
	dataDir := testDir + "/data"

	a, err := NewAnalyzer(dataDir, logPathRegex, ".*", "", "", "", 3, 5, 6, false)
	if err != nil {
		t.Errorf("%v", err)
		return
	}

	if _, err := utils.CopyFile("../../test/data/rarelogdetector/analyzer/sample.log.1",
		fmt.Sprintf("%s/sample.log.1", testDir)); err != nil {
		t.Errorf("%v", err)
		return
	}

	if err := a.Run(0); err != nil {
		t.Errorf("%v", err)
		return
	} else {
		if err := utils.GetGotExpErr("lines processed", a.linesProcessed, 31); err != nil {
			t.Errorf("%v", err)
			return
		}
	}

	if err := utils.GetGotExpErr("rowNo", a.rowID, int64(31)); err != nil {
		t.Errorf("%v", err)
		return
	}

	if err := utils.GetGotExpErr("items count", a.trans.terms.CountAll(nil), 41); err != nil {
		t.Errorf("%v", err)
		return
	}

	var it string
	itemIdx := getColIdx("items", "item")
	fu := func(v []string) bool {
		return strings.Contains(v[itemIdx], it)
	}

	it = "test100"
	if err := utils.GetGotExpErr("items count test100",
		a.trans.terms.CountAll(fu), 1); err != nil {
		t.Errorf("%v", err)
		return
	}
	it = "test102"
	if err := utils.GetGotExpErr("items count test102",
		a.trans.terms.CountAll(fu), 1); err != nil {
		t.Errorf("%v", err)
		return
	}
	it = "test3"
	if err := utils.GetGotExpErr("items count test3*",
		a.trans.terms.CountAll(fu), 31); err != nil {
		t.Errorf("%v", err)
		return
	}

	lastFileEpoch := 0
	lastFileRow := 0

	if err := a.lastStatusTable.Select1Row(nil,
		[]string{"lastFileEpoch", "lastFileRow"},
		&lastFileEpoch, &lastFileRow); err != nil {
		t.Errorf("%v", err)
		return
	} else {
		if lastFileEpoch == 0 || lastFileRow == 0 {
			t.Errorf("lastStatus is not properly configured")
			return
		}
	}

	a.Close()

	a, err = NewAnalyzer(dataDir, logPathRegex, ".*", "", "", "", 3, 5, 6, false)
	if err != nil {
		t.Errorf("%v", err)
		return
	}

	if _, err := utils.CopyFile("../../test/data/rarelogdetector/analyzer/sample.log",
		fmt.Sprintf("%s/sample.log", testDir)); err != nil {
		t.Errorf("%v", err)
		return
	}

	if err := a.Run(0); err != nil {
		t.Errorf("%v", err)
		return
	} else {
		if err := utils.GetGotExpErr("lines processed", a.linesProcessed, 4); err != nil {
			t.Errorf("%v", err)
			return
		}
	}
}
