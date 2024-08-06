package rarelogdetector

import (
	"fmt"
	"goRareLogDetector/pkg/utils"
	"testing"
	"time"
)

func Test_Analyzer_Run(t *testing.T) {
	testDir, err := utils.InitTestDir("Test_Analyzer_Run")
	if err != nil {
		t.Errorf("%v", err)
		return
	}

	logPath := fmt.Sprintf("%s/sample.log*", testDir)
	logFormat := `^(?P<timestamp>\w+ \d+ \d+:\d+:\d+) (?P<message>.+)$`
	layout := "Jan 2 15:04:05"
	dataDir := testDir + "/data"

	a, err := NewAnalyzer(dataDir, logPath, logFormat, layout, "", "", 3, 3, false)
	if err != nil {
		t.Errorf("%v", err)
		return
	}

	if _, err := utils.CopyFile("../../test/data/rarelogdetector/analyzer/sample.log.1",
		fmt.Sprintf("%s/sample.log.1", testDir)); err != nil {
		t.Errorf("%v", err)
		return
	}

	if err := a.Feed(0); err != nil {
		t.Errorf("%v", err)
		return
	} else {
		if err := utils.GetGotExpErr("lines processed", a.linesProcessed, 20); err != nil {
			t.Errorf("%v", err)
			return
		}
	}

	if err := utils.GetGotExpErr("total count of phrases #1", len(a.trans.phrases.members), 6); err != nil {
		t.Errorf("%v", err)
		return
	}

	phraseID := a.trans.phrases.getItemID("comterm1 comterm2 comterm3 comterm4 comterm5 comterm6 comterm7 comterm8 part006")
	phraseCnt := a.trans.phrases.getCount(phraseID)
	if err := utils.GetGotExpErr("phrase count", phraseCnt, 5); err != nil {
		t.Errorf("%v", err)
		return
	}

	a.Close()

	time.Sleep(3000000000)

	// New log added
	if _, err := utils.CopyFile("../../test/data/rarelogdetector/analyzer/sample.log",
		fmt.Sprintf("%s/sample.log", testDir)); err != nil {
		t.Errorf("%v", err)
		return
	}

	a, err = NewAnalyzer(dataDir, logPath, logFormat, layout, "", "", 4, 5, false)
	if err != nil {
		t.Errorf("%v", err)
		return
	}

	if err := a.Feed(0); err != nil {
		t.Errorf("%v", err)
		return
	} else {
		if err := utils.GetGotExpErr("lines processed", a.linesProcessed, 5); err != nil {
			t.Errorf("%v", err)
			return
		}
	}

	if err := utils.GetGotExpErr("total count of phrases #2", len(a.trans.phrases.members), 8); err != nil {
		t.Errorf("%v", err)
		return
	}

}
