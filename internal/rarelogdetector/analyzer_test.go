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

	a, err := NewAnalyzer(dataDir, logPath, logFormat, layout, nil, nil, 100, 100, 0, 0, false)
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

	if err := utils.GetGotExpErr("total count of phrases #1", len(a.trans.matchedPhrases.members), 6); err != nil {
		t.Errorf("%v", err)
		return
	}

	phraseID := a.trans.matchedPhrases.getItemID("comterm1 comterm2 comterm3 comterm4 comterm5 comterm6 comterm7 comterm8 part006")
	phraseCnt := a.trans.matchedPhrases.getCount(phraseID)
	if err := utils.GetGotExpErr("phrase count", phraseCnt, 5); err != nil {
		t.Errorf("%v", err)
		return
	}
	lastValue := a.trans.matchedPhrases.getLastValue(phraseID)
	explected := "Jul 31 20:24:20 Comterm1 comterm2 comterm3 comterm4 comterm5 comterm6 comterm7 comterm8 part006 uniq020"
	if err := utils.GetGotExpErr("last value", lastValue, explected); err != nil {
		t.Errorf("%v", err)
		return
	}

	a.Close()

	time.Sleep(1000000000)

	// New log added
	if _, err := utils.CopyFile("../../test/data/rarelogdetector/analyzer/sample.log",
		fmt.Sprintf("%s/sample.log", testDir)); err != nil {
		t.Errorf("%v", err)
		return
	}

	a, err = NewAnalyzer(dataDir, logPath, logFormat, layout, nil, nil, 100, 100, 0, 0, false)
	if err != nil {
		t.Errorf("%v", err)
		return
	}

	phraseID = a.trans.matchedPhrases.getItemID("comterm1 comterm2 comterm3 comterm4 comterm5 comterm6 comterm7 comterm8 part006")
	lastValue = a.trans.matchedPhrases.getLastValue(phraseID)
	explected = "Jul 31 20:24:20 Comterm1 comterm2 comterm3 comterm4 comterm5 comterm6 comterm7 comterm8 part006 uniq020"
	if err := utils.GetGotExpErr("last value", lastValue, explected); err != nil {
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

	if err := utils.GetGotExpErr("total count of phrases #2", len(a.trans.matchedPhrases.members), 8); err != nil {
		t.Errorf("%v", err)
		return
	}

	time.Sleep(1000000000)

	// New log added
	if _, err := utils.CopyFile("../../test/data/rarelogdetector/analyzer/sample_new.log",
		fmt.Sprintf("%s/sample.log_new", testDir)); err != nil {
		t.Errorf("%v", err)
		return
	}

	a, err = NewAnalyzer(dataDir, logPath, logFormat, layout, nil, nil, 100, 100, 0, 0, true)
	if err != nil {
		t.Errorf("%v", err)
		return
	}

	results, err := a.Detect()
	if err != nil {
		t.Errorf("%v", err)
		return
	}

	if err := utils.GetGotExpErr("results length", len(results), 4); err != nil {
		t.Errorf("%v", err)
		return
	}
	if err := utils.GetGotExpErr("results[1].matchedCount", results[1].matchedCount, 4); err != nil {
		t.Errorf("%v", err)
		return
	}
	if err := utils.GetGotExpErr("results[2].matchedCount", results[3].matchedCount, 1); err != nil {
		t.Errorf("%v", err)
		return
	}

}

func Test_Analyzer_TopN(t *testing.T) {
	testDir, err := utils.InitTestDir("Test_Analyzer_TopN")
	if err != nil {
		t.Errorf("%v", err)
		return
	}
	logPath := fmt.Sprintf("%s/sample.log*", testDir)
	logFormat := `^(?P<timestamp>\w+ \d+ \d+:\d+:\d+) (?P<message>.+)$`
	layout := "Jan 2 15:04:05"
	dataDir := testDir + "/data"

	a, err := NewAnalyzer(dataDir, logPath, logFormat, layout, nil, nil, 100, 100, 0, 0, false)
	if err != nil {
		t.Errorf("%v", err)
		return
	}

	if _, err := utils.CopyFile("../../test/data/rarelogdetector/analyzer/sample.log.1",
		fmt.Sprintf("%s/sample.log.1", testDir)); err != nil {
		t.Errorf("%v", err)
		return
	}
	time.Sleep(1000000000)

	if _, err := utils.CopyFile("../../test/data/rarelogdetector/analyzer/sample_new2.log",
		fmt.Sprintf("%s/sample.log", testDir)); err != nil {
		t.Errorf("%v", err)
		return
	}

	res, err := a.TopN(3, 1, 100)
	if err != nil {
		t.Errorf("%v", err)
		return
	}

	if err := utils.GetGotExpErr("len", len(res), 3); err != nil {
		t.Errorf("%v", err)
		return
	}

	if err := utils.GetGotExpErr("res[0].count", res[0].Count, 1); err != nil {
		t.Errorf("%v", err)
		return
	}

	if res[0].Score < res[1].Score {
		t.Error()
		return
	}
}

func Test_Analyzer_YearDay(t *testing.T) {
	testDir, err := utils.InitTestDir("Test_Analyzer_Run")
	if err != nil {
		t.Errorf("%v", err)
		return
	}

	logPath := "../../test/data/rarelogdetector/analyzer/yeardays.log"
	logFormat := `^(?P<timestamp>\w+ \d+ \d+:\d+:\d+) (?P<message>.+)$`
	layout := "Jan 2 15:04:05"
	dataDir := testDir + "/data"

	a, err := NewAnalyzer(dataDir, logPath, logFormat, layout, nil, nil, 0, 0, 5, 0, false)
	if err != nil {
		t.Errorf("%v", err)
		return
	}

	if err := utils.GetGotExpErr("before feed", a.maxBlocks, 0); err != nil {
		t.Errorf("%v", err)
		return
	}

	if err := a.Feed(0); err != nil {
		t.Errorf("%v", err)
		return
	}

	if err := utils.GetGotExpErr("after feed", a.maxBlocks, 5); err != nil {
		t.Errorf("%v", err)
		return
	}

	blockCount := a.trans.matchedPhrases.CountFromStatusTable(nil)
	if err := utils.GetGotExpErr("after feed: block count", blockCount, 5); err != nil {
		t.Errorf("%v", err)
		return
	}

}
