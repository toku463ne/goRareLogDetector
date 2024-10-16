package rarelogdetector

import (
	"fmt"
	"goRareLogDetector/pkg/utils"
	"strconv"
	"strings"
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

	a, err := NewAnalyzer(dataDir, logPath, logFormat, layout, nil, nil, 100, 100, 0, "", 0, 0, 0, 0, nil, nil, nil, false)
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

	phraseID := a.trans.phrases.getItemID("comterm1 comterm2 comterm3 comterm4 comterm5 comterm6 comterm7 comterm8 part006 *")
	phraseCnt := a.trans.phrases.getCount(phraseID)
	if err := utils.GetGotExpErr("phrase count", phraseCnt, 5); err != nil {
		t.Errorf("%v", err)
		return
	}
	lastValue := a.trans.phrases.getLastValue(phraseID)
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

	a, err = NewAnalyzer(dataDir, logPath, logFormat, layout, nil, nil, 100, 100, 0, "", 0, 0, 0, 0, nil, nil, nil, false)
	if err != nil {
		t.Errorf("%v", err)
		return
	}

	phraseID = a.trans.phrases.getItemID("comterm1 comterm2 comterm3 comterm4 comterm5 comterm6 comterm7 comterm8 part006 *")
	lastValue = a.trans.phrases.getLastValue(phraseID)
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

	if err := utils.GetGotExpErr("total count of phrases #2", len(a.trans.phrases.members), 8); err != nil {
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

	a, err = NewAnalyzer(dataDir, logPath, logFormat, layout, nil, nil, 100, 100, 0, "", 0, 0, 0, 0, nil, nil, nil, true)
	if err != nil {
		t.Errorf("%v", err)
		return
	}

	results, err := a.Detect(0.5, 0)
	if err != nil {
		t.Errorf("%v", err)
		return
	}

	if err := utils.GetGotExpErr("results length", len(results), 4); err != nil {
		t.Errorf("%v", err)
		return
	}
	if err := utils.GetGotExpErr("results[0].count", results[0].count, 28); err != nil {
		t.Errorf("%v", err)
		return
	}
	if err := utils.GetGotExpErr("results[2].count", results[3].count, 1); err != nil {
		t.Errorf("%v", err)
		return
	}

}

func Test_Analyzer_Run2(t *testing.T) {
	testDir, err := utils.InitTestDir("Test_Analyzer_Run2")
	if err != nil {
		t.Errorf("%v", err)
		return
	}

	logPath := fmt.Sprintf("%s/sample.log*", testDir)
	logFormat := `^(?P<timestamp>\w+ \d+ \d+:\d+:\d+) (?P<message>.+)$`
	layout := "Jan 2 15:04:05"
	dataDir := testDir + "/data"

	a, err := NewAnalyzer(dataDir, logPath, logFormat, layout, nil, nil, 100, 100, 0, "", 0, 0.8, 0, 0, nil, nil, nil, false)
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

	if err := utils.GetGotExpErr("total count of phrases #1", len(a.trans.phrases.members), 1); err != nil {
		t.Errorf("%v", err)
		return
	}

	phraseID := a.trans.phrases.getItemID("comterm1 comterm2 comterm3 comterm4 comterm5 comterm6 comterm7 comterm8 * *")
	phraseCnt := a.trans.phrases.getCount(phraseID)
	if err := utils.GetGotExpErr("phrase count", phraseCnt, 20); err != nil {
		t.Errorf("%v", err)
		return
	}

	a.Close()
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

	a, err := NewAnalyzer(dataDir, logPath, logFormat, layout, nil, nil, 100, 100, 0, "", 0, 0, 0, 0, nil, nil, nil, false)
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

	res, err := a.TopN(10, 20, 100, false, 0, 0)
	if err != nil {
		t.Errorf("%v", err)
		return
	}

	if err := utils.GetGotExpErr("len", len(res), 9); err != nil {
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

	if err := utils.GetGotExpErr("res[0].count", res[4].Count, 4); err != nil {
		t.Errorf("%v", err)
		return
	}

	a.Close()

	a, err = NewAnalyzer(dataDir, logPath, logFormat, layout, nil, nil, 100, 100, 0, "", 0, 0, 0, 0, nil, nil, nil, false)
	if err != nil {
		t.Errorf("%v", err)
		return
	}

	res, err = a.TopN(10, 20, 100, false, 0.5, 0)
	if err != nil {
		t.Errorf("%v", err)
		return
	}
	if err := utils.GetGotExpErr("len", len(res), 4); err != nil {
		t.Errorf("%v", err)
		return
	}

	if err := utils.GetGotExpErr("res[1].count", res[3].Count, 20); err != nil {
		t.Errorf("%v", err)
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

	a, err := NewAnalyzer(dataDir, logPath, logFormat, layout, nil, nil, 0, 0, 5, "day", 0, 0, 0, 0, nil, nil, nil, false)
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

	blockCount := a.trans.phrases.CountFromStatusTable(nil)
	if err := utils.GetGotExpErr("after feed: block count", blockCount, 5); err != nil {
		t.Errorf("%v", err)
		return
	}

}

func Test_Analyzer_Hourly(t *testing.T) {
	testDir, err := utils.InitTestDir("Test_Analyzer_Hourly")
	if err != nil {
		t.Errorf("%v", err)
		return
	}

	logPath := "../../test/data/rarelogdetector/analyzer/hourly.log*"
	logFormat := `^(?P<timestamp>\d+\-\d+\-\d+ \d+:\d+:\d+)\ (?P<message>.+)$`
	layout := "2006-07-02 15:04:05"
	dataDir := testDir + "/data"

	a, err := NewAnalyzer(dataDir, logPath, logFormat, layout, nil, nil, 100, 100, 10, "hour", 0, 0, 0, 0, nil, nil, nil, false)
	if err != nil {
		t.Errorf("%v", err)
		return
	}

	if err := a.Feed(0); err != nil {
		t.Errorf("%v", err)
		return
	} else {
		if err := utils.GetGotExpErr("lines processed", a.linesProcessed, 8); err != nil {
			t.Errorf("%v", err)
			return
		}
	}

	csvpath := fmt.Sprintf("%s/phrases/phrases/BLK0000000000.csv.gz", dataDir)
	ta, err := utils.ReadColFromCsv(csvpath)
	if err != nil {
		t.Errorf("%v", err)
		return
	}
	if err := utils.GetGotExpErr("line numbers", len(ta), 2); err != nil {
		t.Errorf("%v", err)
		return
	}
	var crt1, upd1 string
	for _, record := range ta {
		if strings.Contains(record[3], "grp1a") {
			cnt, _ := strconv.Atoi(record[0])
			if err := utils.GetGotExpErr("group numbers", cnt, 2); err != nil {
				t.Errorf("%v", err)
				return
			}
			crt1 = record[1]
			upd1 = record[2]
			break
		}
	}
	if err := utils.GetGotExpErr("created < updated", crt1 < upd1, true); err != nil {
		t.Errorf("%v", err)
		return
	}

	csvpath = fmt.Sprintf("%s/phrases/phrases/BLK0000000001.csv.gz", dataDir)
	ta, err = utils.ReadColFromCsv(csvpath)
	if err != nil {
		t.Errorf("%v", err)
		return
	}
	var crt2, upd2 string
	for _, record := range ta {
		if strings.Contains(record[3], "grp1a") {
			crt2 = record[1]
			upd2 = record[2]
			break
		}
	}
	if err := utils.GetGotExpErr("created < updated", crt2 < upd2, true); err != nil {
		t.Errorf("%v", err)
		return
	}
	if err := utils.GetGotExpErr("updated1 <= created2", upd1 <= crt2, true); err != nil {
		t.Errorf("%v", err)
		return
	}

	p := a.trans.phrases
	phraseID1 := p.getItemID("grp1a grp2a grp3a grp4a grp5a *")
	crt := p.getCreateEpoch(phraseID1)
	upd := p.getLastUpdate(phraseID1)
	if err := utils.GetGotExpErr("created == created1", fmt.Sprint(crt), crt1); err != nil {
		t.Errorf("%v", err)
		return
	}
	if err := utils.GetGotExpErr("updated == updated2", fmt.Sprint(upd), upd2); err != nil {
		t.Errorf("%v", err)
		return
	}

	// OutputPhrases test
	if err := a.OutputPhrases(0.5, 0, 0, ",", testDir+"/phrases.csv"); err != nil {
		t.Errorf("%v", err)
		return
	}

	header, records, err := utils.ReadCsv(testDir + "/phrases.csv")
	if err != nil {
		t.Errorf("%v", err)
		return
	}
	if err := utils.GetGotExpErr("len(header)", len(header), 4); err != nil {
		t.Errorf("%v", err)
		return
	}
	if err := utils.GetGotExpErr("len(records)", len(records), 2); err != nil {
		t.Errorf("%v", err)
		return
	}

	if err := utils.GetGotExpErr("records[0][1]", records[0][2], "4"); err != nil {
		t.Errorf("%v", err)
		return
	}

	// OutputPhrasesHistory test
	if err := a.OutputPhrasesHistory(0.5, 0, 10, ",", testDir+"/out"); err != nil {
		t.Errorf("%v", err)
		return
	}

	header, records, err = utils.ReadCsv(testDir + "/out/history.csv")
	if err != nil {
		t.Errorf("%v", err)
		return
	}
	if err := utils.GetGotExpErr("len(header)", len(header), 3); err != nil {
		t.Errorf("%v", err)
		return
	}
	if err := utils.GetGotExpErr("len(records)", len(records), 2); err != nil {
		t.Errorf("%v", err)
		return
	}
	if err := utils.GetGotExpErr("records[0][0]", records[0][0], "2024-01-15 00"); err != nil {
		t.Errorf("%v", err)
		return
	}
	if err := utils.GetGotExpErr("records[0][1]", records[0][1], "2"); err != nil {
		t.Errorf("%v", err)
		return
	}

}

func Test_Analyzer_keywords(t *testing.T) {
	testDir, err := utils.InitTestDir("Test_Analyzer_keywords")
	if err != nil {
		t.Errorf("%v", err)
		return
	}

	logPath := "../../test/data/rarelogdetector/analyzer/hourly.log*"
	logFormat := `^(?P<timestamp>\d+\-\d+\-\d+ \d+:\d+:\d+)\ (?P<message>.+)$`
	layout := "2006-07-02 15:04:05"
	dataDir := testDir + "/data"

	keywords := []string{"uniq003", "uniq008"}
	ignorewords := []string{"grp1a", "grp1b"}
	a, err := NewAnalyzer(dataDir, logPath, logFormat, layout, nil, nil, 100, 100, 10, "", 0, 0, 0, 0,
		keywords, ignorewords, nil, false)
	if err != nil {
		t.Errorf("%v", err)
		return
	}

	if err := a.Feed(0); err != nil {
		t.Errorf("%v", err)
		return
	} else {
		if err := utils.GetGotExpErr("lines processed", a.linesProcessed, 8); err != nil {
			t.Errorf("%v", err)
			return
		}
	}
	if err := utils.GetGotExpErr("len(a.trans.phrases.members)", len(a.trans.phrases.members), 4); err != nil {
		t.Errorf("%v", err)
		return
	}
	_, ok := a.trans.phrases.members["* grp2b grp3b grp4b grp5b uniq003"]
	if err := utils.GetGotExpErr("'* grp2b grp3b grp4b grp5b uniq003' existance", ok, true); err != nil {
		t.Errorf("%v", err)
		return
	}
	_, ok = a.trans.phrases.members["* grp2b grp3b grp4b grp5b *"]
	if err := utils.GetGotExpErr("'* grp2b grp3b grp4b grp5b *' existance", ok, true); err != nil {
		t.Errorf("%v", err)
		return
	}

}

func Test_Analyzer_rearangePhrases(t *testing.T) {
	testDir, err := utils.InitTestDir("Test_Analyzer_rearangePhrases")
	if err != nil {
		t.Errorf("%v", err)
		return
	}

	logPath := "../../test/data/rarelogdetector/analyzer/changablephrases.log*"
	dataDir := testDir + "/data"
	a, err := NewAnalyzer(dataDir, logPath, "", "", nil, nil, 100, 100, 10, "", 0, 0, 0, 0,
		nil, nil, nil, false)
	if err != nil {
		t.Errorf("%v", err)
		return
	}

	if err := a.Feed(0); err != nil {
		t.Errorf("%v", err)
		return
	} else {
		if err := utils.GetGotExpErr("lines processed", a.linesProcessed, 100); err != nil {
			t.Errorf("%v", err)
			return
		}
	}
	// only uniq* words are converted to "*"
	if err := utils.GetGotExpErr("len(a.trans.phrases.members)", len(a.trans.phrases.members), 10); err != nil {
		t.Errorf("%v", err)
		return
	}

	// all members have 10
	for phraseID, cnt := range a.trans.phrases.counts {
		if err := utils.GetGotExpErr(fmt.Sprintf("Count of phraseID=%d", phraseID), cnt, 10); err != nil {
			t.Errorf("%v", err)
			return
		}
	}

	if err := utils.GetGotExpErr("subjects count", len(a.trans.subjects), 10); err != nil {
		t.Errorf("%v", err)
		return
	}

	//Com1, grpa10 Com2 uniq0401 grpa50 uniq0501 <coM3> uniq0601 grpa20 uniq0601
	line := "Com1, grpa10 Com2 uniq0401 grpa50 uniq0501 <coM3> uniq0601 grpa20 uniq0601"
	exgroup := "com1 grpa10 com2 * grpa50 * com3 * grpa20 *"

	phraseCnt, _, phrasestr, err := a.trans.tokenizeLine(line, 1, 0, cStageElse, 0, 0, true)
	if err != nil {
		t.Errorf("%v", err)
		return
	}
	if err := utils.GetGotExpErr("phraseCnt", phraseCnt, 10); err != nil {
		t.Errorf("%v", err)
		return
	}
	if err := utils.GetGotExpErr("phrasestr", phrasestr, exgroup); err != nil {
		t.Errorf("%v", err)
		return
	}

	phraseID := a.trans.phrases.getItemID(phrasestr)
	subject := a.trans.subjects[phraseID]
	expectedSub := "Com1, grpa10 Com2 * grpa50 * <coM3> * grpa20 *"
	if err := utils.GetGotExpErr("subject", subject, expectedSub); err != nil {
		t.Errorf("%v", err)
		return
	}

	a.Close()
	a, err = NewAnalyzer(dataDir, logPath, "", "", nil, nil, 100, 100, 10, "", 0, 0, 0, 0,
		nil, nil, nil, false)
	if err != nil {
		t.Errorf("%v", err)
		return
	}

	a.Close()
	a, err = NewAnalyzer(dataDir, logPath, "", "", nil, nil, 100, 100, 10, "", 0, 0, 0, 0,
		nil, nil, nil, false)
	if err != nil {
		t.Errorf("%v", err)
		return
	}

	line = "com1 grpe10 com2 uniq0901 grpa50 uniq0901 com3 uniq0901 grpc20 uniq0901"
	exgroup = "com1 grpe10 com2 * grpa50 * com3 * grpc20 *"

	// rearange will not change with this rate
	err = a.trans.rearangePhrases(0.8, 0, 0, 0)
	if err != nil {
		t.Errorf("%v", err)
		return
	}
	phraseCnt, _, phrasestr, err = a.trans.tokenizeLine(line, 1, 0, cStageElse, 0, 0, true)
	if err != nil {
		t.Errorf("%v", err)
		return
	}
	if err := utils.GetGotExpErr("phraseCnt", phraseCnt, 10); err != nil {
		t.Errorf("%v", err)
		return
	}
	if err := utils.GetGotExpErr("phrasestr", phrasestr, exgroup); err != nil {
		t.Errorf("%v", err)
		return
	}

	a.Close()
	a, err = NewAnalyzer(dataDir, logPath, "", "", nil, nil, 100, 100, 10, "", 0, 0, 0, 0,
		nil, nil, nil, false)
	if err != nil {
		t.Errorf("%v", err)
		return
	}

	// grpa*10 will be converted to "*"
	err = a.trans.rearangePhrases(0.6, 0, 0, 0)
	if err != nil {
		t.Errorf("%v", err)
		return
	}

	line = "com1 grpa10 com2 uniq0401 grpa50 uniq0501 com3 uniq0601 grpa20 uniq0601"
	exgroup = "com1 * com2 * grpa50 * com3 * grpa20 *"
	phraseCnt, _, phrasestr, err = a.trans.tokenizeLine(line, 1, 0, cStageElse, 0, 0, true)
	if err != nil {
		t.Errorf("%v", err)
		return
	}
	if err := utils.GetGotExpErr("phraseCnt", phraseCnt, 20); err != nil {
		t.Errorf("%v", err)
		return
	}
	if err := utils.GetGotExpErr("phrasestr", phrasestr, exgroup); err != nil {
		t.Errorf("%v", err)
		return
	}

	a.Close()
	a, err = NewAnalyzer(dataDir, logPath, "", "", nil, nil, 100, 100, 10, "", 0, 0, 0, 0,
		nil, nil, nil, false)
	if err != nil {
		t.Errorf("%v", err)
		return
	}

	// grpa*10 will be converted to "*"
	err = a.trans.rearangePhrases(0.5, 0, 0, 0)
	if err != nil {
		t.Errorf("%v", err)
		return
	}

	line = "com1 grpa10 com2 uniq0401 grpa50 uniq0501 com3 uniq0601 grpa20 uniq0601"
	exgroup = "com1 * com2 * grpa50 * com3 * * *"
	phraseCnt, _, phrasestr, err = a.trans.tokenizeLine(line, 1, 0, cStageElse, 0, 0, true)
	if err != nil {
		t.Errorf("%v", err)
		return
	}
	if err := utils.GetGotExpErr("phraseCnt", phraseCnt, 50); err != nil {
		t.Errorf("%v", err)
		return
	}
	if err := utils.GetGotExpErr("phrasestr", phrasestr, exgroup); err != nil {
		t.Errorf("%v", err)
		return
	}

}

func Test_Analyzer_customPhrases(t *testing.T) {
	testDir, err := utils.InitTestDir("Test_Analyzer_customPhrases")
	if err != nil {
		t.Errorf("%v", err)
		return
	}

	logPath := "../../test/data/rarelogdetector/analyzer/changablephrases.log*"
	dataDir := testDir + "/data"
	a, err := NewAnalyzer(dataDir, logPath, "", "", nil, nil, 100, 100, 10, "", 0, 0, 0, 0,
		nil, nil, []string{
			"Com1, * Com2 * grpa50 * <coM3> * grpa20 *",
		}, false)
	if err != nil {
		t.Errorf("%v", err)
		return
	}

	if err := a.Feed(0); err != nil {
		t.Errorf("%v", err)
		return
	} else {
		if err := utils.GetGotExpErr("lines processed", a.linesProcessed, 100); err != nil {
			t.Errorf("%v", err)
			return
		}
	}

	if err := a.trans.rearangePhrases(0, 21, 0, 0); err != nil {
		t.Errorf("%v", err)
		return
	}

	s := 0
	for _, v := range a.trans.phrases.counts {
		s += v
	}
	if err := utils.GetGotExpErr("total count", s, 100); err != nil {
		t.Errorf("%v", err)
		return
	}

	if _, ok := a.trans.phrases.members["com1 * com2 * grpa50 * com3 * grpa20 *"]; !ok {
		t.Error("custom phrase not registered")
		return
	}

}
