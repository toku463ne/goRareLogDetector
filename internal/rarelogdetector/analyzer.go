package rarelogdetector

import (
	"fmt"
	"goRareLogDetector/pkg/csvdb"
	"goRareLogDetector/pkg/filepointer"
	"goRareLogDetector/pkg/utils"
	"math"
	"regexp"

	"github.com/sirupsen/logrus"
)

type analyzer struct {
	*csvdb.CsvDB
	dataDir         string
	logPath         string
	logFormat       string
	timestampLayout string
	blockSize       int
	maxBlocks       int
	daysToKeep      int
	configTable     *csvdb.Table
	lastStatusTable *csvdb.Table
	trans           *trans
	fp              *filepointer.FilePointer
	filterRe        *regexp.Regexp
	xFilterRe       *regexp.Regexp
	lastFileEpoch   int64
	lastFileRow     int
	rowID           int64
	readOnly        bool
	linesProcessed  int
}

type phraseCnt struct {
	count int
	line  string
}

func NewAnalyzer(dataDir, logPath, logFormat, timestampLayout string,
	searchRegex, exludeRegex string,
	maxBlocks, blockSize, daysToKeep int,
	readOnly bool) (*analyzer, error) {
	a := new(analyzer)
	a.dataDir = dataDir
	a.logPath = logPath
	a.logFormat = logFormat
	a.timestampLayout = timestampLayout
	a.filterRe = utils.GetRegex(searchRegex)
	a.xFilterRe = utils.GetRegex(exludeRegex)
	a.blockSize = blockSize
	a.maxBlocks = maxBlocks
	a.daysToKeep = daysToKeep
	a.readOnly = readOnly

	if err := a.open(); err != nil {
		return nil, err
	}

	return a, nil
}

func NewAnalyzer2(dataDir string, readOnly bool) (*analyzer, error) {
	a := new(analyzer)
	a.dataDir = dataDir
	a.readOnly = readOnly
	if err := a.open(); err != nil {
		return nil, err
	}
	return a, nil
}

func (a *analyzer) open() error {
	if a.dataDir == "" {
		a.initBlocks()
		if err := a.init(); err != nil {
			return err
		}
	} else {
		if utils.PathExist(a.dataDir) {
			if err := a.loadStatus(); err != nil {
				return err
			}
			if err := a.init(); err != nil {
				return err
			}
			if err := a.load(); err != nil {
				return err
			}
		} else {
			a.initBlocks()
			if err := a.init(); err != nil {
				return err
			}
			if err := a.prepareDB(); err != nil {
				return err
			}
			if err := a.saveLastStatus(); err != nil {
				return err
			}
			if err := a.saveConfig(); err != nil {
				return err
			}
		}
	}
	return nil
}

/*
func (a *analyzer) initBlocks() error {
	if a.maxBlocks == 0 && a.blockSize == 0 {
		cnt, fileCnt, err := a.fp.CountNFiles(cNFilesToCheckCount, a.logPath)
		if err != nil {
			return err
		}
		a.calcBlocks(cnt, fileCnt)
	}
	return nil
}

func (a *analyzer) calcBlocks(totalCount int, nFiles int) {
	if nFiles == 0 {
		nFiles = 1
	}
	m := float64(totalCount) / float64(nFiles)
	a.maxBlocks = int(cLogCycle * (float64(m) / float64(a.blockSize)))
}
*/

func (a *analyzer) initBlocks() {
	if a.blockSize == 0 {
		a.blockSize = 10000
	}
	if a.maxBlocks == 0 {
		a.maxBlocks = 100
	}
	if a.trans == nil || a.trans.maxCountByDay == 0 {
		return
	}
	n := int(math.Ceil(float64(a.trans.maxCountByDay) / float64(a.blockSize)))
	m := n * a.daysToKeep
	if m > a.maxBlocks {
		a.maxBlocks = utils.NextDivisibleByN(m, a.maxBlocks)
		a.trans.SetMaxBlocks(a.maxBlocks)
		logrus.Debugf("maxBlocks changed to %d", a.maxBlocks)
	}

}

func (a *analyzer) init() error {
	if a.dataDir != "" && !a.readOnly {
		if err := utils.EnsureDir(a.dataDir); err != nil {
			return err
		}
	}

	trans, err := newTrans(a.dataDir, a.logFormat, a.timestampLayout,
		a.maxBlocks, a.blockSize, a.daysToKeep,
		a.filterRe, a.xFilterRe,
		true, a.readOnly)
	if err != nil {
		return err
	}
	a.trans = trans
	return nil
}

func (a *analyzer) loadStatus() error {
	if a.dataDir != "" {
		if err := a.prepareDB(); err != nil {
			return err
		}
	}
	filterReStr := ""
	xFilterReStr := ""
	/*
		{"logPath", "logFormat",
			"blockSize", "maxBlocks", "maxItemBlocks",
			"filterRe", "xFilterRe"},
	*/
	if err := a.configTable.Select1Row(nil,
		tableDefs["config"],
		&a.logPath, &a.logFormat,
		&a.blockSize, &a.maxBlocks,
		&filterReStr, &xFilterReStr); err != nil {
		return err
	}

	a.filterRe = utils.GetRegex(filterReStr)
	a.xFilterRe = utils.GetRegex(xFilterReStr)

	if a.lastFileEpoch == 0 {
		if err := a.lastStatusTable.Select1Row(nil,
			[]string{"lastRowID", "lastFileEpoch", "lastFileRow"},
			&a.rowID, &a.lastFileEpoch, &a.lastFileRow); err != nil {
			return err
		}

	}

	return nil
}

func (a *analyzer) load() error {
	if err := a.trans.load(); err != nil {
		return err
	}
	return nil
}

func (a *analyzer) prepareDB() error {
	d, err := csvdb.NewCsvDB(a.dataDir)
	if err != nil {
		return err
	}

	ct, err := d.CreateTableIfNotExists("config", tableDefs["config"], false, 1, 1)
	if err != nil {
		return err
	}
	a.configTable = ct

	ls, err := d.CreateTableIfNotExists("lastStatus", tableDefs["lastStatus"], false, 1, 1)
	if err != nil {
		return err
	}
	a.lastStatusTable = ls

	a.CsvDB = d
	return nil
}

func (a *analyzer) saveLastStatus() error {
	if a.dataDir == "" || a.readOnly {
		return nil
	}

	var epoch int64
	rowNo := 0
	if a.fp != nil {
		epoch = a.fp.CurrFileEpoch()
		rowNo = a.fp.Row()
	} else {
		epoch = 0
		rowNo = 0
	}

	err := a.lastStatusTable.Upsert(nil, map[string]interface{}{
		"lastRowID":     a.rowID,
		"lastFileEpoch": epoch,
		"lastFileRow":   rowNo,
	})
	return err
}

func (a *analyzer) saveConfig() error {
	if a.readOnly {
		return nil
	}
	filterReStr := utils.Re2str(a.filterRe)
	xFilterReStr := utils.Re2str(a.xFilterRe)

	/*
		{"logPath", "logFormat",
				"blockSize", "maxBlocks", "maxItemBlocks",
				"filterRe", "xFilterRe"}
	*/
	if err := a.configTable.Upsert(nil, map[string]interface{}{
		"logPath":   a.logPath,
		"logFormat": a.logFormat,
		"blockSize": a.blockSize,
		"maxBlocks": a.maxBlocks,
		"filterRe":  filterReStr,
		"xFilterRe": xFilterReStr,
	}); err != nil {
		return err
	}
	return nil
}

func (a *analyzer) commit(completed bool) error {
	if a.readOnly {
		return nil
	}
	if a.dataDir == "" {
		return nil
	}
	if err := a.trans.commit(completed); err != nil {
		return err
	}
	if err := a.saveConfig(); err != nil {
		return err
	}
	if err := a.saveLastStatus(); err != nil {
		return err
	}

	return nil
}

func (a *analyzer) Close() {
	if a == nil {
		return
	}

	if a.fp != nil {
		a.fp.Close()
	}
	if a.trans != nil {
		a.trans.close()
	}
}

func (a *analyzer) initFilePointer() error {
	var err error
	if a.fp == nil || !a.fp.IsOpen() {
		a.fp, err = filepointer.NewFilePointer(a.logPath, a.lastFileEpoch, a.lastFileRow)
		if err != nil {
			return err
		}
		if err := a.fp.Open(); err != nil {
			return err
		}
	}
	return nil
}

func (a *analyzer) Feed(targetLinesCnt int) error {
	logrus.Infof("Counting terms")
	if _, err := a._run(targetLinesCnt, true, false); err != nil {
		return err
	}

	a.initBlocks()

	logrus.Infof("Analyzing log")
	if _, err := a._run(targetLinesCnt, false, false); err != nil {
		return err
	}
	return nil
}

func (a *analyzer) Detect() ([]phraseCnt, error) {
	logrus.Debug("Starting term registration")
	if _, err := a._run(0, true, false); err != nil {
		return nil, err
	}
	logrus.Debug("Completed term registration")

	a.initBlocks()

	logrus.Debug("Starting log analyzing")
	results, err := a._run(0, false, true)
	if err != nil {
		return nil, err
	}
	logrus.Debug("Completed log analyzing")
	return results, nil
}

func (a *analyzer) DetectAndShow() error {
	results, err := a.Detect()
	if err != nil {
		return err
	}
	for _, res := range results {
		fmt.Printf("%d,%s\n", res.count, res.line)
	}
	return nil
}

func (a *analyzer) TopN(N, minCnt, days int) ([]phraseScore, error) {
	if err := a.Feed(0); err != nil {
		return nil, err
	}
	maxLastUpdate := utils.AddDaysToEpoch(a.trans.latestUpdate, -N)
	phraseScores := a.trans.getTopNScores(N, minCnt, maxLastUpdate)

	return phraseScores, nil
}

func (a *analyzer) TopNShow(N, minCnt, days int) error {
	var err error
	var phraseScores []phraseScore
	phraseScores, err = a.TopN(N, minCnt, days)
	if err != nil {
		return err
	}

	for _, res := range phraseScores {
		fmt.Printf("%d,%f,%s\n", res.Count, res.Score, res.Text)
	}
	return nil
}

func (a *analyzer) _run(targetLinesCnt int, registerPreTerms bool, detectMode bool) ([]phraseCnt, error) {
	var results []phraseCnt
	linesProcessed := 0

	if err := a.initFilePointer(); err != nil {
		return nil, err
	}

	for a.fp.Next() {
		if linesProcessed > 0 && linesProcessed%cLogPerLines == 0 {
			logrus.Infof("processed %d lines", linesProcessed)
		}

		te := a.fp.Text()
		if te == "" {
			//linesProcessed++
			continue
		}

		cnt, err := a.trans.tokenizeLine(te, a.fp.CurrFileEpoch(), true, registerPreTerms)
		if err != nil {
			return nil, err
		}

		if a.fp.IsEOF && (!a.fp.IsLastFile()) {
			if err := a.saveLastStatus(); err != nil {
				return nil, err
			}
		}
		linesProcessed++

		if detectMode {
			p := new(phraseCnt)
			p.count = cnt
			p.line = te
			results = append(results, *p)
		}

		a.rowID++
		if targetLinesCnt > 0 && linesProcessed >= targetLinesCnt {
			break
		}
	}
	if !registerPreTerms && !a.readOnly {
		if err := a.commit(false); err != nil {
			return nil, err
		}
		logrus.Infof("processed %d lines", linesProcessed)
	}
	if registerPreTerms {
		a.trans.preTermRegistered = true
		a.trans.calcStats()
	}
	a.linesProcessed = linesProcessed
	a.fp.Close()
	return results, nil
}
