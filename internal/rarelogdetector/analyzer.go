package rarelogdetector

import (
	"fmt"
	"goRareLogDetector/pkg/csvdb"
	"goRareLogDetector/pkg/filepointer"
	"goRareLogDetector/pkg/utils"
	"math"
	"regexp"
	"sort"

	"github.com/sirupsen/logrus"
)

type Analyzer struct {
	*csvdb.CsvDB
	dataDir         string
	logPath         string
	logFormat       string
	timestampLayout string
	blockSize       int
	maxBlocks       int
	retention       int64
	frequency       string
	configTable     *csvdb.Table
	lastStatusTable *csvdb.Table
	trans           *trans
	fp              *filepointer.FilePointer
	filterRe        []*regexp.Regexp
	xFilterRe       []*regexp.Regexp
	lastFileEpoch   int64
	lastFileRow     int
	rowID           int64
	readOnly        bool
	linesProcessed  int
	minMatchRate    float64
	maxMatchRate    float64
}

type phraseCnt struct {
	count     int
	line      string
	phrasestr string
	tokens    []int
}

type termCntCount struct {
	termCount int
	Count     int
}

func NewAnalyzer(dataDir, logPath, logFormat, timestampLayout string,
	searchRegex, exludeRegex []string,
	maxBlocks, blockSize int,
	retention int64, frequency string,
	minMatchRate, maxMatchRate float64,
	readOnly bool) (*Analyzer, error) {
	a := new(Analyzer)
	a.dataDir = dataDir
	a.logPath = logPath
	a.logFormat = logFormat
	a.timestampLayout = timestampLayout
	a.retention = retention
	a.frequency = frequency

	a.setFilters(searchRegex, exludeRegex)

	a.blockSize = blockSize
	a.maxBlocks = maxBlocks
	a.retention = retention
	if minMatchRate == 0 {
		a.minMatchRate = 0.6
	} else {
		a.minMatchRate = minMatchRate
	}
	if maxMatchRate == 0 {
		a.maxMatchRate = 0.0
	} else {
		a.maxMatchRate = maxMatchRate
	}
	a.readOnly = readOnly

	if err := a.open(); err != nil {
		return nil, err
	}

	if logPath != "" {
		a.logPath = logPath
	}

	return a, nil
}

func NewAnalyzer2(dataDir string,
	searchRegex, exludeRegex []string,
	readOnly bool) (*Analyzer, error) {
	a := new(Analyzer)
	a.dataDir = dataDir
	a.setFilters(searchRegex, exludeRegex)
	a.readOnly = readOnly
	if err := a.open(); err != nil {
		return nil, err
	}
	return a, nil
}

func (a *Analyzer) setFilters(searchRegex, exludeRegex []string) {
	a.filterRe = make([]*regexp.Regexp, 0)
	for _, s := range searchRegex {
		a.filterRe = append(a.filterRe, utils.GetRegex(s))
	}

	a.xFilterRe = make([]*regexp.Regexp, 0)
	for _, s := range exludeRegex {
		a.xFilterRe = append(a.xFilterRe, utils.GetRegex(s))
	}
}

func (a *Analyzer) open() error {
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
func (a *Analyzer) initBlocks() error {
	if a.maxBlocks == 0 && a.blockSize == 0 {
		cnt, fileCnt, err := a.fp.CountNFiles(cNFilesToCheckCount, a.logPath)
		if err != nil {
			return err
		}
		a.calcBlocks(cnt, fileCnt)
	}
	return nil
}

func (a *Analyzer) calcBlocks(totalCount int, nFiles int) {
	if nFiles == 0 {
		nFiles = 1
	}
	m := float64(totalCount) / float64(nFiles)
	a.maxBlocks = int(cLogCycle * (float64(m) / float64(a.blockSize)))
}
*/

func (a *Analyzer) initBlocks() {
	if a.maxBlocks > 0 && a.blockSize > 0 {
		if a.trans != nil {
			a.trans.setBlockSize(a.blockSize)
			a.trans.setMaxBlocks(a.maxBlocks)
		}
		return
	}

	if a.trans == nil || a.trans.maxCountByBlock == 0 {
		return
	}

	maxCountByBlock := a.trans.maxCountByBlock
	retention := a.retention
	if retention == 0 {
		retention = 30
	}

	if a.blockSize == 0 {
		if maxCountByBlock < 3000 {
			a.blockSize = 10000
		} else if maxCountByBlock < 30000 {
			a.blockSize = 100000
		} else if maxCountByBlock < 300000 {
			a.blockSize = 100000
		} else {
			a.blockSize = 1000000
		}
	}

	if a.maxBlocks == 0 {
		n := int(math.Ceil(float64(a.trans.maxCountByBlock) / float64(a.blockSize)))
		a.maxBlocks = n * int(a.retention)
	}
	a.trans.setBlockSize(a.blockSize)
	a.trans.setMaxBlocks(a.maxBlocks)

}

func (a *Analyzer) init() error {
	if a.dataDir != "" && !a.readOnly {
		if err := utils.EnsureDir(a.dataDir); err != nil {
			return err
		}
	}

	trans, err := newTrans(a.dataDir, a.logFormat, a.timestampLayout,
		a.maxBlocks, a.blockSize,
		a.retention, a.frequency,
		a.filterRe, a.xFilterRe,
		true, a.readOnly)
	if err != nil {
		return err
	}
	a.trans = trans
	return nil
}

func (a *Analyzer) loadStatus() error {
	if a.dataDir != "" {
		if err := a.prepareDB(); err != nil {
			return err
		}
	}
	/*
		"config": {"logPath", "blockSize", "maxBlocks", "matchRate",
			"logFormat", "filterRe", "xFilterRe"}
	*/
	if err := a.configTable.Select1Row(nil,
		tableDefs["config"],
		&a.logPath,
		&a.blockSize, &a.maxBlocks, &a.minMatchRate, &a.maxMatchRate,
		&a.logFormat); err != nil {
		return err
	}

	if a.lastFileEpoch == 0 {
		if err := a.lastStatusTable.Select1Row(nil,
			[]string{"lastRowID", "lastFileEpoch", "lastFileRow"},
			&a.rowID, &a.lastFileEpoch, &a.lastFileRow); err != nil {
			return err
		}

	}

	return nil
}

func (a *Analyzer) load() error {
	if err := a.trans.load(); err != nil {
		return err
	}
	return nil
}

func (a *Analyzer) prepareDB() error {
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

func (a *Analyzer) saveLastStatus() error {
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

func (a *Analyzer) saveConfig() error {
	if a.readOnly {
		return nil
	}

	/*
		{"logPath", "logFormat",
				"blockSize", "maxBlocks", "maxItemBlocks",
				"filterRe", "xFilterRe"}
	*/
	if err := a.configTable.Upsert(nil, map[string]interface{}{
		"logPath":      a.logPath,
		"blockSize":    a.blockSize,
		"maxBlocks":    a.maxBlocks,
		"minMatchRate": a.minMatchRate,
		"maxMatchRate": a.maxMatchRate,
		"logFormat":    a.logFormat,
	}); err != nil {
		return err
	}
	return nil
}

func (a *Analyzer) commit(completed bool) error {
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

func (a *Analyzer) Close() {
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

func (a *Analyzer) Purge() error {
	a.Close()
	if err := utils.RemoveDirectory(a.dataDir); err != nil {
		return err
	}
	return nil
}

func (a *Analyzer) initFilePointer() error {
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

func (a *Analyzer) Feed(targetLinesCnt int) error {
	logrus.Infof("Counting terms")
	if _, err := a._run(targetLinesCnt, true, false, false); err != nil {
		return err
	}

	a.initBlocks()
	a.trans.currRetentionPos = 0

	logrus.Debug("Starting phrase tree registration")
	if _, err := a._run(0, false, true, false); err != nil {
		return err
	}
	logrus.Debug("Completed phrase tree registration")

	a.initBlocks()
	a.trans.currRetentionPos = 0

	logrus.Infof("Analyzing log")
	if _, err := a._run(targetLinesCnt, false, false, false); err != nil {
		return err
	}

	return nil
}

func (a *Analyzer) Detect(termCountBorderRate float64) ([]phraseCnt, error) {
	logrus.Debug("Starting term registration")
	if _, err := a._run(0, true, false, false); err != nil {
		return nil, err
	}
	logrus.Debug("Completed term registration")

	a.initBlocks()
	a.trans.currRetentionPos = 0

	logrus.Debug("Starting phrase tree registration")
	if _, err := a._run(0, false, true, false); err != nil {
		return nil, err
	}
	logrus.Debug("Completed phrase tree registration")

	a.initBlocks()
	a.trans.currRetentionPos = 0

	logrus.Debug("Starting log analyzing")
	results, err := a._run(0, false, false, true)
	if err != nil {
		return nil, err
	}

	// in case different termCountBorderRate is specified, rearange phrases again
	if termCountBorderRate > 0 {
		a.trans.rearangePhrases(termCountBorderRate)
	}
	p := a.trans.phrases
	for i := range results {
		phraseID, phraseStr := a.trans.registerPhrase(results[i].tokens, 0, "", false, a.minMatchRate, a.maxMatchRate)
		results[i].count = p.getCount(phraseID)
		results[i].phrasestr = phraseStr
	}

	logrus.Debug("Completed log analyzing")
	return results, nil
}

func (a *Analyzer) DetectAndShow(M int, termCountBorderRate float64) error {
	results, err := a.Detect(termCountBorderRate)
	if err != nil {
		return err
	}
	for _, res := range results {
		if res.count >= M {
			fmt.Printf("%d,%s\n", res.count, res.line)
			fmt.Printf("  =>  %s\n\n", res.phrasestr)
		}
	}
	return nil
}

func (a *Analyzer) TopN(N, minCnt, days int,
	showLastText bool, termCountBorderRate float64) ([]phraseScore, error) {
	if err := a.Feed(0); err != nil {
		return nil, err
	}
	maxLastUpdate := utils.AddDaysToEpoch(a.trans.latestUpdate, -N)
	phraseScores := a.trans.getTopNScores(N, minCnt, maxLastUpdate, showLastText, termCountBorderRate)

	return phraseScores, nil
}

func (a *Analyzer) TopNShow(N, minCnt, days int,
	showLastText bool, termCountBorderRate float64) error {
	var err error
	var phraseScores []phraseScore
	phraseScores, err = a.TopN(N, minCnt, days, showLastText, termCountBorderRate)
	if err != nil {
		return err
	}

	for _, res := range phraseScores {
		fmt.Printf("%d,%f,%s\n", res.Count, res.Score, res.Text)
	}
	return nil
}

func (a *Analyzer) termCountCounts() []termCntCount {
	termCounts := a.trans.terms.counts

	// Step 1: Count occurrences using a map
	countMap := make(map[int]int)
	for _, val := range termCounts {
		countMap[val]++
	}

	t := make([]termCntCount, 0)
	for k, v := range countMap {
		t = append(t, termCntCount{k, v})
	}

	sort.Slice(t, func(i, j int) bool {
		return t[i].termCount < t[j].termCount
	})

	return t
}

func (a *Analyzer) TermCountCountsShow(N int) error {
	if _, err := a._run(0, true, false, false); err != nil {
		return err
	}
	counts := a.termCountCounts()

	n := 0
	fmt.Println("termCount,Count")
	for _, c := range counts {
		fmt.Printf("%d,%d\n", c.termCount, c.Count)
		n++
		if n >= N {
			break
		}
	}
	return nil
}

func (a *Analyzer) OutputPhrases(termCountBorderRate float64, delim, outfile string) error {
	if err := a.trans.outputPhrases(termCountBorderRate, delim, outfile); err != nil {
		return err
	}
	return nil
}

func (a *Analyzer) _run(targetLinesCnt int,
	registerPreTerms, registerPT bool, detectMode bool) ([]phraseCnt, error) {
	var results []phraseCnt
	linesProcessed := 0
	registerItems := true
	if registerPT {
		registerItems = false
	}

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

		_, tokens, _, err := a.trans.tokenizeLine(te, a.fp.CurrFileEpoch(), registerItems,
			registerPreTerms, registerPT, a.minMatchRate, a.maxMatchRate)
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
			if a.trans.match(te) {
				results = append(results, phraseCnt{
					tokens: tokens,
					line:   te,
				})
			}
		}

		a.rowID++
		if targetLinesCnt > 0 && linesProcessed >= targetLinesCnt {
			break
		}
	}
	if !registerPreTerms && !registerPT && !a.readOnly {
		if err := a.commit(false); err != nil {
			return nil, err
		}
		logrus.Infof("processed %d lines", linesProcessed)
	}
	if registerPreTerms {
		a.trans.preTermRegistered = true
		//a.trans.calcStats()
		a.trans.calcCountBorder(cTermCountBorderRate)
	} else if registerPT {
		a.trans.ptRegistered = true
	} else {
		a.trans.calcPhrasesScore()
	}
	a.linesProcessed = linesProcessed
	a.fp.Close()

	return results, nil
}

func (a *Analyzer) AnalyzeLine(line string) error {
	if a.dataDir == "" || !utils.PathExist(a.dataDir) {
		return fmt.Errorf("datadir does not exist")
	}
	return a.trans.analyzeLine(line)
}
