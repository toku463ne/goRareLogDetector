package rarelogdetector

import (
	"fmt"
	"goRareLogDetector/pkg/csvdb"
	"goRareLogDetector/pkg/filepointer"
	"goRareLogDetector/pkg/utils"
	"math"
	"regexp"
	"sort"
	"strings"

	"github.com/sirupsen/logrus"
)

type Analyzer struct {
	*csvdb.CsvDB
	dataDir             string
	logPath             string
	logFormat           string
	timestampLayout     string
	blockSize           int
	maxBlocks           int
	retention           int64
	frequency           string
	configTable         *csvdb.Table
	lastStatusTable     *csvdb.Table
	trans               *trans
	fp                  *filepointer.FilePointer
	filterRe            []*regexp.Regexp
	xFilterRe           []*regexp.Regexp
	lastFileEpoch       int64
	lastFileRow         int
	rowID               int64
	readOnly            bool
	linesProcessed      int
	minMatchRate        float64
	maxMatchRate        float64
	termCountBorderRate float64
	termCountBorder     int
	keywords            []string
	ignorewords         []string
	customPhrases       []string
}

type phraseCnt struct {
	count     int
	line      string
	phrasestr string
	tokens    []int
}

type termCntCount struct {
	termCount int
	count     int
	terms     string
}

func NewAnalyzer(dataDir, logPath, logFormat, timestampLayout string,
	searchRegex, exludeRegex []string,
	maxBlocks, blockSize int,
	retention int64, frequency string,
	minMatchRate, maxMatchRate float64,
	termCountBorderRate float64,
	termCountBorder int,
	keywords, ignorewords, customPhrases []string,
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
	a.keywords = keywords
	a.ignorewords = ignorewords
	a.customPhrases = customPhrases

	a.termCountBorder = termCountBorder
	if termCountBorderRate == 0 {
		a.termCountBorderRate = cTermCountBorderRate
	} else {
		a.termCountBorderRate = termCountBorderRate
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
			if err := a.saveKeywords(); err != nil {
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
		a.termCountBorderRate,
		a.filterRe, a.xFilterRe,
		a.keywords, a.ignorewords, a.customPhrases,
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
		&a.blockSize, &a.maxBlocks,
		&a.retention, &a.frequency,
		&a.minMatchRate, &a.maxMatchRate,
		&a.termCountBorderRate,
		&a.termCountBorder,
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
	if err := a.loadKeywords(); err != nil {
		return err
	}

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

	if err := a.configTable.Upsert(nil, map[string]interface{}{
		"logPath":             a.logPath,
		"blockSize":           a.blockSize,
		"maxBlocks":           a.maxBlocks,
		"retention":           a.retention,
		"frequency":           a.frequency,
		"minMatchRate":        a.minMatchRate,
		"maxMatchRate":        a.maxMatchRate,
		"termCountBorderRate": a.termCountBorderRate,
		"termCountBorder":     a.termCountBorder,
		"logFormat":           a.logFormat,
	}); err != nil {
		return err
	}
	return nil
}

func (a *Analyzer) getKeywordsFilePath() string {
	return fmt.Sprintf("%s/keywords.txt", a.dataDir)
}
func (a *Analyzer) getIgnorewordsFilePath() string {
	return fmt.Sprintf("%s/ignorewords.txt", a.dataDir)
}

func (a *Analyzer) saveKeywords() error {
	if err := utils.Slice2File(a.keywords, a.getKeywordsFilePath()); err != nil {
		return err
	}
	return utils.Slice2File(a.keywords, a.getIgnorewordsFilePath())
}

func (a *Analyzer) loadKeywords() error {
	var err error
	keywordsPath := a.getKeywordsFilePath()
	if utils.PathExist(keywordsPath) {
		a.keywords, err = utils.ReadFile2Slice(keywordsPath)
		if err != nil {
			return err
		}
	}
	ignorewordsPath := a.getIgnorewordsFilePath()
	if utils.PathExist(ignorewordsPath) {
		a.ignorewords, err = utils.ReadFile2Slice(ignorewordsPath)
		if err != nil {
			return err
		}
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
	if err := a.saveKeywords(); err != nil {
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
	if _, err := a._run(targetLinesCnt, cStageRegisterTerms, false); err != nil {
		return err
	}

	a.initBlocks()
	a.trans.currRetentionPos = 0

	logrus.Debug("Starting phrase tree registration")
	if _, err := a._run(0, cStageRegisterPT, false); err != nil {
		return err
	}
	logrus.Debug("Completed phrase tree registration")

	a.initBlocks()
	a.trans.currRetentionPos = 0

	logrus.Infof("Analyzing log")
	if _, err := a._run(targetLinesCnt, cStageRegisterPhrases, false); err != nil {
		return err
	}

	return nil
}

func (a *Analyzer) Detect(termCountBorderRate float64, termCountBorder int) ([]phraseCnt, error) {
	logrus.Debug("Starting term registration")
	if _, err := a._run(0, cStageRegisterTerms, false); err != nil {
		return nil, err
	}
	logrus.Debug("Completed term registration")

	a.initBlocks()
	a.trans.currRetentionPos = 0

	logrus.Debug("Starting phrase tree registration")
	if _, err := a._run(0, cStageRegisterPT, false); err != nil {
		return nil, err
	}
	logrus.Debug("Completed phrase tree registration")

	a.initBlocks()
	a.trans.currRetentionPos = 0

	logrus.Debug("Starting log analyzing")
	results, err := a._run(0, cStageRegisterPhrases, true)
	if err != nil {
		return nil, err
	}

	// in case different termCountBorderRate is specified, rearange phrases again
	if termCountBorderRate > 0 {
		a.trans.rearangePhrases(termCountBorderRate, termCountBorder, a.minMatchRate, a.maxMatchRate)
	}
	p := a.trans.phrases
	for i := range results {
		phraseID, phraseStr := a.trans.registerPhrase(results[i].tokens, 0, "", 0, a.minMatchRate, a.maxMatchRate, true)
		results[i].count = p.getCount(phraseID)
		results[i].phrasestr = phraseStr
	}

	logrus.Debug("Completed log analyzing")
	return results, nil
}

func (a *Analyzer) DetectAndShow(M int, termCountBorderRate float64, termCountBorder int) error {
	results, err := a.Detect(termCountBorderRate, termCountBorder)
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
	showLastText bool,
	termCountBorderRate float64, termCountBorder int) ([]phraseScore, error) {
	if err := a.Feed(0); err != nil {
		return nil, err
	}
	maxLastUpdate := utils.AddDaysToEpoch(a.trans.latestUpdate, -N)
	phraseScores := a.trans.getTopNScores(N, minCnt, maxLastUpdate, showLastText,
		termCountBorderRate, termCountBorder,
		a.minMatchRate, a.maxMatchRate)

	return phraseScores, nil
}

func (a *Analyzer) TopNShow(N, minCnt, days int,
	showLastText bool,
	termCountBorderRate float64, termCountBorder int) error {
	var err error
	var phraseScores []phraseScore
	phraseScores, err = a.TopN(N, minCnt, days, showLastText, termCountBorderRate, termCountBorder)
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
	members := a.trans.terms.memberMap

	// Step 1: Count occurrences using a map
	countMap := make(map[int]int)
	wordsMap := make(map[int]string)
	for termID, val := range termCounts {
		countMap[val]++
		if countMap[val] <= 5 {
			w := members[termID]
			if len(w) > 10 {
				w = w[:10] + "..."
			}
			wordsMap[val] += " " + w
		}
	}

	t := make([]termCntCount, 0)
	for termCount, count := range countMap {
		words := strings.TrimSpace(wordsMap[termCount])
		t = append(t, termCntCount{termCount, count, words})
	}

	sort.Slice(t, func(i, j int) bool {
		return t[i].termCount > t[j].termCount
	})

	return t
}

func (a *Analyzer) TermCountCountsShow(N int) error {
	if _, err := a._run(0, cStageElse, false); err != nil {
		return err
	}
	counts := a.termCountCounts()

	n := 0
	fmt.Println("termCount,count,samples")
	for _, c := range counts {
		fmt.Printf("%d,%d,%s\n", c.termCount, c.count, c.terms)
		n++
		if n >= N {
			break
		}
	}
	return nil
}

func (a *Analyzer) OutputPhrases(termCountBorderRate float64, termCountBorder int,
	biggestN int,
	delim, outfile string) error {
	if termCountBorderRate == 0 {
		termCountBorderRate = a.termCountBorderRate
	}
	if err := a.trans.outputPhrases(termCountBorderRate, termCountBorder,
		biggestN,
		a.minMatchRate, a.maxMatchRate,
		delim, outfile); err != nil {
		return err
	}
	return nil
}

func (a *Analyzer) OutputPhrasesHistory(termCountBorderRate float64, termCountBorder int,
	biggestN int,
	delim, outfile string) error {
	if termCountBorderRate == 0 {
		termCountBorderRate = a.termCountBorderRate
	}
	if err := a.trans.outputPhrasesHistory(termCountBorderRate, termCountBorder,
		a.minMatchRate, a.maxMatchRate,
		biggestN,
		delim, outfile); err != nil {
		return err
	}
	return nil
}

func (a *Analyzer) _run(targetLinesCnt int,
	stage int, detectMode bool) ([]phraseCnt, error) {
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

		_, tokens, _, err := a.trans.tokenizeLine(te, a.fp.CurrFileEpoch(), stage,
			a.minMatchRate, a.maxMatchRate)
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
	if stage == cStageRegisterPhrases && !a.readOnly {
		if err := a.commit(false); err != nil {
			return nil, err
		}
		logrus.Infof("processed %d lines", linesProcessed)
	}

	switch stage {
	case cStageRegisterTerms:
		a.trans.calcCountBorder(a.termCountBorderRate, a.termCountBorder)
	case cStageRegisterPT:
		a.trans.ptRegistered = true
	case cStageRegisterPhrases:
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
