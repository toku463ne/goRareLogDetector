package rarelogdetector

import (
	"goRareLogDetector/pkg/csvdb"
	"goRareLogDetector/pkg/filepointer"
	"goRareLogDetector/pkg/utils"
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
	maxItemBlocks   int
	configTable     *csvdb.Table
	lastStatusTable *csvdb.Table
	trans           *trans
	rarePhrases     *rarePhrases
	fp              *filepointer.FilePointer
	filterRe        *regexp.Regexp
	xFilterRe       *regexp.Regexp
	lastFileEpoch   int64
	lastFileRow     int
	rowID           int64
	readOnly        bool
	linesProcessed  int
}

func NewAnalyzer(dataDir, logPath, logFormat, timestampLayout string,
	searchRegex, exludeRegex string,
	maxBlocks, blockSize, maxItemBlocks int,
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
	a.maxItemBlocks = maxItemBlocks
	a.readOnly = readOnly

	if err := a.open(); err != nil {
		return nil, err
	}

	return a, nil
}

func (a *analyzer) open() error {
	if a.dataDir == "" {
		if err := a.initBlocks(); err != nil {
			return err
		}
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
			if err := a.initBlocks(); err != nil {
				return err
			}
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
	a.maxItemBlocks = a.maxBlocks * 5
}

func (a *analyzer) init() error {
	if a.dataDir != "" && !a.readOnly {
		if err := utils.EnsureDir(a.dataDir); err != nil {
			return err
		}
	}

	trans, err := newTrans(a.dataDir, a.logFormat, a.timestampLayout,
		a.maxItemBlocks, a.blockSize, true)
	if err != nil {
		return err
	}
	a.trans = trans
	return nil
}

func (a *analyzer) loadStatus() error {
	if a.dataDir != "" && !a.readOnly {
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
		&a.blockSize, &a.maxBlocks, &a.maxItemBlocks,
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
	if a.readOnly {
		return nil
	}
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
		"logPath":       a.logPath,
		"logFormat":     a.logFormat,
		"blockSize":     a.blockSize,
		"maxBlocks":     a.maxBlocks,
		"maxItemBlocks": a.maxItemBlocks,
		"filterRe":      filterReStr,
		"xFilterRe":     xFilterReStr,
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

func (a *analyzer) Run(targetLinesCnt int) error {
	linesProcessed := 0
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

	for a.fp.Next() {
		if linesProcessed > 0 && linesProcessed%cLogPerLines == 0 {
			logrus.Printf("processed %d lines", linesProcessed)
		}

		te := a.fp.Text()
		if te == "" {
			//linesProcessed++
			continue
		}

		if err := a.trans.tokenizeLine(te, true); err != nil {
			return err
		}

		if a.fp.IsEOF && (!a.fp.IsLastFile()) {
			if err := a.saveLastStatus(); err != nil {
				return err
			}
		}
		linesProcessed++

		a.rowID++
		if targetLinesCnt > 0 && linesProcessed >= targetLinesCnt {
			break
		}
	}
	if err := a.commit(false); err != nil {
		return err
	}
	logrus.Printf("processed %d lines", linesProcessed)
	a.linesProcessed = linesProcessed
	return nil
}
