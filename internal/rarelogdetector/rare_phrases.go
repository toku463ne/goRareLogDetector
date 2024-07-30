package rarelogdetector

import (
	"goRareLogDetector/pkg/csvdb"

	"github.com/pkg/errors"
)

type rarePhrases struct {
	*csvdb.CircuitDB
	blockSize int
}

func newRarePhrases(dataDir string,
	maxBlocks, blockSize int) (*rarePhrases, error) {
	lr := new(rarePhrases)
	cdb, err := csvdb.NewCircuitDB(dataDir, "rarePhrases",
		tableDefs["rarePhrases"], maxBlocks, blockSize, true)
	if err != nil {
		return nil, err
	}
	lr.CircuitDB = cdb

	return lr, nil
}

func (lr *rarePhrases) load() error {
	if lr.DataDir == "" {
		return nil
	}
	cnt := lr.CountFromStatusTable(nil)
	if cnt <= 0 {
		return nil
	}

	if err := lr.LoadCircuitDBStatus(); err != nil {
		return err
	}
	return nil
}

func (lr *rarePhrases) insert(count int, record string) error {
	if lr.DataDir == "" {
		return nil
	}
	if err := lr.InsertRow(tableDefs["rarePhrases"],
		count, record); err != nil {
		return errors.WithStack(err)
	}

	if lr.blockSize > 0 && lr.RowNo >= lr.blockSize {
		if lr.DataDir != "" {
			if err := lr.FlushCurrentTable(); err != nil {
				return errors.WithStack(err)
			}
		}
		if err := lr.NextBlock(); err != nil {
			return err
		}
	}
	return nil
}
