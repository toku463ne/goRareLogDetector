package rarelogdetector

var (
	tableDefs = map[string][]string{
		"config":     {"logPath", "blockSize", "maxBlocks", "logFormat"},
		"lastStatus": {"lastRowID", "lastFileEpoch", "lastFileRow"},
		"items":      {"count", "lastUpdate", "item", "lastValue"},
	}
)
