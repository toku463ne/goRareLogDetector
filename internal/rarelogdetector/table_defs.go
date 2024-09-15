package rarelogdetector

var (
	tableDefs = map[string][]string{
		"config":     {"logPath", "blockSize", "maxBlocks", "minMatchRate", "maxMatchRate", "logFormat"},
		"lastStatus": {"lastRowID", "lastFileEpoch", "lastFileRow"},
		"items":      {"count", "createEpoch", "lastUpdate", "item", "lastValue"},
	}
)
