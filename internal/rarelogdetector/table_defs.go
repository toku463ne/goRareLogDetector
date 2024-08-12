package rarelogdetector

var (
	tableDefs = map[string][]string{
		"config": {"logPath", "logFormat",
			"blockSize", "maxBlocks",
			"filterRe", "xFilterRe"},
		"lastStatus": {"lastRowID", "lastFileEpoch", "lastFileRow"},
		"items":      {"count", "lastUpdate", "item", "lastValue"},
	}
)
