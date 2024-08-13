package rarelogdetector

var (
	tableDefs = map[string][]string{
		"config": {"logPath", "blockSize", "maxBlocks",
			"logFormat", "filterRe", "xFilterRe"},
		"lastStatus": {"lastRowID", "lastFileEpoch", "lastFileRow"},
		"items":      {"count", "lastUpdate", "item", "lastValue"},
	}
)
