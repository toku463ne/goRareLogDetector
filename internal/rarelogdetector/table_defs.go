package rarelogdetector

var (
	tableDefs = map[string][]string{
		"config": {"logPath", "blockSize", "maxBlocks",
			"retention", "frequency",
			"minMatchRate", "maxMatchRate",
			"termCountBorderRate", "termCountBorder",
			"logFormat"},
		"lastStatus": {"lastRowID", "lastFileEpoch", "lastFileRow"},
		"items":      {"count", "createEpoch", "lastUpdate", "item", "lastValue"},
	}
)
