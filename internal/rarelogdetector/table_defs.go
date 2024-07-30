package rarelogdetector

var (
	tableDefs = map[string][]string{
		"config": {"logPathRegex", "logFormatRegex",
			"blockSize", "maxBlocks", "maxItemBlocks",
			"filterRe", "xFilterRe"},
		"lastStatus":  {"lastRowID", "lastFileEpoch", "lastFileRow"},
		"rarePhrases": {"count", "phrase"},
		"items":       {"count", "lastUpdate", "item"},
	}
)
