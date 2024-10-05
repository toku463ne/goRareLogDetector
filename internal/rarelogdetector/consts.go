package rarelogdetector

const (
	CDefaultBlockSize   = 0
	CDefaultNBlocks     = 0
	CDefaultNItemBlocks = 0

	cIPReStr             = `[0-9]+\.[0-9]+\.[0-9]+.[0-9]+`
	cWordReStr           = `[0-9\pL\p{Mc}\p{Mn}.%]{2,}`
	cDelimiters          = `[\r\n\t"'\\,;[\]<>{}=\(\)|:&\?+/\s!.]+`
	cMaxNumDigits        = 3  // HTTP codes
	cMaxWordLen          = 40 // IPv6
	cMaxTermLength       = 128
	cMaxBlockDitigs      = 10
	cLogCycle            = 14
	cMaxRowID            = int64(9223372036854775806)
	cLogPerLines         = 1000000
	cDefaultBuffSize     = 10000
	cErrorKeywords       = "failure|failed|error|down|crit"
	cNFilesToCheckCount  = 5
	cTermCountBorderRate = 0.999
	cCountbyScoreLen     = 100

	cStageElse            = -1
	cStageRegisterTerms   = 1
	cStageRegisterPT      = 2
	cStageRegisterPhrases = 3

	cAsteriskItemID = -1
)
