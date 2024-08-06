package rarelogdetector

type tranMatchRate struct {
	matchLen  int
	matchRate float64
}

var (
	tranMatchRates = []tranMatchRate{
		{
			matchLen:  1,
			matchRate: 1.0,
		},
		{
			matchLen:  4,
			matchRate: 0.75,
		},
		{
			matchLen:  10,
			matchRate: 0.7,
		},
	}
)
