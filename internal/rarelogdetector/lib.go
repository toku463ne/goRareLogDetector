package rarelogdetector

func getColIdx(tableName, colName string) int {
	cols, ok := tableDefs[tableName]
	if !ok {
		return -1
	}
	for i, col := range cols {
		if col == colName {
			return i
		}
	}
	return -1
}
