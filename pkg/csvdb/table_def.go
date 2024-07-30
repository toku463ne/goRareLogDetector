package csvdb

type TableDef struct {
	groupName string
	tableName string
	path      string
}

func newTableDef(groupName, tableName, path string) *TableDef {
	td := new(TableDef)
	td.groupName = groupName
	td.tableName = tableName
	td.path = path
	return td
}
