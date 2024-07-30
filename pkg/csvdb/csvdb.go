package csvdb

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/pkg/errors"
)

type CsvDB struct {
	Groups  map[string]*TableGroup
	baseDir string
}

// NewCsvDB(baseDir) create a new CsvDB object
func NewCsvDB(baseDir string) (*CsvDB, error) {
	db := new(CsvDB)
	db.Groups = make(map[string]*TableGroup)
	db.baseDir = baseDir
	if _, err := os.Stat(baseDir); os.IsNotExist(err) {
		os.Mkdir(baseDir, 0755)
	} else if err != nil {
		return nil, err
	}

	iniFiles, err := filepath.Glob(fmt.Sprintf("%s/*.%s", baseDir, cTblIniExt))
	if err != nil {
		return nil, err
	}
	for _, iniFile := range iniFiles {
		g := new(TableGroup)
		if err := g.load(iniFile); err != nil {
			return nil, err
		}
		db.Groups[g.groupName] = g
	}

	return db, nil
}

func (db *CsvDB) CreateGroup(groupName string,
	columns []string, useGzip bool, bufferSize, readBufferSize int) (*TableGroup, error) {
	g, err := newTableGroup(groupName, db.baseDir, columns, useGzip, bufferSize, readBufferSize)
	if err != nil {
		return nil, err
	}
	db.Groups[groupName] = g
	return g, nil
}

func (db *CsvDB) GetGroup(groupName string) (*TableGroup, error) {
	g, ok := db.Groups[groupName]
	if !ok {
		return nil, errors.New(fmt.Sprintf("Group %s does not exit", groupName))
	}
	return g, nil
}

func (db *CsvDB) createTable(groupName, tableName string,
	columns []string, useGzip bool, bufferSize, readBufferSize int) (*Table, error) {

	if groupName == "" {
		groupName = tableName
	}

	g, ok := db.Groups[groupName]
	var err error
	if ok {
		if db.tableExists(groupName, tableName) {
			return nil, errors.New(fmt.Sprintf("The table %s exists", tableName))
		}
	} else {
		g, err = newTableGroup(groupName, db.baseDir, columns, useGzip, bufferSize, readBufferSize)
		if err != nil {
			return nil, err
		}
	}

	t, err := g.CreateTable(tableName)
	if err != nil {
		return nil, err
	}

	db.Groups[groupName] = g
	return t, nil
}

func (db *CsvDB) CreateTable(tableName string,
	columns []string, useGzip bool, bufferSize, readBufferSize int) (*Table, error) {
	return db.createTable("", tableName, columns, useGzip, bufferSize, readBufferSize)
}

func (db *CsvDB) GetTable(tableName string) (*Table, error) {
	return db.getTable("", tableName)
}

func (db *CsvDB) getTable(groupName, tableName string) (*Table, error) {
	if groupName == "" {
		groupName = tableName
	}
	g, ok := db.Groups[groupName]
	if ok {
		return g.GetTable(tableName)
	} else {
		return nil, errors.New(fmt.Sprintf("The group %s does not exist", groupName))
	}
}

func (db *CsvDB) CloseAll() {
	for _, g := range db.Groups {
		for tableName, _ := range g.tableDefs {
			if t, _ := g.GetTable(tableName); t != nil {
				t.Close()
			}
		}
	}
}

// DropAllTables() drop all tables in the CsvDB object
func (db *CsvDB) DropAll() error {
	for _, g := range db.Groups {
		if err := g.Drop(); err != nil {
			return err
		}
	}
	return nil
}

func (db *CsvDB) dropTable(groupName, tableName string) error {
	if groupName == "" {
		groupName = tableName
	}
	g, ok := db.Groups[groupName]
	if ok {
		return g.DropTable(tableName)
	}
	return nil
}

func (db *CsvDB) DropTable(tableName string) error {
	return db.dropTable("", tableName)
}

func (db *CsvDB) GroupExists(groupName string) bool {
	_, ok := db.Groups[groupName]
	return ok
}

func (db *CsvDB) TableExists(tableName string) bool {
	return db.tableExists("", tableName)
}

func (db *CsvDB) tableExists(groupName, tableName string) bool {
	if groupName == "" {
		groupName = tableName
	}
	g := db.Groups[groupName]
	return g.TableExists(tableName)
}

func (db *CsvDB) CreateTableIfNotExists(tableName string,
	columns []string, useGzip bool, bufferSize, readBufferSize int) (*Table, error) {
	return db.createTableIfNotExists("", tableName, columns, useGzip, bufferSize, readBufferSize)
}

func (db *CsvDB) createTableIfNotExists(groupName, tableName string,
	columns []string, useGzip bool, bufferSize, readBufferSize int) (*Table, error) {
	if groupName == "" {
		groupName = tableName
	}
	g, ok := db.Groups[groupName]
	var err error
	if !ok {
		g, err = db.CreateGroup(groupName, columns, useGzip, bufferSize, readBufferSize)
		if err != nil {
			return nil, err
		}
	}
	return g.CreateTableIfNotExists(tableName)
}
