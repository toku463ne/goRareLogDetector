package utils

import (
	"compress/gzip"
	"encoding/csv"
	"fmt"
	"os"

	"github.com/pkg/errors"
)

func GetGotExpErr(title string, got interface{}, exp interface{}) error {
	if got == exp {
		return nil
	}
	return errors.New(fmt.Sprintf("%s got=%v expected=%v", title, got, exp))
}

func InitTestDir(testname string) (string, error) {
	userDir, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}
	rootDir := fmt.Sprintf("%s/rarelogs/%s", userDir, testname)
	if _, err := os.Stat(rootDir); err == nil {
		os.RemoveAll(rootDir)
	}
	EnsureDir(rootDir)

	return rootDir, nil
}

func ReadColFromCsv(filepath string) ([][]string, error) {
	file, err := os.Open(filepath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	// Create a new gzip reader
	gzReader, err := gzip.NewReader(file)
	if err != nil {
		return nil, err
	}
	defer gzReader.Close()

	// Create a new CSV reader
	csvReader := csv.NewReader(gzReader)

	// Slices to hold the second and third columns
	var table [][]string

	// Read the CSV file line by line
	for {
		record, err := csvReader.Read()
		if err != nil {
			// Check if we have reached the end of the file
			if err.Error() == "EOF" {
				break
			}
			return nil, err
		}

		table = append(table, record)
	}
	return table, nil
}
