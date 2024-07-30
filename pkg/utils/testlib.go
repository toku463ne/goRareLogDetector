package utils

import (
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
