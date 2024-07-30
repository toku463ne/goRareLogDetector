package filepointer

import (
	"fmt"
	"goRareLogDetector/pkg/utils"
	"testing"
	"time"
)

func TestFilePointer_run1(t *testing.T) {
	testName := "TestFileRarityAnalyzer_run1"
	testDir, err := utils.InitTestDir(testName)
	if err != nil {
		t.Errorf("%v", err)
		return
	}
	if _, err := utils.CopyFile("../../test/data/filepointer/sample1.log.1.gz",
		fmt.Sprintf("%s/sample1.log.1.gz", testDir)); err != nil {
		t.Errorf("%v", err)
		return
	}
	time.Sleep(time.Second * 2)
	if _, err := utils.CopyFile("../../test/data/filepointer/sample1.log",
		fmt.Sprintf("%s/sample1.log", testDir)); err != nil {
		t.Errorf("%v", err)
		return
	}
	logPathRegex := fmt.Sprintf("%s/sample1.log*", testDir)

	fp, _ := NewFilePointer(logPathRegex, 0, 0)
	if err := fp.Open(); err != nil {
		t.Errorf("%v", err)
		return
	}
	s := []string{"001", "002", "003", "004", "005",
		"006", "007", "008", "009", "010", "011", "012"}

	i := 0
	for fp.Next() {
		te := fp.Text()
		if s[i] != te {
			t.Errorf("want=%s got=%s", s[i], te)
			return
		}
		i++
	}

}

func TestFilePointer_run2(t *testing.T) {
	testName := "TestFilePointer_run2"
	testDir, err := utils.InitTestDir(testName)
	if err != nil {
		t.Errorf("%v", err)
		return
	}

	if _, err := utils.CopyFile("../../test/data/filepointer/long.txt.tar.gz",
		fmt.Sprintf("%s/long.txt.tar.gz", testDir)); err != nil {
		t.Errorf("%v", err)
		return
	}

	logPathRegex := fmt.Sprintf("%s/long.txt.tar.gz", testDir)

	fp, _ := NewFilePointer(logPathRegex, 0, 0)
	if err := fp.Open(); err != nil {
		t.Errorf("%v", err)
		return
	}
	for fp.Next() {
		te := fp.Text()
		println(te)
	}
	if fp.currErr != nil && !fp.IsEOF {
		t.Errorf("%+v", fp.currErr)
		return
	}
}

func Test_countNFiles(t *testing.T) {
	logPathRegex := "../../test/data/filepointer/sample1.log*"
	fp, _ := NewFilePointer(logPathRegex, 0, 0)
	cnt, fileCnt, err := fp.CountNFiles(2, logPathRegex)
	if err != nil {
		t.Errorf("%v", err)
		return
	}
	if cnt != 12 {
		t.Error("count does not match")
	}
	if fileCnt != 2 {
		t.Error("count does not match")
	}
	cnt, fileCnt, err = fp.CountNFiles(1, logPathRegex)
	if err != nil {
		t.Errorf("%v", err)
		return
	}
	if cnt != 9 && cnt != 3 {
		t.Error("count does not match")
	}
	if fileCnt != 1 {
		t.Error("count does not match")
	}
}
