package utils

import (
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"syscall"
	"time"
	"unicode"

	"github.com/pkg/errors"
)

// Round ...
func Round(num, places float64) float64 {
	shift := math.Pow(10, places)
	return roundInt(num*shift) / shift
}

// RoundUp ...
func RoundUp(num, places float64) float64 {
	shift := math.Pow(10, places)
	return roundUpInt(num*shift) / shift
}

// RoundDown ...
func RoundDown(num, places float64) float64 {
	shift := math.Pow(10, places)
	return math.Trunc(num*shift) / shift
}

func Str2date(dateFormat, dateStr string) (time.Time, error) {
	parsedDate, err := time.Parse(dateFormat, dateStr)
	if err != nil {
		return parsedDate, err
	}

	currentTime := time.Now()
	currentYear := currentTime.Year()
	parsedMonth := parsedDate.Month()

	// Check if the current month is earlier than the parsed month
	if currentTime.Month() < parsedMonth {
		currentYear--
	}

	finalDate := time.Date(currentYear, parsedMonth, parsedDate.Day(),
		parsedDate.Hour(), parsedDate.Minute(), parsedDate.Second(), 0, time.Local)

	return finalDate, nil
}

// roundInt
func roundInt(num float64) float64 {
	t := math.Trunc(num)
	if math.Abs(num-t) >= 0.5 {
		return t + math.Copysign(1, num)
	}
	return t
}

// roundInt
func roundUpInt(num float64) float64 {
	t := math.Trunc(num)
	return t + math.Copysign(1, num)
}

func CopyFile(src, dst string) (int64, error) {
	sourceFileStat, err := os.Stat(src)
	if err != nil {
		return 0, err
	}

	if !sourceFileStat.Mode().IsRegular() {
		return 0, fmt.Errorf("%s is not a regular file", src)
	}

	source, err := os.Open(src)
	if err != nil {
		return 0, err
	}
	defer source.Close()

	destination, err := os.Create(dst)
	if err != nil {
		return 0, err
	}
	defer destination.Close()
	nBytes, err := io.Copy(destination, source)
	return nBytes, err
}

func _pivot(a []int64, i, j int) int {
	k := i + 1
	for k <= j && a[i] == a[k] {
		k++
	}
	if k > j {
		return -1
	}
	if a[i] >= a[k] {
		return i
	}
	return k
}

func _partition(a []int64, s []string, i, j int, x int64) int {
	l := i
	r := j

	for l <= r {
		for l <= j && a[l] < x {
			l++
		}
		for r >= i && a[r] >= x {
			r--
		}
		if l > r {
			break
		}
		t := a[l]
		s1 := s[l]
		a[l] = a[r]
		s[l] = s[r]
		a[r] = t
		s[r] = s1
		l++
		r--
	}
	return l
}

func QuickSort(a []int64, s []string, i, j int) {
	if i == j {
		return
	}
	p := _pivot(a, i, j)
	if p != -1 {
		k := _partition(a, s, i, j, a[p])
		QuickSort(a, s, i, k-1)
		QuickSort(a, s, k, j)
	}
}

func GetSortedGlob(pathRegex string) ([]int64, []string, error) {
	fileNames, err := filepath.Glob(pathRegex)
	if err != nil {
		return nil, nil, err
	}
	if fileNames == nil {
		return nil, nil, errors.New(fmt.Sprintf("No files found at %s", pathRegex))
	}
	filesEpoch := make([]int64, len(fileNames))

	for i, fileName := range fileNames {
		file, _ := os.Stat(fileName)
		//ts, _ := times.Stat(fileName)
		t := file.ModTime()
		//t := ts.BirthTime()
		filesEpoch[i] = t.Unix()
	}

	QuickSort(filesEpoch, fileNames, 0, len(fileNames)-1)
	return filesEpoch, fileNames, nil
}

func UniqueStringSplit(s []string) []string {
	m := make(map[string]bool, 0)
	for _, v := range s {
		m[v] = true
	}
	u := make([]string, 0)
	for k := range m {
		u = append(u, k)
	}
	return u
}

// PathExist ..
func PathExist(path string) bool {
	if _, err := os.Stat(path); err != nil {
		return false
	}
	return true
}

func EnsureDir(dirPath string) error {
	if err := os.MkdirAll(dirPath, 0755); err != nil && !os.IsExist(err) {
		return errors.WithStack(err)
	}
	return nil
}

func IsInt(s string) bool {
	if len(s) > 1 && string(s[0]) == "0" {
		return false
	}
	for _, c := range s {
		if !unicode.IsDigit(c) {
			return false
		}
	}
	return true
}

func IsNumeric(s string) bool {
	_, err := strconv.ParseFloat(s, 64)
	return err == nil
}

func GetRegex(reStr string) *regexp.Regexp {
	if reStr == "" {
		return nil
	}

	return regexp.MustCompile(fmt.Sprintf(".*%s.*", reStr))
}

func Re2str(re *regexp.Regexp) string {
	if re == nil {
		return ""
	}
	return re.String()
}

func RemovePath(pathRegex string) error {
	fileNames, _ := filepath.Glob(pathRegex)
	for _, p := range fileNames {
		if err := os.Remove(p); err != nil {
			return errors.WithStack(err)
		}
	}
	return nil
}

func _pivotFloatInt(a []float64, i, j int) int {
	k := i + 1
	for k <= j && a[i] == a[k] {
		k++
	}
	if k > j {
		return -1
	}
	if a[i] >= a[k] {
		return i
	}
	return k
}

func _partitionFloatInt(a []float64, s []int, i, j int, x float64) int {
	l := i
	r := j

	for l <= r {
		for l <= j && a[l] < x {
			l++
		}
		for r >= i && a[r] >= x {
			r--
		}
		if l > r {
			break
		}
		t := a[l]
		s1 := s[l]
		a[l] = a[r]
		s[l] = s[r]
		a[r] = t
		s[r] = s1
		l++
		r--
	}
	return l
}

func QuickSortFloatInt(a []float64, s []int, i, j int) {
	if i == j {
		return
	}
	p := _pivotFloatInt(a, i, j)
	if p != -1 {
		k := _partitionFloatInt(a, s, i, j, a[p])
		QuickSortFloatInt(a, s, i, k-1)
		QuickSortFloatInt(a, s, k, j)
	}
}

func TimespecToTime(ts syscall.Timespec) time.Time {
	return time.Unix(int64(ts.Sec), int64(ts.Nsec))
}

// Struct to hold the value and its original index
type ValueIndex struct {
	Value float64
	Index int
}

func SortIndexByValue(values []float64, isAsc bool) []int {
	indexes := make([]int, len(values))
	for i, _ := range values {
		indexes[i] = i
	}
	if isAsc {
		sort.Slice(indexes, func(i, j int) bool {
			return values[i] < values[j]
		})
	} else {
		sort.Slice(indexes, func(i, j int) bool {
			return values[i] > values[j]
		})
	}
	return indexes
}

func AddDaysToEpoch(epoch int64, days int) int64 {
	epochTime := time.Unix(epoch, 0)
	epochTime = epochTime.AddDate(0, 0, days)
	return epochTime.Unix()
}

func StringToInt64(s string) int64 {
	// Use strconv.ParseInt to convert string to int64
	i, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0
	}
	return i
}
