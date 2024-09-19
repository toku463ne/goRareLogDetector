package main

import (
	"errors"
	"flag"
	"fmt"
	"goRareLogDetector/internal/rarelogdetector"
	"goRareLogDetector/pkg/utils"
	"io/ioutil"
	"os"
	"regexp"
	"runtime"
	"time"

	"github.com/sirupsen/logrus"
	"gopkg.in/yaml.v3"
)

// Define command line arguments
var (
	configPath string

	debug               bool
	silent              bool
	readOnly            bool
	dataDir             string
	logPath             string
	searchString        string
	excludeString       string
	searchStrings       []string
	excludeStrings      []string
	mode                string
	logFormat           string
	timestampLayout     string
	maxBlocks           int
	blockSize           int
	retention           int64
	frequency           string
	minMatchRate        float64
	maxMatchRate        float64
	N                   int
	M                   int
	D                   int
	termCountBorderRate float64
	showLastText        bool
	line                string
	outputFile          string
	delim               string
	biggestN            int
)

type config struct {
	DataDir         string   `yaml:"dataDir"`
	LogPath         string   `yaml:"logPath"`
	SearchStrings   []string `yaml:"searchString"`
	ExcludeStrings  []string `yaml:"excludeString"`
	LogFormat       string   `yaml:"logFormat"`
	TimestampLayout string   `yaml:"timestampLayout"`
	Retention       int64    `yaml:"retention"`
	Frequency       string   `yaml:"frequency"`
	MinMatchRate    float64  `yaml:"minMatchRate"`
	MaxMatchRate    float64  `yaml:"maxMatchRate"`
}

func init() {
	// Set up command line flags
	flag.StringVar(&configPath, "c", "", "Path to the configuration file")
	flag.BoolVar(&debug, "debug", false, "Enable debug mode")
	flag.BoolVar(&silent, "silent", false, "Enable silent mode")
	flag.BoolVar(&readOnly, "readonly", false, "Read only mode. Do not update data directory.")
	flag.StringVar(&dataDir, "d", "", "Path to the data directory")
	flag.StringVar(&logPath, "f", "", "Log file")
	flag.StringVar(&frequency, "frequency", "day", "Frequency to rotate logs. day|hour")
	flag.StringVar(&searchString, "s", "", "Search string")
	flag.StringVar(&excludeString, "x", "", "Exclude string")
	flag.StringVar(&mode, "m", "", "Run mode: topN|detect|feed|termCounts|analyzeLine|outputPhrases|outputPhrasesHistory")
	flag.Float64Var(&minMatchRate, "minR", 0.6, "It is considered 2 log lines 'match', if more than matchRate number of terms in a log line matches.")
	flag.Float64Var(&maxMatchRate, "maxR", 0.0, "Do not check more terms than this rate when grouping lines")
	flag.IntVar(&N, "N", 0, "Show Top N rare logs in topN mode")
	flag.IntVar(&M, "M", 0, "Show ony logs appeared M times in topN mode")
	flag.IntVar(&D, "retention", 0, "Recent days to show in topN mode")
	flag.Float64Var(&termCountBorderRate, "R", 0.0, "Words that have less number than this rate will be ignored")
	flag.BoolVar(&showLastText, "showLastText", false, "If show the last text in the phrase group instead of the phrase.")
	flag.StringVar(&line, "line", "", "Log line to analyze")
	flag.StringVar(&outputFile, "o", "", "Output file when using -m outputPhrases|outputPhrasesHistory")
	flag.StringVar(&delim, "delim", "", "Deliminator of CSV file when using -m outputPhrases|outputPhrasesHistory")
	flag.IntVar(&biggestN, "biggestN", 100, "Top N biggest groups when -m outputPhrases|outputPhrasesHistory")

	logFormat = ""
	timestampLayout = ""
	maxBlocks = 0
	blockSize = 0
	retention = 0

	// Parse command line flags
	//flag.Parse()

	// Set up logging format
	logrus.SetFormatter(&logrus.TextFormatter{
		FullTimestamp:   true,
		TimestampFormat: time.RFC3339,
	})

	// Set log level
	if debug {
		logrus.SetLevel(logrus.DebugLevel)
	} else if silent {
		logrus.SetLevel(logrus.ErrorLevel)
	} else {
		logrus.SetLevel(logrus.InfoLevel)
	}
}

func main() {
	defer func() {
		if r := recover(); r != nil {
			buf := make([]byte, 1024)
			n := runtime.Stack(buf, false)
			logrus.WithFields(logrus.Fields{
				"panic": r,
				"stack": string(buf[:n]),
			}).Error("A panic occurred")
		}
	}()

	flag.CommandLine.Parse(os.Args[1:])

	// Load configuration
	if configPath != "" {
		if err := loadConfig(configPath); err != nil {
			logrus.WithField("configPath", configPath).Fatal("Failed to load configuration")
		}
	}

	// Default values
	if mode == "" {
		mode = "topN"
	}
	if N == 0 {
		N = 10
	}
	if M == 0 {
		M = 1
	}

	// Start your application logic
	if err := run(); err != nil {
		logrus.WithError(err).Fatal("Application encountered an error")
	}

	logrus.Debug("Application finished successfully")
}

/*
*
---
dataDir: logdata
logPath: /var/log/syslog*
logFormat:
timestampLayout:
searchString:
excludeString:
retention:
*
*/
func loadConfig(path string) error {
	logrus.WithField("path", path).Info("Loading configuration")

	replaceEnvVars := func(content string) string {
		// Regex to find placeholders of the form {{ VAR }}
		re := regexp.MustCompile(`\{\{\s*(\w+)\s*\}\}`)
		return re.ReplaceAllStringFunc(content, func(placeholder string) string {
			// Extract the variable name from the placeholder
			varName := re.FindStringSubmatch(placeholder)[1]
			// Return the environment variable value or the original placeholder if not found
			return os.Getenv(varName)
		})
	}

	// Read the YAML file
	yamlFile, err := ioutil.ReadFile(path)
	if err != nil {
		logrus.Fatalf("Error reading YAML file: %v", err)
	}
	// Replace all placeholders with environment variable values
	yamlContent := replaceEnvVars(string(yamlFile))

	var c config
	err = yaml.Unmarshal([]byte(yamlContent), &c)
	if err != nil {
		logrus.Fatalf("Error unmarshalling YAML: %v", err)
	}
	if dataDir == "" {
		dataDir = c.DataDir
	}
	if logPath == "" {
		logPath = c.LogPath
	}
	if searchStrings == nil {
		searchStrings = c.SearchStrings
	}
	if excludeStrings == nil {
		excludeStrings = c.ExcludeStrings
	}
	if logFormat == "" {
		logFormat = c.LogFormat
	}
	if timestampLayout == "" {
		timestampLayout = c.TimestampLayout
	}
	if retention == 0 {
		retention = c.Retention
	}
	return nil
}

func run() error {
	logrus.Debug("Starting application")
	var err error
	var a *rarelogdetector.Analyzer

	if len(searchStrings) == 0 && searchString != "" {
		searchStrings = []string{searchString}
	}
	if len(excludeStrings) == 0 && excludeString != "" {
		excludeStrings = []string{excludeString}
	}

	if utils.PathExist(fmt.Sprintf("%s/config.tbl.ini", dataDir)) {
		a, err = rarelogdetector.NewAnalyzer2(dataDir, searchStrings, excludeStrings, readOnly)
	} else {
		a, err = rarelogdetector.NewAnalyzer(dataDir, logPath, logFormat, timestampLayout,
			searchStrings, excludeStrings,
			maxBlocks, blockSize,
			retention, frequency,
			minMatchRate, maxMatchRate,
			readOnly)
	}
	if err != nil {
		return err
	}
	switch mode {
	case "feed":
		err = a.Feed(0)
	case "detect":
		err = a.DetectAndShow(M, termCountBorderRate)
	case "topN":
		err = a.TopNShow(N, M, D, showLastText, termCountBorderRate)
	case "termCounts":
		a.TermCountCountsShow(N)
	case "analyzeLine":
		a.AnalyzeLine(line)
	case "outputPhrases":
		a.OutputPhrases(termCountBorderRate, delim, outputFile)
	case "outputPhrasesHistory":
		a.OutputPhrasesHistory(termCountBorderRate, biggestN, delim, outputFile)
	default:
		err = errors.New("-m: mode must be one of topN|detect|feed|termCounts|analyzeLine|outputPhrases")
	}
	if err != nil {
		return err
	}
	return nil
}
