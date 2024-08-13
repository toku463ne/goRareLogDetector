package main

import (
	"errors"
	"flag"
	"goRareLogDetector/internal/rarelogdetector"
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

	debug           bool
	silent          bool
	readOnly        bool
	dataDir         string
	logPath         string
	searchString    string
	excludeString   string
	mode            string
	logFormat       string
	timestampLayout string
	maxBlocks       int
	blockSize       int
	daysToKeep      int
	N               int
	M               int
	D               int
)

type config struct {
	DataDir         string `yaml:"dataDir"`
	LogPath         string `yaml:"logPath"`
	SearchString    string `yaml:"searchString"`
	ExcludeString   string `yaml:"excludeString"`
	LogFormat       string `yaml:"logFormat"`
	TimestampLayout string `yaml:"timestampLayout"`
	DaysToKeep      int    `yaml:"daysToKeep"`
}

func init() {
	// Set up command line flags
	flag.StringVar(&configPath, "c", "", "Path to the configuration file")
	flag.BoolVar(&debug, "debug", false, "Enable debug mode")
	flag.BoolVar(&silent, "silent", false, "Enable silent mode")
	flag.BoolVar(&readOnly, "r", false, "Read only mode. Do not update data directory.")
	flag.StringVar(&dataDir, "d", "", "Path to the data directory")
	flag.StringVar(&logPath, "f", "", "Log file")
	flag.StringVar(&searchString, "s", "", "Search string")
	flag.StringVar(&excludeString, "x", "", "Exclude string")
	flag.StringVar(&mode, "m", "", "Run mode: topN|detect|feed")
	flag.IntVar(&N, "N", 0, "Show Top N rare logs in topN mode")
	flag.IntVar(&M, "M", 0, "Show ony logs appeared M times in topN mode")
	flag.IntVar(&D, "D", 0, "Recent days to show in topN mode")

	logFormat = ""
	timestampLayout = ""
	maxBlocks = 100
	blockSize = 10000
	daysToKeep = 0

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

	logrus.Info("Application finished successfully")
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
daysToKeep:
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
	if searchString == "" {
		searchString = c.SearchString
	}
	if excludeString == "" {
		excludeString = c.ExcludeString
	}
	if logFormat == "" {
		logFormat = c.LogFormat
	}
	if timestampLayout == "" {
		timestampLayout = c.TimestampLayout
	}
	if daysToKeep == 0 {
		daysToKeep = c.DaysToKeep
	}
	return nil
}

func run() error {
	logrus.Info("Starting application")

	a, err := rarelogdetector.NewAnalyzer(dataDir, logPath, logFormat, timestampLayout,
		searchString, excludeString,
		maxBlocks, blockSize, daysToKeep,
		readOnly)
	if err != nil {
		return err
	}
	switch mode {
	case "feed":
		err = a.Feed(0)
	case "detect":
		err = a.DetectAndShow()
	case "topN":
		err = a.TopNShow(N, M, D)
	default:
		err = errors.New("-m: mode must be one of topN|detect|feed")
	}
	if err != nil {
		return err
	}
	return nil
}
