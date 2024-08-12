package main

import (
	"errors"
	"flag"
	"goRareLogDetector/internal/rarelogdetector"
	"os"
	"runtime"
	"time"

	"github.com/sirupsen/logrus"
)

// Define command line arguments
var (
	configPath string

	debug              bool
	silent             bool
	readOnly           bool
	dataDir            string
	logPath            string
	searchString       string
	excludeString      string
	mode               string
	logFormat          string
	timestampLayout    string
	maxBlocks          int
	blockSize          int
	daysToKeep         int
	nLastLinesToDetect int
	N                  int
	M                  int
	D                  int
)

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
	flag.StringVar(&mode, "m", "topN", "Run mode: topN|detect|feed")
	flag.IntVar(&nLastLinesToDetect, "n", 100, "Number of the last log lines to check rarity in detect mode")
	flag.IntVar(&N, "N", 10, "Show Top N rare logs in topN mode")
	flag.IntVar(&M, "M", 1, "Show ony logs appeared M times in topN mode")
	flag.IntVar(&D, "D", 0, "Recent days to show in topN mode")

	logFormat = ""
	timestampLayout = ".*"
	maxBlocks = 100
	blockSize = 10000

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

	logrus.Info("Starting application")
	flag.CommandLine.Parse(os.Args[1:])

	// Load configuration
	if configPath != "" {
		if err := loadConfig(configPath); err != nil {
			logrus.WithField("configPath", configPath).Fatal("Failed to load configuration")
		}
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
	// Placeholder for loading configuration logic
	// Example: Read YAML file, parse JSON, etc.
	logrus.WithField("path", path).Info("Loading configuration")
	return nil
}

func run() error {
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
		err = a.DetectAndShow(nLastLinesToDetect)
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
