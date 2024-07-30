package rarelogdetector

import (
	"flag"
	"runtime"
	"time"

	"github.com/sirupsen/logrus"
)

// Define command line arguments
var (
	configPath string
	debug      bool
)

func init() {
	// Set up command line flags
	flag.StringVar(&configPath, "config", "config.yaml", "Path to the configuration file")
	flag.BoolVar(&debug, "debug", false, "Enable debug mode")

	// Parse command line flags
	flag.Parse()

	// Set up logging format
	logrus.SetFormatter(&logrus.TextFormatter{
		FullTimestamp:   true,
		TimestampFormat: time.RFC3339,
	})

	// Set log level
	if debug {
		logrus.SetLevel(logrus.DebugLevel)
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

	// Load configuration
	if err := loadConfig(configPath); err != nil {
		logrus.WithField("configPath", configPath).Fatal("Failed to load configuration")
	}

	// Start your application logic
	if err := run(); err != nil {
		logrus.WithError(err).Fatal("Application encountered an error")
	}

	logrus.Info("Application finished successfully")
}

func loadConfig(path string) error {
	// Placeholder for loading configuration logic
	// Example: Read YAML file, parse JSON, etc.
	logrus.WithField("path", path).Info("Loading configuration")
	return nil
}

func run() error {
	// Placeholder for your main application logic
	// Example: Start server, process data, etc.
	logrus.Info("Running application logic")
	return nil
}
