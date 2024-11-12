package spqrlog

import (
	"fmt"
	"strings"

	"github.com/rs/zerolog"
)

var Zero = NewZeroLogger("")

// NewZeroLogger creates a new ZeroLogger with the specified filepath.
// It initializes a writer and creates a logger with a timestamp.
// The logger is returned as a pointer to a zerolog.Logger.
// If the filepath is empty, the logger will be set to os.Stdout.
// Otherwise, a new log file will be opened and used for logging.
//
// Parameters:
//   - filepath: The path to the log file where the data will be written.
//
// Returns:
//   - *zerolog.Logger: A pointer to the created ZeroLogger.
func NewZeroLogger(filepath string) *zerolog.Logger {
	_, writer, err := newWriter(filepath)
	if err != nil {
		fmt.Printf("FAILED TO INITIALIZED LOGGER: %v", err)
	}
	logger := zerolog.New(writer).With().Timestamp().Logger()

	return &logger
}

// UpdateZeroLogLevel updates the log level of the Zero logger.
// It takes a logLevel string as input and returns an error if any.
// The logLevel string should be one of the following: "debug", "info", "warn", "error", "fatal", "panic".
// If the logLevel string is invalid, the function will return an error.
//
// Parameters:
//   - logLevel: The log level to set for the Zero logger.
//
// Returns:
//   - error: An error if any error occurs during the process.
func UpdateZeroLogLevel(logLevel string) error {
	level := parseLevel(logLevel)
	zeroLogger := Zero.With().Logger().Level(level)
	Zero = &zeroLogger
	return nil
}

// ReloadLogger reloads the logger with a new log file.
// If the filepath is empty, the logger will be set to os.Stdout.
// Otherwise, a new log file will be opened and used for logging.
//
// Parameters:
//   - filepath: The path to the log file where the data will be written.
func ReloadLogger(filepath string) {
	if filepath == "" {
		return // this means os.Stdout, so no need to open new file
	}
	newLogger := NewZeroLogger(filepath).Level(Zero.GetLevel())
	Zero = &newLogger
}

// parseLevel parses the given level string and returns the corresponding zerolog.Level.
// If the level string is not recognized, it defaults to zerolog.InfoLevel.
//
// Parameters:
//   - level: The log level string to parse.
//
// Returns:
//   - zerolog.Level: The corresponding zerolog.Level.
func parseLevel(level string) zerolog.Level {
	switch strings.ToLower(level) {
	case "disabled":
		return zerolog.Disabled
	case "debug":
		return zerolog.DebugLevel
	case "info":
		return zerolog.InfoLevel
	case "warning":
		return zerolog.WarnLevel
	case "error":
		return zerolog.ErrorLevel
	case "fatal":
		return zerolog.FatalLevel
	default:
		return zerolog.InfoLevel
	}
}

func IsDebugLevel() bool {
	return Zero.GetLevel() == zerolog.DebugLevel
}
