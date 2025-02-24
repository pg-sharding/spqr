package spqrlog

import (
	"fmt"
	"strings"

	"github.com/rs/zerolog"
)

var Zero = NewZeroLogger("", true)

// NewZeroLogger initializes a zerolog.Logger.
// If prettyLogging is true, it outputs in a human-readable format.
// Prints an error message if writer initialization fails.
//
// Parameters:
// - filepath: Log file path.
// - prettyLogging: Enable pretty logging.
//
// Returns:
// - *zerolog.Logger: Pointer to the logger instance.
func NewZeroLogger(filepath string, prettyLogging bool) *zerolog.Logger {
	_, writer, err := newWriter(filepath)
	if err != nil {
		fmt.Printf("FAILED TO INITIALIZED LOGGER: %v", err)
	}

	if prettyLogging {
		writer = zerolog.ConsoleWriter{Out: writer}
	}

	logger := zerolog.New(writer).With().Timestamp().Logger() // TODO pass level here
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

// ReloadLogger reloads the logger with a new log file or stdout if filepath is empty.
//
// Parameters:
//   - filepath: The log file path.
//   - prettyLogging: Enable pretty logging.
func ReloadLogger(logFileName string, prettyLogging bool) {
	Zero = NewZeroLogger(logFileName, prettyLogging)
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
