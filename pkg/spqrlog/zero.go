package spqrlog

import (
	"log"
	"strings"
	"time"

	"github.com/rs/zerolog"
)

// Zero is a singleton object of zerolog.Logger.
// Initialized with structured JSON logging by default (prettyLogging = false)
var Zero = NewZeroLogger("", "info", false)

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
func NewZeroLogger(filepath string, logLevel string, prettyLogging bool) *zerolog.Logger {
	_, writer, err := newWriter(filepath)
	if err != nil {
		log.Printf("failed to initialize logger: %v", err)
	}

	level := parseLevel(logLevel)

	if prettyLogging {
		writer = zerolog.ConsoleWriter{Out: writer}
	}

	logger := zerolog.New(writer).With().Timestamp().Logger().Level(level)
	return &logger
}

// ReloadLogger reloads the logger with a new log file or stdout if filepath is empty.
//
// Parameters:
//   - filepath: The log file path.
//   - logLevel: the log level
//   - prettyLogging: Enable pretty logging.
func ReloadLogger(logFileName string, logLevel string, prettyLogging bool) {
	Zero = NewZeroLogger(logFileName, logLevel, prettyLogging)
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
		log.Printf("unknown log level: %s, defaulting to info", level)
		return zerolog.InfoLevel
	}
}

func IsDebugLevel() bool {
	return Zero.GetLevel() == zerolog.DebugLevel
}

func ReportStatement(t time.Duration, s string, threshold time.Duration) {
	if t > threshold {
		Zero.Info().Dur("time duration", t).Str("stmt", s).Msg("report statement")
	}
}
