package spqrlog

import (
	"fmt"
	"io"
	"log"
	"strings"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/diode"
)

// Zero is a singleton object of zerolog.Logger.
// Initialized with structured JSON logging by default (prettyLogging = false)
var Zero = NewZeroLogger("", "info", false, false)

var lastDiode *diode.Writer

func CloseLogger() {
	if lastDiode != nil {
		_ = lastDiode.Close()
	}
}

// NewZeroLogger initializes a zerolog.Logger.
// If prettyLogging is true, it outputs in a human-readable format.
// Prints an error message if writer initialization fails.
//
// Parameters:
// - filepath: Log file path.
// - async: Log asynchronously.
// - prettyLogging: Enable pretty logging.
//
// Returns:
// - *zerolog.Logger: Pointer to the logger instance.
func NewZeroLogger(filePath string, logLevel string, async bool, prettyLogging bool) *zerolog.Logger {
	_, writer, err := newWriter(filePath)
	if err != nil {
		log.Printf("failed to initialize logger: %v", err)
	}
	return newZeroLoggerInternal(writer, logLevel, async, prettyLogging)
}

func newZeroLoggerInternal(writer io.Writer, logLevel string, async bool, prettyLogging bool) *zerolog.Logger {
	level := parseLevel(logLevel)

	if prettyLogging {
		writer = zerolog.ConsoleWriter{Out: writer}
	}

	if async {
		/* TODO: configure this using "common" router/coordinator config */
		d := diode.NewWriter(writer, 1000, 10*time.Millisecond, func(missed int) {
			fmt.Printf("Logger Dropped %d messages", missed)
		})

		lastDiode = &d
		writer = d
	} else {
		lastDiode = nil
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
func ReloadLogger(logFileName string, logLevel string, asyncLog bool, prettyLogging bool) {
	sv := lastDiode
	Zero = NewZeroLogger(logFileName, logLevel, asyncLog, prettyLogging)
	if sv != nil {
		_ = sv.Close()
	}
}

// ReloadLogger reloads the logger with a new log file or stdout if filepath is empty.
//
// Parameters:
//   - filepath: The log file path.
//   - logLevel: the log level
//   - prettyLogging: Enable pretty logging.
func ReloadRouterLoggers(logFileName string, wdLogFileName string, logLevel string, asyncLog bool, prettyLogging bool) {
	if logFileName == wdLogFileName || wdLogFileName == "" {
		sv := lastDiode
		wdSv := wdLastDiode
		_, writer, err := newWriter(logFileName)
		if err != nil {
			log.Printf("failed to initialize logger: %v", err)
		}
		Zero = newZeroLoggerInternal(writer, logLevel, asyncLog, prettyLogging)
		WdLog = newWdLoggerInternal(writer, logLevel, asyncLog, prettyLogging)
		if sv != nil {
			_ = sv.Close()
		}
		if wdSv != nil {
			_ = wdSv.Close()
		}
		return
	}
	ReloadLogger(logFileName, logLevel, asyncLog, prettyLogging)
	ReloadWdLogger(wdLogFileName, logLevel, asyncLog, prettyLogging)
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
	case "trace":
		return zerolog.TraceLevel
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
