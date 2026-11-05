package spqrlog

import (
	"fmt"
	"io"
	"log"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/diode"
)

// Zero is a singleton object of zerolog.Logger.
// Initialized with structured JSON logging by default (prettyLogging = false)
var WdLog = NewWdLogger("", "info", false, false)

var wdLastDiode *diode.Writer

func CloseWdLogger() {
	if wdLastDiode != nil {
		_ = wdLastDiode.Close()
	}
}

// NewWdLogger initializes a zerolog.Logger.
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
func NewWdLogger(filepath string, logLevel string, async bool, prettyLogging bool) *zerolog.Logger {
	_, writer, err := newWriter(filepath)
	if err != nil {
		log.Printf("failed to initialize logger: %v", err)
	}

	return newWdLoggerInternal(writer, logLevel, async, prettyLogging)
}

func newWdLoggerInternal(writer io.Writer, logLevel string, async bool, prettyLogging bool) *zerolog.Logger {
	level := parseLevel(logLevel)

	if prettyLogging {
		writer = zerolog.ConsoleWriter{Out: writer}
	}

	if async {
		/* TODO: configure this using "common" router/coordinator config */
		d := diode.NewWriter(writer, 1000, 10*time.Millisecond, func(missed int) {
			fmt.Printf("Logger Dropped %d messages", missed)
		})

		wdLastDiode = &d
		writer = d
	} else {
		wdLastDiode = nil
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
func ReloadWdLogger(logFileName string, logLevel string, asyncLog bool, prettyLogging bool) {
	sv := wdLastDiode
	WdLog = NewWdLogger(logFileName, logLevel, asyncLog, prettyLogging)
	if sv != nil {
		_ = sv.Close()
	}
}
