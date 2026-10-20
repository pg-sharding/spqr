package spqrlog

import (
	"fmt"
	"log"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/diode"
)

// Zero retains its identity during reloads. Each event uses one immutable
// logger snapshot while a replacement is published atomically.
var Zero = newReloadableLogger(NewZeroLogger("", "info", false, false))

var lastDiode *diode.Writer
var loggerMu sync.Mutex

type reloadableLogger struct {
	logger atomic.Pointer[zerolog.Logger]
}

func newReloadableLogger(logger *zerolog.Logger) *reloadableLogger {
	result := &reloadableLogger{}
	result.logger.Store(logger)
	return result
}

func (l *reloadableLogger) Trace() *zerolog.Event        { return l.logger.Load().Trace() }
func (l *reloadableLogger) Debug() *zerolog.Event        { return l.logger.Load().Debug() }
func (l *reloadableLogger) Info() *zerolog.Event         { return l.logger.Load().Info() }
func (l *reloadableLogger) Warn() *zerolog.Event         { return l.logger.Load().Warn() }
func (l *reloadableLogger) Error() *zerolog.Event        { return l.logger.Load().Error() }
func (l *reloadableLogger) Fatal() *zerolog.Event        { return l.logger.Load().Fatal() }
func (l *reloadableLogger) Err(err error) *zerolog.Event { return l.logger.Load().Err(err) }
func (l *reloadableLogger) Log() *zerolog.Event          { return l.logger.Load().Log() }
func (l *reloadableLogger) GetLevel() zerolog.Level      { return l.logger.Load().GetLevel() }

func CloseLogger() {
	loggerMu.Lock()
	defer loggerMu.Unlock()
	if lastDiode != nil {
		_ = lastDiode.Close()
		lastDiode = nil
	}
}

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
func NewZeroLogger(filepath string, logLevel string, async bool, prettyLogging bool) *zerolog.Logger {
	loggerMu.Lock()
	defer loggerMu.Unlock()
	logger, d := newZeroLogger(filepath, logLevel, async, prettyLogging)
	lastDiode = d
	return logger
}

func newZeroLogger(filepath string, logLevel string, async bool, prettyLogging bool) (*zerolog.Logger, *diode.Writer) {
	_, writer, err := newWriter(filepath)
	if err != nil {
		log.Printf("failed to initialize logger: %v", err)
	}

	level := parseLevel(logLevel)

	if prettyLogging {
		writer = zerolog.ConsoleWriter{Out: writer}
	}

	var asyncWriter *diode.Writer
	if async {
		/* TODO: configure this using "common" router/coordinator config */
		d := diode.NewWriter(writer, 1000, 10*time.Millisecond, func(missed int) {
			fmt.Printf("Logger Dropped %d messages", missed)
		})

		asyncWriter = &d
		writer = d
	}

	logger := zerolog.New(writer).With().Timestamp().Logger().Level(level)
	return &logger, asyncWriter
}

// ReloadLogger reloads the logger with a new log file or stdout if filepath is empty.
//
// Parameters:
//   - filepath: The log file path.
//   - logLevel: the log level
//   - prettyLogging: Enable pretty logging.
func ReloadLogger(logFileName string, logLevel string, asyncLog bool, prettyLogging bool) {
	loggerMu.Lock()
	defer loggerMu.Unlock()
	sv := lastDiode
	logger, d := newZeroLogger(logFileName, logLevel, asyncLog, prettyLogging)
	lastDiode = d
	Zero.logger.Store(logger)
	if sv != nil {
		_ = sv.Close()
	}
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
