package spqrlog

import (
	"os"
	"time"

	"github.com/rs/zerolog"
)

// TODO rewrite log init
var Zero = NewZeroLogger("")

func NewZeroLogger(filepath string) *zerolog.Logger {
	output := zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: time.RFC3339}
	logger := zerolog.New(output).With().Logger()

	return &logger
}

func UpdateZeroLogLevel(logLevel string) error {
	level := parseLevel(logLevel)
	zeroLogger := Zero.With().Logger().Level(level)
	Zero = &zeroLogger
	return nil
}

func parseLevel(level string) zerolog.Level {
	switch level {
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
