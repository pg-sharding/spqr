package spqrlog

import (
	"fmt"
	"github.com/rs/zerolog"
)

var Zero = NewZeroLogger("")

func NewZeroLogger(filepath string) *zerolog.Logger {
	_, writer, err := newWriter(filepath)
	if err != nil {
		fmt.Printf("FAILED TO INITIALIZED LOGGER: %v", err)
	}
	logger := zerolog.New(writer).With().Timestamp().Logger()

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
