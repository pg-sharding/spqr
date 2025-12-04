package spqrlog

import (
	"bytes"
	"strings"
	"testing"

	"github.com/rs/zerolog"
)

func TestNewZeroLogger_DefaultIsJSON(t *testing.T) {
	var buf bytes.Buffer

	logger := NewZeroLogger("", "info", false)
	l := logger.Output(&buf)
	l.Info().Msg("test message")

	out := buf.String()

	if !strings.Contains(out, `"level":"info"`) {
		t.Fatalf("expected JSON output with level field, got: %s", out)
	}
	if !strings.Contains(out, `"message":"test message"`) {
		t.Fatalf("expected JSON output with message field, got: %s", out)
	}
}

func TestZeroDefaultLevelIsInfo(t *testing.T) {
	level := Zero.GetLevel()
	if level != zerolog.InfoLevel {
		t.Fatalf("expected default log level to be Info, got: %v", level)
	}
}
