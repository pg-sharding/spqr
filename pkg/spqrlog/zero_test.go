package spqrlog

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

func TestNewZeroLogger_DefaultIsJSON(t *testing.T) {
	var buf bytes.Buffer

	logger := NewZeroLogger("", "info", false, false)
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

func TestReloadLoggerConcurrentLogging(t *testing.T) {
	previous := Zero.logger.Load()
	previousMinimum := SLogger.logMinDurationStatement.Load()
	t.Cleanup(func() {
		CloseLogger()
		Zero.logger.Store(previous)
		SLogger.logMinDurationStatement.Store(previousMinimum)
	})
	logPath := filepath.Join(t.TempDir(), "reload.log")
	ReloadLogger(logPath, "trace", true, false)
	loggerIdentity, statementIdentity := Zero, SLogger
	stop := make(chan struct{})
	var readers sync.WaitGroup
	for range 2 {
		readers.Go(func() {
			ticker := time.NewTicker(time.Millisecond)
			defer ticker.Stop()
			for {
				select {
				case <-stop:
					return
				case <-ticker.C:
					Zero.Trace().Msg("concurrent reload")
					Zero.Debug().Msg("concurrent reload")
					Zero.Info().Msg("concurrent reload")
					Zero.Warn().Msg("concurrent reload")
					Zero.Error().Msg("concurrent reload")
					Zero.Err(errors.New("concurrent reload")).Msg("concurrent reload")
					Zero.Log().Msg("concurrent reload")
					_ = Zero.GetLevel()
					SLogger.ReportStatement(StmtTypeQuery, "SELECT 1", time.Second)
				}
			}
		})
	}
	for i := range 20 {
		ReloadLogger(logPath, "trace", true, false)
		ReloadSLogger(time.Duration(i % 2))
	}
	close(stop)
	readers.Wait()
	CloseLogger()
	// Closing twice and reloading after Close are both safe lifecycle operations.
	CloseLogger()
	ReloadLogger(logPath, "debug", true, false)
	Zero.Debug().Msg("final reload")
	CloseLogger()
	if Zero != loggerIdentity || SLogger != statementIdentity {
		t.Fatal("reload replaced a shared logger identity")
	}
	if !IsDebugLevel() {
		t.Fatal("reload did not publish the new log level")
	}
	content, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(content, []byte("concurrent reload")) || !bytes.Contains(content, []byte("final reload")) {
		t.Fatalf("expected concurrent and final log events, got %d bytes", len(content))
	}
}

func TestReloadStatementLoggerConcurrentReads(t *testing.T) {
	previousMinimum := SLogger.logMinDurationStatement.Load()
	t.Cleanup(func() { SLogger.logMinDurationStatement.Store(previousMinimum) })
	var readers sync.WaitGroup
	for range 4 {
		readers.Go(func() {
			for range 1000 {
				_ = SLogger.shouldLogStatement(time.Second)
			}
		})
	}
	for range 1000 {
		ReloadSLogger(-1)
		ReloadSLogger(0)
	}
	readers.Wait()
	ReloadSLogger(time.Second)
	if SLogger.shouldLogStatement(time.Second) || !SLogger.shouldLogStatement(2*time.Second) {
		t.Fatal("statement logger did not retain threshold semantics")
	}
}
