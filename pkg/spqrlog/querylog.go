package spqrlog

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/tracelog"
	"github.com/rs/zerolog"
)

type ZeroTraceLogger struct{}

// Log implements [tracelog.Logger].
func (z *ZeroTraceLogger) Log(ctx context.Context, level tracelog.LogLevel, msg string, data map[string]any) {
	var event *zerolog.Event
	switch level {
	case tracelog.LogLevelTrace:
		event = Zero.Trace()
	case tracelog.LogLevelDebug:
		event = Zero.Debug()
	case tracelog.LogLevelInfo:
		event = Zero.Info()
	case tracelog.LogLevelWarn:
		event = Zero.Warn()
	case tracelog.LogLevelError:
		event = Zero.Error()
	case tracelog.LogLevelNone:
		fallthrough
	default:
		return
	}
	event.Str("data", fmt.Sprintf("%s", data)).Msg(msg)
}

var _ tracelog.Logger = &ZeroTraceLogger{}
