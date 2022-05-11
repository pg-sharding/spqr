package spqrlog

import (
	"github.com/pg-sharding/spqr/pkg/config"
	"io"
	"log"
	"os"
)

type Severity int

const (
	DEBUG5 = Severity(iota)
	DEBUG4
	DEBUG3
	DEBUG2
	DEBUG1
	WARNING
	INFO
	LOG
	ERROR
	FATAL
)

var mp = map[string]Severity{
	"DEBUG5":  DEBUG5,
	"DEBUG4":  DEBUG4,
	"DEBUG3":  DEBUG3,
	"DEBUG2":  DEBUG2,
	"DEBUG1":  DEBUG1,
	"WARNING": WARNING,
	"INFO":    INFO,
	"LOG":     LOG,
	"ERROR":   ERROR,
	"FATAL":   FATAL,
}

type errorLogger struct {
	logMp map[Severity]*log.Logger
}

func NewErrorLogger(out io.Writer) *errorLogger {
	el := &errorLogger{
		logMp: make(map[Severity]*log.Logger),
	}

	for k, v := range mp {
		el.logMp[v] = log.New(out, k+": ", log.LstdFlags|log.Lmicroseconds)
	}

	return el
}

var Logger = NewErrorLogger(os.Stdout)

func (el *errorLogger) Printf(severity Severity, fmt string, args ...interface{}) {
	if mp[config.RouterConfig().LogLevel] <= severity {
		el.logMp[severity].Printf(fmt, args...)
	}
}

func (el *errorLogger) Fatalf(severity Severity, fmt string, args ...interface{}) {
	if mp[config.RouterConfig().LogLevel] <= severity {
		el.logMp[severity].Fatalf(fmt, args)
	}
}

func (el *errorLogger) Errorf(fmt string, args ...interface{}) {
	el.logMp[ERROR].Printf(fmt, args)
}

func (el *errorLogger) PrintError(err error) {
	el.logMp[ERROR].Printf("%w", err)
}

func (el *errorLogger) FatalOnError(err error) {
	el.logMp[FATAL].Fatalf("%w", err)
}
