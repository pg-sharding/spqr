package spqrlog

import (
	"fmt"
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

var defaultLogLevel = INFO

func UpdateDefaultLogLevel(val string) error {
	if len(val) == 0 {
		defaultLogLevel = INFO
		return nil
	}
	if v, ok := mp[val]; !ok {
		return fmt.Errorf("no matching log level found %v", val)
	} else {
		defaultLogLevel = v
	}
	return nil
}

func (el *errorLogger) Printf(severity Severity, fmt string, args ...interface{}) {
	if defaultLogLevel <= severity {
		el.logMp[severity].Printf(fmt, args...)
	}
}

func (el *errorLogger) Fatalf(fmt string, args ...interface{}) {
	el.logMp[FATAL].Fatalf(fmt, args)
}

func (el *errorLogger) Errorf(fmt string, args ...interface{}) {
	el.logMp[ERROR].Printf(fmt, args)
}

func (el *errorLogger) PrintError(err error) {
	el.logMp[ERROR].Printf("%v", err)
}

func (el *errorLogger) FatalOnError(err error) {
	el.logMp[FATAL].Fatalf("%v", err)
}
