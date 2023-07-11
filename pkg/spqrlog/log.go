package spqrlog

import (
	"fmt"
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
var defaultLogLevel = INFO
var Logger = NewErrorLogger("")

type errorLogger struct {
	file  *os.File
	logMp map[Severity]*log.Logger
}

func NewErrorLogger(filepath string) *errorLogger {
	file, writer, err := newWriter(filepath)
	if err != nil {
		fmt.Printf("FAILED TO INITIALIZED LOGGER: %v", err)
	}

	logMp := make(map[Severity]*log.Logger)
	for k, v := range mp {
		logMp[v] = log.New(writer, k+": ", log.LstdFlags|log.Lmicroseconds)
	}

	return &errorLogger{
		file:  file,
		logMp: logMp,
	}
}

func ReloadLogger(filepath string) {
	if filepath == "" { //
		return // this means os.Stdout, so no need to open new file
	}
	oldFile := Logger.file
	Logger = NewErrorLogger(filepath)
	Zero = NewZeroLogger(filepath)
	if oldFile != nil {
		oldFile.Close()
	}
}

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

func (el *errorLogger) ClientPrintf(severity Severity, fmt string, clientId string, args ...interface{}) {
	if defaultLogLevel <= severity {
		nArsg := make([]interface{}, 0)
		nArsg = append(nArsg, clientId)
		nArsg = append(nArsg, args...)
		el.Printf(severity, fmt, nArsg...)
	}
}

func (el *errorLogger) Fatalf(fmt string, args ...interface{}) {
	el.logMp[FATAL].Fatalf(fmt, args...)
}

func (el *errorLogger) ClientErrorf(fmt string, clientId string, args ...interface{}) {
	nArsg := make([]interface{}, 0)
	nArsg = append(nArsg, clientId)
	nArsg = append(nArsg, args...)
	el.logMp[ERROR].Printf("[client %s] "+fmt, nArsg...)
}

func (el *errorLogger) Errorf(fmt string, args ...interface{}) {
	el.logMp[ERROR].Printf(fmt, args...)
}

func (el *errorLogger) PrintError(err error) {
	el.logMp[ERROR].Printf("%v", err)
}

func (el *errorLogger) FatalOnError(err error) {
	el.logMp[FATAL].Fatalf("%v", err)
}
