package spqrlog

import (
	"sync/atomic"
	"time"
)

type StmtType string

const (
	StmtTypeQuery = "QUERY"
	StmtTypeParse = "PARSE"
	StmtTypeBind  = "BIND"
	StmtTypeExec  = "EXECUTE"
)

var SLogger = NewStmtLogger(-1)

type StmtLogger struct {
	logMinDurationStatement atomic.Int64
}

func NewStmtLogger(logMinDurationStatement time.Duration) *StmtLogger {
	logger := &StmtLogger{}
	logger.logMinDurationStatement.Store(int64(logMinDurationStatement))
	return logger
}

func ReloadSLogger(logMinDurationStatement time.Duration) {
	SLogger.logMinDurationStatement.Store(int64(logMinDurationStatement))
}

// TODO unit tests
func (s *StmtLogger) shouldLogStatement(t time.Duration) bool {
	minimum := time.Duration(s.logMinDurationStatement.Load())
	return minimum != -1 && t > minimum
}

func (s *StmtLogger) ReportStatement(typ StmtType, stmt string, t time.Duration) {
	if s.shouldLogStatement(t) {
		Zero.Log().Str("stmt", stmt).Str("stmt_type", string(typ)).Dur("duration", t).Msg("log statement")
	}
}
