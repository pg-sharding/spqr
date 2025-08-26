package spqrlog

import "time"

type StmtType string

const (
	StmtTypeQuery = "QUERY"
	StmtTypeParse = "PARSE"
	StmtTypeBind  = "BIND"
	StmtTypeExec  = "EXECUTE"
)

var SLogger *StmtLogger

type StmtLogger struct {
	logMinDurationStatement time.Duration
}

func NewStmtLogger(logMinDurationStatement time.Duration) *StmtLogger {
	return &StmtLogger{
		logMinDurationStatement: logMinDurationStatement,
	}
}

func ReloadSLogger(logMinDurationStatement time.Duration) {
	SLogger = NewStmtLogger(logMinDurationStatement)
}

// TODO unit tests
func (s *StmtLogger) shouldLogStatement(t time.Duration) bool {
	return s.logMinDurationStatement != -1 && t > s.logMinDurationStatement
}

func (s *StmtLogger) ReportStatement(typ StmtType, stmt string, t time.Duration) {
	if s.shouldLogStatement(t) {
		Zero.Info().Str("stmt", stmt).Str("stmt_type", string(typ)).Dur("duration", t).Msg("log statement")
	}
}
