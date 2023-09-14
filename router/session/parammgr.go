package session

import "github.com/jackc/pgx/v5/pgproto3"

type ParamMgr interface {
	SetParam(string, string)
	ResetParam(string)
	ResetAll()
	ConstructClientParams() *pgproto3.Query
	Params() map[string]string

	StartTx()
	CommitActiveSet()
	Savepoint(string)
	Rollback()
	RollbackToSP(string)
}
