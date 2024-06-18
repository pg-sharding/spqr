package relay

import "github.com/pg-sharding/spqr/router/parser"

// Execute requered command via
// some protoc-specific logic
type ProtoStateHandler interface {
	ExecCommit(rst RelayStateMgr, query string) error
	ExecRollback(rst RelayStateMgr, query string) error

	ExecSet(rst RelayStateMgr, query, name, value string) error
	ExecSetLocal(rst RelayStateMgr, query, name, value string) error
	ExecReset(rst RelayStateMgr, query, name string) error
	ExecResetMetadata(rst RelayStateMgr, query, setting string) error

	/* Custom logic aroung parsing routines */
	ParseSQL(rst RelayStateMgr, query string) (parser.ParseState, string, error)
}
