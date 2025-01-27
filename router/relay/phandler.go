package relay

import "github.com/pg-sharding/spqr/router/parser"

// Execute requered command via
// some protoc-specific logic
type ProtoStateHandler interface {
	ExecBegin(rst RelayStateMgr, query string, st *parser.ParseStateTXBegin) error
	ExecCommit(rst RelayStateMgr, query string) error
	ExecRollback(rst RelayStateMgr, query string) error

	ExecSet(rst RelayStateMgr, query, name, value string) error
	ExecSetLocal(rst RelayStateMgr, query, name, value string) error
	ExecReset(rst RelayStateMgr, query, name string) error
	ExecResetMetadata(rst RelayStateMgr, query, setting string) error
}
