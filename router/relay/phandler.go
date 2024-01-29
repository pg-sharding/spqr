package relay

import "github.com/jackc/pgx/v5/pgproto3"

// Execute requered command via
// some protoc-specofoc logic
type ProtoStateHandler interface {
	ExecCommit(rst RelayStateMgr, query string) error
	ExecRollback(rst RelayStateMgr, query string) error

	ExecSet(rst RelayStateMgr, msg pgproto3.FrontendMessage, name, value string) error
	ExecSetLocal(rst RelayStateMgr, msg pgproto3.FrontendMessage) error
	ExecReset(rst RelayStateMgr, msg pgproto3.FrontendMessage) error
	ExecResetMetadata(rst RelayStateMgr, msg pgproto3.FrontendMessage, setting string) error
}
