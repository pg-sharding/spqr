package relay

// Execute requered command via
// some protoc-specofoc logic
type ProtoStateHandler interface {
	ExecCommit(rst RelayStateMgr, query string) error
	ExecRollback(rst RelayStateMgr, query string) error

	ExecSet(rst RelayStateMgr, query, name, value string) error
	ExecSetLocal(rst RelayStateMgr, query, name, value string) error
	ExecReset(rst RelayStateMgr, query, name string) error
	ExecResetMetadata(rst RelayStateMgr, query, setting string) error
}
