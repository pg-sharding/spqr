package virtual

const (
	VirtualFuncIsReady    = "__spqr__is_ready"
	VirtualShards         = "__spqr__shards"
	VirtualShow           = "__spqr__show"
	VirtualConsoleExecute = "__spqr__console_execute"
	VirtualRemoteExecute  = "__spqr__remote_execute"
	VirtualRouteKey       = "__spqr__route_key"

	VirtualCTID      = "__spqr__ctid"
	VirtualFuncHosts = "__spqr__host_status"

	/* isolation tester support function */
	VirtualAwaitTask                = "__spqr__await_task"
	PGIsolationTestSessionIsBlocked = "pg_isolation_test_session_is_blocked"
)

func IsVirtualFuncName(n string) bool {
	switch n {
	case VirtualFuncIsReady, VirtualShards, VirtualShow, VirtualConsoleExecute,
		VirtualCTID, VirtualFuncHosts, VirtualAwaitTask,
		VirtualRouteKey, VirtualRemoteExecute,
		PGIsolationTestSessionIsBlocked:
		return true
	default:
		return false
	}
}
