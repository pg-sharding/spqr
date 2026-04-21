package virtual

const (
	VirtualFuncIsReady    = "__spqr__is_ready"
	VirtualShards         = "__spqr__shards"
	VirtualShow           = "__spqr__show"
	VirtualConsoleExecute = "__spqr__console_execute"
	VirtualRemoteExecute  = "__spqr__remote_execute"
	VirtualRouteKey       = "__spqr__route_key"
	VirtualRun2PCRecover  = "__spqr__run_2pc_recover"
	VirtualClear2PCData   = "__spqr__clear_2pc_data"

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
		VirtualRouteKey, VirtualRemoteExecute, VirtualRun2PCRecover,
		VirtualClear2PCData,
		PGIsolationTestSessionIsBlocked:
		return true
	default:
		return false
	}
}
