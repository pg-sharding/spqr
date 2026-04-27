package connmgr

import (
	"github.com/pg-sharding/spqr/pkg/pool"
	"github.com/pg-sharding/spqr/pkg/tsa"
)

type ConnectionStatMgr interface {
	InstanceHealthChecks() map[string]tsa.CachedCheckResult
	TsaCacheEntries() map[pool.TsaKey]pool.CachedEntry

	/*
		user-facing connection stat callbacks.
		TODO: Refactor it, add:
		- handshake counter
		- unexpected eof error counter
	*/
	TotalTCPCount() int64
	ActiveTCPCount() int64
	TotalCancelCount() int64
}

type ConnectionMgr interface {
	ConnectionIterator
	ConnectionStatMgr
}
