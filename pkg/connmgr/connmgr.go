package connmgr

import (
	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/tsa"
)

type ConnectionStatsMgr interface {
	ConnectionIterator

	InstanceHealthChecks() map[config.Host]tsa.TimedCheckResult

	/*
		user-facing connection stat callbacks.
		TODO: Refactor it, add:
		- handshake counter
		- connect start time
		- unexpected eof error counter
	*/
	TotalTcpCount() int64
	ActiveTcpCount() int64
	TotalCancelCount() int64
}
