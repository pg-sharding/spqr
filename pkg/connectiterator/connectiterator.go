package connectiterator

import (
	"github.com/pg-sharding/spqr/pkg/client"
	"github.com/pg-sharding/spqr/pkg/pool"
	"github.com/pg-sharding/spqr/pkg/shard"
)

type ConnectIterator interface {
	client.Pool
	shard.ShardHostIterator
	pool.PoolIterator

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
