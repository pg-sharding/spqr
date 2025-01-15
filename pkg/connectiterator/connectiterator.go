package connectiterator

import (
	"github.com/pg-sharding/spqr/pkg/client"
	"github.com/pg-sharding/spqr/pkg/pool"
	"github.com/pg-sharding/spqr/pkg/shard"
)

type ConnectIterator interface {
	client.Pool
	shard.ShardIterator
	pool.PoolIterator

	/* user-facing connection stat callbacks */
	TotalTcpCount() int64
	ActiveTcpCount() int64
	TotalCancelCount() int64
}
