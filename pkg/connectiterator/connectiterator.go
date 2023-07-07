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
}
