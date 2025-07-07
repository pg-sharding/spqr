package connmgr

import (
	"github.com/pg-sharding/spqr/pkg/client"
	"github.com/pg-sharding/spqr/pkg/pool"
	"github.com/pg-sharding/spqr/pkg/shard"
)

type ConnectionIterator interface {
	client.Pool
	shard.ShardHostIterator
	pool.PoolIterator
}
