package pool

import (
	"time"

	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/pkg/shard"
)

const (
	defaultInstanceConnectionLimit   = 50
	defaultInstanceConnectionRetries = 10
	defaultInstanceConnectionTimeout = time.Second
	defaultKeepAlive                 = time.Second
	defaultTcpUserTimeout            = time.Millisecond * 9500
)

type ConnectionKepper interface {
	Put(host shard.ShardHostInstance) error
	Discard(sh shard.ShardHostInstance) error
	View() Statistics
}

type Statistics struct {
	DB                string
	Usr               string
	Hostname          string
	RouterName        string
	UsedConnections   int
	IdleConnections   int
	QueueResidualSize int
}

/* dedicated host connection pool */
type Pool interface {
	ConnectionKepper
	shard.ShardIterator

	Connection(clid uint, shardKey kr.ShardKey) (shard.ShardHostInstance, error)
}

/* Host  */
type MultiShardPool interface {
	ConnectionKepper
	shard.ShardIterator
	PoolIterator

	ID() uint

	ConnectionHost(clid uint, shardKey kr.ShardKey, host config.Host) (shard.ShardHostInstance, error)

	SetRule(rule *config.BackendRule)
}

type PoolIterator interface {
	ForEachPool(cb func(p Pool) error) error
}

type ConnectionAllocFn func(shardKey kr.ShardKey, host config.Host, rule *config.BackendRule) (shard.ShardHostInstance, error)
