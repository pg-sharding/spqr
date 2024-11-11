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
	Put(host shard.Shard) error
	Discard(sh shard.Shard) error

	UsedConnectionCount() int
	IdleConnectionCount() int
	QueueResidualSize() int

	Hostname() string
	RouterName() string

	Rule() *config.BackendRule
}

/* dedicated host connection pool */
type Pool interface {
	ConnectionKepper
	shard.ShardIterator

	Connection(clid uint, shardKey kr.ShardKey) (shard.Shard, error)
}

type MultiShardPool interface {
	ConnectionKepper
	shard.ShardIterator
	PoolIterator

	Connection(clid uint, shardKey kr.ShardKey, host string) (shard.Shard, error)

	SetRule(rule *config.BackendRule)
}

type PoolIterator interface {
	ForEachPool(cb func(p Pool) error) error
}

type ConnectionAllocFn func(shardKey kr.ShardKey, host string, rule *config.BackendRule) (shard.Shard, error)

type DBPool interface {
	MultiShardPool

	ShardMapping() map[string]*config.Shard

	SetShuffleHosts(bool)
}
