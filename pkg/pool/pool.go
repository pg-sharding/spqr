package pool

import (
	"time"

	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/pkg/shard"
	"github.com/pg-sharding/spqr/pkg/tsa"
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
	shard.ShardHostIterator

	Connection(clid uint, shardKey kr.ShardKey) (shard.ShardHostInstance, error)
}

/* Host  */
type MultiShardPool interface {
	ConnectionKepper
	shard.ShardHostIterator
	PoolIterator

	ID() uint

	ConnectionHost(clid uint, shardKey kr.ShardKey, host config.Host) (shard.ShardHostInstance, error)

	SetRule(rule *config.BackendRule)
}

type MultiShardTSAPool interface {
	MultiShardPool

	ShardMapping() map[string]*config.Shard
	ConnectionWithTSA(clid uint, key kr.ShardKey, targetSessionAttrs tsa.TSA) (shard.ShardHostInstance, error)
	InstanceHealhChecks() map[config.Host]tsa.TimedCheckResult
}

type PoolIterator interface {
	ForEachPool(cb func(p Pool) error) error
}

type ConnectionAllocFn func(shardKey kr.ShardKey, host config.Host, rule *config.BackendRule) (shard.ShardHostInstance, error)
