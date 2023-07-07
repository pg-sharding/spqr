package pool

import (
	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/pkg/shard"
)

const defaultInstanceConnectionLimit = 50

type Pool interface {
	shard.ShardIterator

	Connection(clid string, shardKey kr.ShardKey, host string) (shard.Shard, error)

	Put(host shard.Shard) error
	Discard(sh shard.Shard) error

	List() []shard.Shard
}

type MultiShardPool interface {
	shard.ShardIterator
	PoolInterator
	Pool

	InitRule(rule *config.BackendRule) error
	Cut(host string) []shard.Shard
}

type PoolInterator interface {
	ForEachPool(cb func(p Pool) error) error
}

type ConnectionAllocFn func(shardKey kr.ShardKey, host string, rule *config.BackendRule) (shard.Shard, error)

type DBPool interface {
	shard.ShardIterator
	PoolInterator
	MultiShardPool

	ShardMapping() map[string]*config.Shard
}
