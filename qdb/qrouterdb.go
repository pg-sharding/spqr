package qdb

import (
	"context"
)

type QrouterDB interface {
	/*
	* sharding rules
	 */
	// AddShardingRule persists sharding rule to qdb
	AddShardingRule(ctx context.Context, rule *ShardingRule) error

	DropShardingRule(ctx context.Context, id string) error
	DropShardingRuleAll(ctx context.Context) ([]*ShardingRule, error)

	ListShardingRules(ctx context.Context) ([]*ShardingRule, error)

	/* key ranges */
	LockKeyRange(ctx context.Context, keyRangeID string) (*KeyRange, error)
	Unlock(ctx context.Context, keyRangeID string) error

	ListKeyRanges(_ context.Context) ([]*KeyRange, error)

	AddKeyRange(ctx context.Context, keyRange *KeyRange) error
	UpdateKeyRange(ctx context.Context, keyRange *KeyRange) error

	DropKeyRange(ctx context.Context, id string) error
	DropKeyRangeAll(ctx context.Context) ([]*KeyRange, error)

	GetKeyRange(ctx context.Context, id string) (*KeyRange, error)

	CheckLocked(ctx context.Context, KeyRangeID string) (*KeyRange, error)
	Share(key *KeyRange) error

	/* routers (coordinator info) */
	AddRouter(ctx context.Context, r *Router) error
	DeleteRouter(ctx context.Context, rID string) error
	ListRouters(ctx context.Context) ([]*Router, error)
	LockRouter(ctx context.Context, id string) error

	/* TODO: probably drop those */
	Check(ctx context.Context, kr *KeyRange) bool
	Watch(krid string, status *KeyRangeStatus, notifyio chan<- interface{}) error

	/* shards */
	AddShard(ctx context.Context, shard *Shard) error
	ListShards(ctx context.Context) ([]*Shard, error)
	GetShardInfo(ctx context.Context, shardID string) (*Shard, error)

	/* keyspaces */
	AddKeyspace(ctx context.Context, ks *Keyspace) error
	ListKeyspace(ctx context.Context) ([]*Keyspace, error)
}
