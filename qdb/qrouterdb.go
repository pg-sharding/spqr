package qdb

import (
	"context"
)

type QrouterDB interface {
	AddShardingRule(ctx context.Context, rule *ShardingRule) error

	DropShardingRule(ctx context.Context, id string) error
	DropShardingRuleAll(ctx context.Context) ([]*ShardingRule, error)

	ListShardingRules(ctx context.Context) ([]*ShardingRule, error)

	LockKeyRange(ctx context.Context, keyRangeID string) (*KeyRange, error)
	Unlock(ctx context.Context, keyRangeID string) error

	ListKeyRanges(_ context.Context) ([]*KeyRange, error)

	AddKeyRange(ctx context.Context, keyRange *KeyRange) error
	UpdateKeyRange(ctx context.Context, keyRange *KeyRange) error

	DropKeyRange(ctx context.Context, id string) error
	DropKeyRangeAll(ctx context.Context) ([]*KeyRange, error)

	GetKeyRange(ctx context.Context, id string) (*KeyRange, error)

	AddRouter(ctx context.Context, r *Router) error
	DeleteRouter(ctx context.Context, rID string) error
	ListRouters(ctx context.Context) ([]*Router, error)
	LockRouter(ctx context.Context, id string) error

	Check(ctx context.Context, kr *KeyRange) bool
	Watch(krid string, status *KeyRangeStatus, notifyio chan<- interface{}) error

	AddShard(ctx context.Context, shard *Shard) error
	ListShards(ctx context.Context) ([]*Shard, error)
	GetShardInfo(ctx context.Context, shardID string) (*Shard, error)

	CheckLocked(ctx context.Context, KeyRangeID string) (*KeyRange, error)
	Share(key *KeyRange) error
}
