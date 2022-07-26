package qdb

import (
	"context"

	"github.com/pg-sharding/spqr/pkg/models/shrule"
)

type QrouterDB interface {
	shrule.ShardingRulesMgr

	AddKeyRange(ctx context.Context, keyRange *KeyRange) error
	Lock(ctx context.Context, krid string) (*KeyRange, error)
	Unlock(ctx context.Context, krid string) error

	ListKeyRanges(_ context.Context) ([]*KeyRange, error)

	UpdateKeyRange(ctx context.Context, keyRange *KeyRange) error
	DropKeyRange(ctx context.Context, id string) error
	GetKeyRange(ctx context.Context, id string) (*KeyRange, error)

	AddRouter(ctx context.Context, r *Router) error
	DeleteRouter(ctx context.Context, rID string) error
	ListRouters(ctx context.Context) ([]*Router, error)

	Check(ctx context.Context, kr *KeyRange) bool
	Watch(krid string, status *KeyRangeStatus, notifyio chan<- interface{}) error

	AddShard(ctx context.Context, shard *Shard) error
	ListShards(ctx context.Context) ([]*Shard, error)
	GetShardInfo(ctx context.Context, shardID string) (*ShardInfo, error)

	CheckLocked(ctx context.Context, KeyRangeID string) (*KeyRange, error)
	Share(key *KeyRange) error
	DropKeyRangeAll(ctx context.Context) ([]*KeyRange, error)
}
