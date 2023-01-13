package qdb

import (
	"context"
)

<<<<<<< Updated upstream:qdb/qrouterdb.go
type QrouterDB interface {
=======
type QDB interface {
>>>>>>> Stashed changes:qdb/qdb.go
	AddShardingRule(ctx context.Context, rule *ShardingRule) error
	DropShardingRule(ctx context.Context, id string) error
	DropShardingRuleAll(ctx context.Context) error
	GetShardingRule(ctx context.Context, id string) (*ShardingRule, error)
	ListShardingRules(ctx context.Context) ([]*ShardingRule, error)

<<<<<<< Updated upstream:qdb/qrouterdb.go
	LockKeyRange(ctx context.Context, keyRangeID string) (*KeyRange, error)
	Unlock(ctx context.Context, keyRangeID string) error

	ListKeyRanges(_ context.Context) ([]*KeyRange, error)

=======
>>>>>>> Stashed changes:qdb/qdb.go
	AddKeyRange(ctx context.Context, keyRange *KeyRange) error
	GetKeyRange(ctx context.Context, id string) (*KeyRange, error)
	UpdateKeyRange(ctx context.Context, keyRange *KeyRange) error
	DropKeyRange(ctx context.Context, id string) error
	DropKeyRangeAll(ctx context.Context) error
	ListKeyRanges(_ context.Context) ([]*KeyRange, error)
	LockKeyRange(ctx context.Context, id string) (*KeyRange, error)
	UnlockKeyRange(ctx context.Context, id string) error
	CheckLockedKeyRange(ctx context.Context, id string) (*KeyRange, error)
	ShareKeyRange(id string) error

	AddRouter(ctx context.Context, r *Router) error
	DeleteRouter(ctx context.Context, rID string) error
	ListRouters(ctx context.Context) ([]*Router, error)
	LockRouter(ctx context.Context, id string) error

<<<<<<< Updated upstream:qdb/qrouterdb.go
	Check(ctx context.Context, kr *KeyRange) bool
	Watch(krid string, status *KeyRangeStatus, notifyio chan<- interface{}) error

=======
>>>>>>> Stashed changes:qdb/qdb.go
	AddShard(ctx context.Context, shard *Shard) error
	ListShards(ctx context.Context) ([]*Shard, error)
	GetShard(ctx context.Context, shardID string) (*Shard, error)

<<<<<<< Updated upstream:qdb/qrouterdb.go
	CheckLocked(ctx context.Context, KeyRangeID string) (*KeyRange, error)
	Share(key *KeyRange) error
=======
	AddDataspace(ctx context.Context, ks *Dataspace) error
	ListDataspaces(ctx context.Context) ([]*Dataspace, error)
>>>>>>> Stashed changes:qdb/qdb.go
}
