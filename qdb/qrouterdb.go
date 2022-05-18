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
	DropKeyRange(ctx context.Context, krl *KeyRange) error

	AddRouter(ctx context.Context, r *Router) error
	ListRouters(ctx context.Context) ([]*Router, error)

	Check(ctx context.Context, kr *KeyRange) bool
	Watch(krid string, status *KeyRangeStatus, notifyio chan<- interface{}) error

	ListShardingRules(ctx context.Context) ([]*shrule.ShardingRule, error)
	Share(key *KeyRange) error
}
