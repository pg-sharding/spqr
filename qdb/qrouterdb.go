package qdb

import (
	"context"

	"github.com/pg-sharding/spqr/pkg/models/shrule"
)

type QrouterDB interface {
	Lock(ctx context.Context, keyRangeID string) (*KeyRange, error)
	UnLock(ctx context.Context, keyRangeID string) error

	AddKeyRange(ctx context.Context, keyRange *KeyRange) error
	UpdateKeyRange(ctx context.Context, keyRange *KeyRange) error

	AddRouter(ctx context.Context, r *Router) error
	Check(ctx context.Context, kr *KeyRange) bool

	Watch(krid string, status *KeyRangeStatus, notifyio chan<- interface{}) error
	ListRouters(ctx context.Context) ([]*Router, error)
	DropKeyRange(ctx context.Context, krl *KeyRange) error
	ListShardingRules(ctx context.Context) ([]*shrule.ShardingRule, error)
	ListKeyRanges(ctx context.Context) ([]*KeyRange, error)
}
