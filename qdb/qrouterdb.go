package qdb

import (
	"context"
)

type QrouterDB interface {
	Lock(ctx context.Context, keyRangeID string) (*KeyRange, error)
	UnLock(ctx context.Context, keyRangeID string) error

	AddKeyRange(ctx context.Context, keyRange *KeyRange) error
	UpdateKeyRange(ctx context.Context, keyRange *KeyRange) error
	DropKeyRange(ctx context.Context, krl *KeyRange) error

	AddRouter(ctx context.Context, r *Router) error
	DeleteRouter(ctx context.Context, rID string) error
	ListRouters(ctx context.Context) ([]*Router, error)

	Check(ctx context.Context, kr *KeyRange) bool

	Watch(krid string, status *KeyRangeStatus, notifyio chan<- interface{}) error

	AddShardingRule(ctx context.Context, shRules *ShardingRule) error
	ListShardingRules(ctx context.Context) ([]*ShardingRule, error)

	ListKeyRanges(ctx context.Context) ([]*KeyRange, error)
}
