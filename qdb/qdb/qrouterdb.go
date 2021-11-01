package qdb

import (
	"github.com/pg-sharding/spqr/pkg/models/shrule"
)

type QrouterDB interface {
	Lock(keyRangeID string) (*KeyRange, error)
	UnLock(keyRangeID string) error

	AddKeyRange(keyRange *KeyRange) error
	UpdateKeyRange(keyRange *KeyRange) error

	Begin() error
	Commit() error

	AddRouter(r *Router) error
	Check(kr *KeyRange) bool

	Watch(krid string, status *KeyRangeStatus, notifyio chan<- interface{}) error
	ListRouters() ([]*Router, error)
	DropKeyRange(krl *KeyRange) error
	ListShardingRules() ([]*shrule.ShardingRule, error)
}
