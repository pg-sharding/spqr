package qdb

import (
	"context"
	"fmt"

	"github.com/pg-sharding/spqr/pkg/config"
)

type QDB interface {
	AddShardingRule(ctx context.Context, rule *ShardingRule) error
	DropShardingRule(ctx context.Context, id string) error
	DropShardingRuleAll(ctx context.Context) ([]*ShardingRule, error)
	GetShardingRule(ctx context.Context, id string) (*ShardingRule, error)
	ListShardingRules(ctx context.Context) ([]*ShardingRule, error)

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
	OpenRouter(ctx context.Context, rID string) error
	DeleteRouter(ctx context.Context, rID string) error
	ListRouters(ctx context.Context) ([]*Router, error)
	LockRouter(ctx context.Context, id string) error

	AddShard(ctx context.Context, shard *Shard) error
	ListShards(ctx context.Context) ([]*Shard, error)
	GetShard(ctx context.Context, shardID string) (*Shard, error)

	MatchShardingRules(ctx context.Context, m func(shrules map[string]*ShardingRule) error) error

	AddDataspace(ctx context.Context, ks *Dataspace) error
	ListDataspaces(ctx context.Context) ([]*Dataspace, error)

	RecordTransferTx(ctx context.Context, key string, info *DataTransferTransaction) error
	GetTransferTx(ctx context.Context, key string) (*DataTransferTransaction, error)
	RemoveTransferTx(ctx context.Context, key string) error
}

func NewQDB(qdbType string) (QDB, error) {
	switch qdbType {
	case "etcd":
		return NewEtcdQDB(config.CoordinatorConfig().QdbAddr)
	case "mem":
		return NewMemQDB("")
	default:
		return nil, fmt.Errorf("qdb implementation %s is invalid", qdbType)
	}
}

type TxStatus string

const (
	Commited   = TxStatus("commit")
	Processing = TxStatus("process")
)

type DataTransferTransaction struct {
	ToShardId   string   `json:"to_shard"`
	FromShardId string   `json:"from_shard"`
	FromTxName  string   `json:"from_transaction"`
	ToTxName    string   `json:"to_transaction"`
	FromStatus  TxStatus `json:"from_tx_status"`
	ToStatus    TxStatus `json:"to_tx_status"`
}
