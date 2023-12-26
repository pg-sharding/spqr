package qdb

import (
	"context"
	"fmt"
	"github.com/pg-sharding/spqr/pkg/spqrlog"

	"github.com/pg-sharding/spqr/pkg/config"
)

type ShardingSchemaKeeper interface {
	/* persist start of key range move in distributed storage */
	RecordKeyRangeMove(ctx context.Context, m *MoveKeyRange) error
	/* list all key-range moves in progress */
	ListKeyRangeMoves(ctx context.Context) ([]*MoveKeyRange, error)
	/* mark key range move as completed */
	UpdateKeyRangeMoveStatus(ctx context.Context, moveId string, s MoveKeyRangeStatus) error
}

type TopolodyKeeper interface {
	AddRouter(ctx context.Context, r *Router) error
	DeleteRouter(ctx context.Context, rID string) error
	ListRouters(ctx context.Context) ([]*Router, error)

	// OpenRouter: change state of router to online
	// Making it usable to use for query executiong.
	// "Online" mode.
	OpenRouter(ctx context.Context, rID string) error

	// CloseRouter: change state of router to offline
	// Making it unusable to use for query executiong.
	// "Offline" mode.
	CloseRouter(ctx context.Context, rID string) error
}

// Keep track of the status of the two-phase data move transaction.
type DistributedXactKepper interface {
	RecordTransferTx(ctx context.Context, key string, info *DataTransferTransaction) error
	GetTransferTx(ctx context.Context, key string) (*DataTransferTransaction, error)
	RemoveTransferTx(ctx context.Context, key string) error
}

/* This is a generic interface to be used by both the coordinator and the router.
* The router should use a memory-based version of this interface to cache
* the state of the routing schema, while the coordinator should use an etcd-based
* implementation to keep the distributed state in sync.
 */
type QDB interface {
	AddShardingRule(ctx context.Context, rule *ShardingRule) error
	DropShardingRule(ctx context.Context, id string) error
	DropShardingRuleAll(ctx context.Context) ([]*ShardingRule, error)
	GetShardingRule(ctx context.Context, id string) (*ShardingRule, error)
	ListShardingRules(ctx context.Context, dataspace string) ([]*ShardingRule, error)
	ListAllShardingRules(ctx context.Context) ([]*ShardingRule, error)

	AddKeyRange(ctx context.Context, keyRange *KeyRange) error
	GetKeyRange(ctx context.Context, id string) (*KeyRange, error)
	UpdateKeyRange(ctx context.Context, keyRange *KeyRange) error
	DropKeyRange(ctx context.Context, id string) error
	DropKeyRangeAll(ctx context.Context) error
	ListKeyRanges(_ context.Context, datspace string) ([]*KeyRange, error)
	ListAllKeyRanges(_ context.Context) ([]*KeyRange, error)
	LockKeyRange(ctx context.Context, id string) (*KeyRange, error)
	UnlockKeyRange(ctx context.Context, id string) error
	CheckLockedKeyRange(ctx context.Context, id string) (*KeyRange, error)
	ShareKeyRange(id string) error

	AddShard(ctx context.Context, shard *Shard) error
	ListShards(ctx context.Context) ([]*Shard, error)
	GetShard(ctx context.Context, shardID string) (*Shard, error)

	MatchShardingRules(ctx context.Context, m func(shrules map[string]*ShardingRule) error) error

	AddDataspace(ctx context.Context, ks *Dataspace) error
	ListDataspaces(ctx context.Context) ([]*Dataspace, error)
	DropDataspace(ctx context.Context, id string) error

	UpdateCoordinator(ctx context.Context, address string) error
	GetCoordinator(ctx context.Context) (string, error)
}

// Extended QDB
// The coordinator should use an etcd-based implementation to keep the distributed state in sync.
type XQDB interface {
	// routing schema
	QDB
	// router topology
	TopolodyKeeper
	// data move state
	ShardingSchemaKeeper
	DistributedXactKepper

	TryCoordinatorLock(ctx context.Context) error
}

func NewXQDB(qdbType string) (XQDB, error) {
	switch qdbType {
	case "etcd":
		shard, err := config.LoadShardDataCfg(config.CoordinatorConfig().ShardDataCfg)
		if err != nil {
			spqrlog.Zero.Error().Err(err).Msg("error loading config")
		}
		etcd, err := NewEtcdQDB(config.CoordinatorConfig().QdbAddr)
		sh := make(map[string]Shard)
		for id, data := range shard.ShardsData {
			sh[id] = *&Shard{ID: id, Hosts: data.Hosts}
		}
		etcd.SetShards(sh)
		return etcd, err
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
