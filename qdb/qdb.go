package qdb

import (
	"context"
	"fmt"

	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/router/rfqn"
)

type ShardingSchemaKeeper interface {
	// RecordKeyRangeMove persists start of key range movement in distributed storage
	RecordKeyRangeMove(ctx context.Context, m *MoveKeyRange) error
	// ListKeyRangeMoves lists all key-range moves that are in progress
	ListKeyRangeMoves(ctx context.Context) ([]*MoveKeyRange, error)
	// UpdateKeyRangeMoveStatus marks the key range move as complete
	UpdateKeyRangeMoveStatus(ctx context.Context, moveId string, s MoveKeyRangeStatus) error
	// DeleteKeyRangeMove removes information about key range moves
	DeleteKeyRangeMove(ctx context.Context, moveId string) error
}

type TopologyKeeper interface {
	// AddRouter adds a new router to the cluster
	AddRouter(ctx context.Context, r *Router) error
	// DeleteRouter removes the router from the cluster
	DeleteRouter(ctx context.Context, rID string) error
	//ListRouters lists the routers of the cluster
	ListRouters(ctx context.Context) ([]*Router, error)
	// OpenRouter changes the state of the router to online, making it usable for query execution.
	OpenRouter(ctx context.Context, rID string) error
	// CloseRouter changes the state of the router to offline, making it unavailable for query execution.
	CloseRouter(ctx context.Context, rID string) error
}

// Keep track of the status of the two-phase data move transaction.
type DistributedXactKepper interface {
	RecordTransferTx(ctx context.Context, key string, info *DataTransferTransaction) error
	GetTransferTx(ctx context.Context, key string) (*DataTransferTransaction, error)
	RemoveTransferTx(ctx context.Context, key string) error
}

// QDB is a generic interface used by both the coordinator and the router.
// The router uses a memory-based version of this interface to cache routing schema state
// while the coordinator uses etcd-based implementation to synchronize distributed state.
type QDB interface {
	// Key ranges
	CreateKeyRange(ctx context.Context, keyRange *KeyRange) error
	GetKeyRange(ctx context.Context, id string) (*KeyRange, error)
	UpdateKeyRange(ctx context.Context, keyRange *KeyRange) error
	DropKeyRange(ctx context.Context, id string) error
	DropKeyRangeAll(ctx context.Context) error
	ListKeyRanges(_ context.Context, distribution string) ([]*KeyRange, error)
	ListAllKeyRanges(_ context.Context) ([]*KeyRange, error)
	LockKeyRange(ctx context.Context, id string) (*KeyRange, error)
	UnlockKeyRange(ctx context.Context, id string) error
	CheckLockedKeyRange(ctx context.Context, id string) (*KeyRange, error)
	ShareKeyRange(id string) error
	RenameKeyRange(ctx context.Context, krId, ktIdNew string) error

	// Shards
	AddShard(ctx context.Context, shard *Shard) error
	ListShards(ctx context.Context) ([]*Shard, error)
	GetShard(ctx context.Context, shardID string) (*Shard, error)
	DropShard(ctx context.Context, shardID string) error

	// Distribution management
	CreateDistribution(ctx context.Context, distr *Distribution) error
	ListDistributions(ctx context.Context) ([]*Distribution, error)
	DropDistribution(ctx context.Context, id string) error
	GetDistribution(ctx context.Context, id string) (*Distribution, error)
	// TODO: fix this by passing FQRN (fully qualified relation name (+schema))
	GetRelationDistribution(ctx context.Context, relation *rfqn.RelationFQN) (*Distribution, error)

	// Reference relations
	CreateReferenceRelation(ctx context.Context, r *ReferenceRelation) error
	GetReferenceRelation(ctx context.Context, relName *rfqn.RelationFQN) (*ReferenceRelation, error)
	AlterReferenceRelationStorage(ctx context.Context, relName *rfqn.RelationFQN, shs []string) error
	ListReferenceRelations(ctx context.Context) ([]*ReferenceRelation, error)
	DropReferenceRelation(ctx context.Context, relName *rfqn.RelationFQN) error

	// Update distribution
	AlterDistributionAttach(ctx context.Context, id string, rels []*DistributedRelation) error
	AlterDistributionDetach(ctx context.Context, id string, relName *rfqn.RelationFQN) error
	AlterDistributedRelation(ctx context.Context, id string, rel *DistributedRelation) error

	// Task group
	GetMoveTaskGroup(ctx context.Context) (*MoveTaskGroup, error)
	WriteMoveTaskGroup(ctx context.Context, group *MoveTaskGroup) error
	UpdateMoveTaskGroupSetCurrentTask(ctx context.Context, taskIndex int) error
	GetCurrentMoveTaskIndex(ctx context.Context) (int, error)
	RemoveMoveTaskGroup(ctx context.Context) error

	// MOVE tasks
	CreateMoveTask(ctx context.Context, task *MoveTask) error
	GetMoveTask(ctx context.Context, id string) (*MoveTask, error)
	UpdateMoveTask(ctx context.Context, task *MoveTask) error
	RemoveMoveTask(ctx context.Context, id string) error

	// Redistribute tasks
	GetRedistributeTask(ctx context.Context) (*RedistributeTask, error)
	WriteRedistributeTask(ctx context.Context, task *RedistributeTask) error
	RemoveRedistributeTask(ctx context.Context) error

	// Balancer interaction
	GetBalancerTask(ctx context.Context) (*BalancerTask, error)
	WriteBalancerTask(ctx context.Context, task *BalancerTask) error
	RemoveBalancerTask(ctx context.Context) error

	// Coordinator interaction
	UpdateCoordinator(ctx context.Context, address string) error
	GetCoordinator(ctx context.Context) (string, error)
	ListRouters(ctx context.Context) ([]*Router, error)

	// Sequences for reference relation
	CreateSequence(ctx context.Context, seqName string, initialValue int64) error
	ListSequences(ctx context.Context) ([]string, error)
	AlterSequenceAttach(ctx context.Context, seqName string, relName *rfqn.RelationFQN, colName string) error
	GetRelationSequence(ctx context.Context, relName *rfqn.RelationFQN) (map[string]string, error)
	NextVal(ctx context.Context, seqName string) (int64, error)
	CurrVal(ctx context.Context, seqName string) (int64, error)
	DropSequence(ctx context.Context, seqName string, force bool) error
}

// XQDB means extended QDB
// The coordinator should use an etcd-based implementation to keep the distributed state in sync.
type XQDB interface {
	// routing schema
	QDB
	// router topology
	TopologyKeeper
	// data move state
	ShardingSchemaKeeper
	DistributedXactKepper

	TryCoordinatorLock(ctx context.Context, addr string) error
}

func NewXQDB(qdbType string) (XQDB, error) {
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
	Planned    = TxStatus("planned")
	DataCopied = TxStatus("data_copied")
)

// DataTransferTransaction contains information about data transfer
// from one shard to another
type DataTransferTransaction struct {
	ToShardId   string   `json:"to_shard"`
	FromShardId string   `json:"from_shard"`
	Status      TxStatus `json:"status"`
}
