package meta

import (
	"context"
	"fmt"

	"github.com/pg-sharding/spqr/pkg/models/distributions"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/pkg/models/rrelation"
	"github.com/pg-sharding/spqr/pkg/models/spqrerror"
	"github.com/pg-sharding/spqr/pkg/models/tasks"
	"github.com/pg-sharding/spqr/pkg/models/topology"
	meta_transaction "github.com/pg-sharding/spqr/pkg/models/transaction"
	"github.com/pg-sharding/spqr/qdb"
	"github.com/pg-sharding/spqr/router/cache"
	"github.com/pg-sharding/spqr/router/rfqn"
)

// Wrapper of EntityManager. Keeps track of changes made during a transaction that have not yet been committed.
// It's NO NOT THREAD-SAFE because process in single console thread.
// WARNING: ALL implemented methods MUST have 100% test coverage
type TranEntityManager struct {
	mngr          EntityMgr
	distributions MetaEntityList[*distributions.Distribution]
	keyRanges     MetaEntityList[*kr.KeyRange]
}

var _ EntityMgr = &TranEntityManager{}

// NewTranEntityManager creates a new instance of the TranEntityManager struct.
//
// Parameters:
// - mngr (EntityMgr): Underlying entity manager that performs metadata operations
//
// Returns:
// - a pointer to an TranEntityManager object.
func NewTranEntityManager(mngr EntityMgr) *TranEntityManager {
	keyRangesList := NewMetaEntityList[*kr.KeyRange]()
	return &TranEntityManager{
		mngr:          mngr,
		distributions: *distrList,
		keyRanges:     *keyRangesList,
	}
}

// AddDataShard implements [EntityMgr].
func (t *TranEntityManager) AddDataShard(ctx context.Context, shard *topology.DataShard) error {
	panic("AddDataShard unimplemented")
}

// AddWorldShard implements [EntityMgr].
func (t *TranEntityManager) AddWorldShard(ctx context.Context, shard *topology.DataShard) error {
	panic("AddWorldShard unimplemented")
}

// AlterDistributedRelation implements [EntityMgr].
func (t *TranEntityManager) AlterDistributedRelation(ctx context.Context, id string, rel *distributions.DistributedRelation) error {
	panic("AlterDistributedRelation unimplemented")
}

// AlterDistributedRelationDistributionKey implements [EntityMgr].
func (t *TranEntityManager) AlterDistributedRelationDistributionKey(ctx context.Context, id string, relName string, distributionKey []distributions.DistributionKeyEntry) error {
	panic("AlterDistributedRelationDistributionKey unimplemented")
}

// AlterDistributedRelationSchema implements [EntityMgr].
func (t *TranEntityManager) AlterDistributedRelationSchema(ctx context.Context, id string, relName string, schemaName string) error {
	panic("AlterDistributedRelationSchema unimplemented")
}

// AlterDistributionAttach implements [EntityMgr].
func (t *TranEntityManager) AlterDistributionAttach(ctx context.Context, id string, rels []*distributions.DistributedRelation) error {
	panic("AlterDistributionAttach unimplemented")
}

// AlterDistributionDetach implements [EntityMgr].
func (t *TranEntityManager) AlterDistributionDetach(ctx context.Context, id string, relName *rfqn.RelationFQN) error {
	panic("AlterDistributionDetach unimplemented")
}

// AlterReferenceRelationStorage implements [EntityMgr].
func (t *TranEntityManager) AlterReferenceRelationStorage(ctx context.Context, relName *rfqn.RelationFQN, shs []string) error {
	panic("AlterReferenceRelationStorage unimplemented")
}

// BatchMoveKeyRange implements [EntityMgr].
func (t *TranEntityManager) BatchMoveKeyRange(ctx context.Context, req *kr.BatchMoveKeyRange) error {
	panic("BatchMoveKeyRange unimplemented")
}

// BeginTran implements [EntityMgr].
func (t *TranEntityManager) BeginTran(ctx context.Context) (*meta_transaction.MetaTransaction, error) {
	return t.mngr.BeginTran(ctx)
}

// Cache implements [EntityMgr].
func (t *TranEntityManager) Cache() *cache.SchemaCache {
	panic("Cache unimplemented")
}

// CommitTran implements [EntityMgr].
func (t *TranEntityManager) CommitTran(ctx context.Context, transaction *meta_transaction.MetaTransaction) error {
	return t.mngr.CommitTran(ctx, transaction)
}

// CreateDistribution implements [EntityMgr].
func (t *TranEntityManager) CreateDistribution(ctx context.Context, ds *distributions.Distribution) (*meta_transaction.MetaTransactionChunk, error) {
	if chunk, err := t.mngr.CreateDistribution(ctx, ds); err != nil {
		return nil, err
	} else {
		t.distributions.Save(ds.Id, ds)
		return chunk, nil
	}
}

// CreateKeyRange implements [EntityMgr].
func (t *TranEntityManager) CreateKeyRange(ctx context.Context, kr *kr.KeyRange) error {
	chunk, err := t.mngr.CreateKeyRange(ctx, kr)
	if err != nil {
		return nil, err
	}
	if _, ok := t.keyRanges.Items()[kr.ID]; ok {
		return nil, fmt.Errorf("key range %s already present in qdb", kr.ID)
	}
	t.keyRanges.Save(kr.ID, kr)
	return chunk, nil
}

// CreateReferenceRelation implements [EntityMgr].
func (t *TranEntityManager) CreateReferenceRelation(ctx context.Context, r *rrelation.ReferenceRelation, e []*rrelation.AutoIncrementEntry) error {
	panic("CreateReferenceRelation unimplemented")
}

// CurrVal implements [EntityMgr].
func (t *TranEntityManager) CurrVal(ctx context.Context, seqName string) (int64, error) {
	panic("CurrVal unimplemented")
}

// DCStateKeeper implements [EntityMgr].
func (t *TranEntityManager) DCStateKeeper() qdb.DCStateKeeper {
	panic("DCStateKeeper unimplemented")
}

// DropDistribution implements [EntityMgr].
func (t *TranEntityManager) DropDistribution(ctx context.Context, id string) error {
	t.distributions.Delete(id)
	return nil
}

// DropKeyRange implements [EntityMgr].
func (t *TranEntityManager) DropKeyRange(ctx context.Context, krid string) error {
	panic("DropKeyRange unimplemented")
}

// DropKeyRangeAll implements [EntityMgr].
func (t *TranEntityManager) DropKeyRangeAll(ctx context.Context) error {
	panic("DropKeyRangeAll unimplemented")
}

// DropReferenceRelation implements [EntityMgr].
func (t *TranEntityManager) DropReferenceRelation(ctx context.Context, relName *rfqn.RelationFQN) error {
	panic("DropReferenceRelation unimplemented")
}

// DropSequence implements [EntityMgr].
func (t *TranEntityManager) DropSequence(ctx context.Context, name string, force bool) error {
	panic("DropSequence unimplemented")
}

// DropShard implements [EntityMgr].
func (t *TranEntityManager) DropShard(ctx context.Context, id string) error {
	panic("DropShard unimplemented")
}

// ExecNoTran implements [EntityMgr].
func (t *TranEntityManager) ExecNoTran(ctx context.Context, chunk *meta_transaction.MetaTransactionChunk) error {
	return t.mngr.ExecNoTran(ctx, chunk)
}

// GetBalancerTask implements [EntityMgr].
func (t *TranEntityManager) GetBalancerTask(ctx context.Context) (*tasks.BalancerTask, error) {
	panic("GetBalancerTask unimplemented")
}

// GetCoordinator implements [EntityMgr].
func (t *TranEntityManager) GetCoordinator(ctx context.Context) (string, error) {
	panic("GetCoordinator unimplemented")
}

// GetDistribution implements [EntityMgr].
func (t *TranEntityManager) GetDistribution(ctx context.Context, id string) (*distributions.Distribution, error) {
	if _, ok := t.distributions.DeletedItems()[id]; ok {
		return nil, spqrerror.Newf(spqrerror.SPQR_OBJECT_NOT_EXIST, "distribution \"%s\" not found", id)
	}
	if savedDs, ok := t.distributions.Items()[id]; ok {
		return savedDs, nil
	}

	if distribution, err := t.mngr.GetDistribution(ctx, id); err != nil {
		return nil, err
	} else {
		return distribution, nil
	}
}

// GetKeyRange implements [EntityMgr].
func (t *TranEntityManager) GetKeyRange(ctx context.Context, krId string) (*kr.KeyRange, error) {
	if _, ok := t.keyRanges.DeletedItems()[krId]; ok {
		return nil, spqrerror.Newf(spqrerror.SPQR_OBJECT_NOT_EXIST, "key range \"%s\" not found", krId)
	}
	if savedKr, ok := t.keyRanges.Items()[krId]; ok {
		return savedKr, nil
	}
	if keyRangeFromQdb, err := t.mngr.GetKeyRange(ctx, krId); err != nil {
		return nil, err
	} else {
		return keyRangeFromQdb, nil
	}
}

// GetMoveTaskGroup implements [EntityMgr].
func (t *TranEntityManager) GetMoveTaskGroup(ctx context.Context, id string) (*tasks.MoveTaskGroup, error) {
	panic("GetMoveTaskGroup unimplemented")
}

// GetReferenceRelation implements [EntityMgr].
func (t *TranEntityManager) GetReferenceRelation(ctx context.Context, relName *rfqn.RelationFQN) (*rrelation.ReferenceRelation, error) {
	panic("GetReferenceRelation unimplemented")
}

// GetRelationDistribution implements [EntityMgr].
func (t *TranEntityManager) GetRelationDistribution(ctx context.Context, relation_name *rfqn.RelationFQN) (*distributions.Distribution, error) {
	panic("GetRelationDistribution unimplemented")
}

// GetSequenceRelations implements [EntityMgr].
func (t *TranEntityManager) GetSequenceRelations(ctx context.Context, seqName string) ([]*rfqn.RelationFQN, error) {
	panic("GetSequenceRelations unimplemented")
}

// GetShard implements [EntityMgr].
func (t *TranEntityManager) GetShard(ctx context.Context, shardID string) (*topology.DataShard, error) {
	// TODO convert track change behaviour
	return t.mngr.GetShard(ctx, shardID)
}

// ListAllKeyRanges implements [EntityMgr].
func (t *TranEntityManager) ListAllKeyRanges(ctx context.Context) ([]*kr.KeyRange, error) {
	panic("ListAllKeyRanges unimplemented")
}

// ListDistributions implements [EntityMgr].
func (t *TranEntityManager) ListDistributions(ctx context.Context) ([]*distributions.Distribution, error) {
	if list, err := t.mngr.ListDistributions(ctx); err != nil {
		return nil, err
	} else {
		result := make([]*distributions.Distribution, 0, len(list)+len(t.distributions.Items()))
		for _, distribution := range t.distributions.Items() {
			result = append(result, distribution)
		}
		for _, distribution := range list {
			if _, ok := t.distributions.DeletedItems()[distribution.Id]; ok {
				continue
			}
			if _, ok := t.distributions.Items()[distribution.Id]; ok {
				continue
			}
			result = append(result, distribution)
		}
		return result, nil
	}
}

// ListKeyRangeLocks implements [EntityMgr].
func (t *TranEntityManager) ListKeyRangeLocks(ctx context.Context) ([]string, error) {
	panic("ListKeyRangeLocks unimplemented")
}

// TODO: ADD more tests when altering key range will be realized
// ListKeyRanges implements [EntityMgr].
func (t *TranEntityManager) ListKeyRanges(ctx context.Context, distribution string) ([]*kr.KeyRange, error) {
	list, err := t.mngr.ListKeyRanges(ctx, distribution)
	if err != nil {
		return nil, err
	}
	result := make([]*kr.KeyRange, 0, len(list)+len(t.keyRanges.Items()))
	for _, keyRange := range t.keyRanges.Items() {
		if keyRange.Distribution == distribution {

			result = append(result, keyRange)
	}
	for _, keyRange := range list {
		if _, ok := t.keyRanges.DeletedItems()[keyRange.ID]; ok {
			continue
		}
		if _, ok := t.keyRanges.Items()[keyRange.ID]; ok {
			continue
		}
		result = append(result, keyRange)
	}
	return result, nil


}

// ListMoveTaskGroups implements [EntityMgr].
func (t *TranEntityManager) ListMoveTaskGroups(ctx context.Context) (map[string]*tasks.MoveTaskGroup, error) {
	panic("ListMoveTaskGroups unimplemented")
}

// ListMoveTasks implements [EntityMgr].
func (t *TranEntityManager) ListMoveTasks(ctx context.Context) (map[string]*tasks.MoveTask, error) {
	panic("ListMoveTasks unimplemented")
}

// ListReferenceRelations implements [EntityMgr].
func (t *TranEntityManager) ListReferenceRelations(ctx context.Context) ([]*rrelation.ReferenceRelation, error) {
	panic("ListReferenceRelations unimplemented")
}

// ListRelationSequences implements [EntityMgr].
func (t *TranEntityManager) ListRelationSequences(ctx context.Context, rel *rfqn.RelationFQN) (map[string]string, error) {
	panic("ListRelationSequences unimplemented")
}

// ListRouters implements [EntityMgr].
func (t *TranEntityManager) ListRouters(ctx context.Context) ([]*topology.Router, error) {
	panic("ListRouters unimplemented")
}

// ListSequences implements [EntityMgr].
func (t *TranEntityManager) ListSequences(ctx context.Context) ([]string, error) {
	panic("ListSequences unimplemented")
}

// ListShards implements [EntityMgr].
func (t *TranEntityManager) ListShards(ctx context.Context) ([]*topology.DataShard, error) {
	panic("ListShards unimplemented")
}

// LockKeyRange implements [EntityMgr].
func (t *TranEntityManager) LockKeyRange(ctx context.Context, krid string) (*kr.KeyRange, error) {
	panic("LockKeyRange unimplemented")
}

// Move implements [EntityMgr].
func (t *TranEntityManager) Move(ctx context.Context, move *kr.MoveKeyRange) error {
	panic("Move unimplemented")
}

// NextRange implements [EntityMgr].
func (t *TranEntityManager) NextRange(ctx context.Context, seqName string, rangeSize uint64) (*qdb.SequenceIdRange, error) {
	panic("NextRange unimplemented")
}

// QDB implements [EntityMgr].
func (t *TranEntityManager) QDB() qdb.QDB {
	panic("QDB unimplemented")
}

// RedistributeKeyRange implements [EntityMgr].
func (t *TranEntityManager) RedistributeKeyRange(ctx context.Context, req *kr.RedistributeKeyRange) error {
	panic("RedistributeKeyRange unimplemented")
}

// RegisterRouter implements [EntityMgr].
func (t *TranEntityManager) RegisterRouter(ctx context.Context, r *topology.Router) error {
	panic("RegisterRouter unimplemented")
}

// RemoveBalancerTask implements [EntityMgr].
func (t *TranEntityManager) RemoveBalancerTask(ctx context.Context) error {
	panic("RemoveBalancerTask unimplemented")
}

// RemoveMoveTaskGroup implements [EntityMgr].
func (t *TranEntityManager) RemoveMoveTaskGroup(ctx context.Context, id string) error {
	panic("RemoveMoveTaskGroup unimplemented")
}

// RenameKeyRange implements [EntityMgr].
func (t *TranEntityManager) RenameKeyRange(ctx context.Context, krId string, krIdNew string) error {
	panic("RenameKeyRange unimplemented")
}

// RetryMoveTaskGroup implements [EntityMgr].
func (t *TranEntityManager) RetryMoveTaskGroup(ctx context.Context, id string) error {
	panic("RetryMoveTaskGroup unimplemented")
}

// ShareKeyRange implements [EntityMgr].
func (t *TranEntityManager) ShareKeyRange(id string) error {
	panic("ShareKeyRange unimplemented")
}

// Split implements [EntityMgr].
func (t *TranEntityManager) Split(ctx context.Context, split *kr.SplitKeyRange) error {
	panic("Split unimplemented")
}

// StopMoveTaskGroup implements [EntityMgr].
func (t *TranEntityManager) StopMoveTaskGroup(ctx context.Context, id string) error {
	panic("StopMoveTaskGroup unimplemented")
}

// SyncReferenceRelations implements [EntityMgr].
func (t *TranEntityManager) SyncReferenceRelations(ctx context.Context, ids []*rfqn.RelationFQN, destShard string) error {
	panic("SyncReferenceRelations unimplemented")
}

// SyncRouterCoordinatorAddress implements [EntityMgr].
func (t *TranEntityManager) SyncRouterCoordinatorAddress(ctx context.Context, router *topology.Router) error {
	panic("SyncRouterCoordinatorAddress unimplemented")
}

// SyncRouterMetadata implements [EntityMgr].
func (t *TranEntityManager) SyncRouterMetadata(ctx context.Context, router *topology.Router) error {
	panic("SyncRouterMetadata unimplemented")
}

// Unite implements [EntityMgr].
func (t *TranEntityManager) Unite(ctx context.Context, unite *kr.UniteKeyRange) error {
	panic("Unite unimplemented")
}

// UnlockKeyRange implements [EntityMgr].
func (t *TranEntityManager) UnlockKeyRange(ctx context.Context, krid string) error {
	panic("UnlockKeyRange unimplemented")
}

// UnregisterRouter implements [EntityMgr].
func (t *TranEntityManager) UnregisterRouter(ctx context.Context, id string) error {
	panic("UnregisterRouter unimplemented")
}

// UpdateCoordinator implements [EntityMgr].
func (t *TranEntityManager) UpdateCoordinator(ctx context.Context, address string) error {
	panic("UpdateCoordinator unimplemented")
}

// WriteBalancerTask implements [EntityMgr].
func (t *TranEntityManager) WriteBalancerTask(ctx context.Context, task *tasks.BalancerTask) error {
	panic("WriteBalancerTask unimplemented")
}

// WriteMoveTaskGroup implements [EntityMgr].
func (t *TranEntityManager) WriteMoveTaskGroup(ctx context.Context, taskGroup *tasks.MoveTaskGroup) error {
	panic("WriteMoveTaskGroup unimplemented")
}

// CreateUniqueIndex implements [EntityMgr].
func (t *TranEntityManager) CreateUniqueIndex(ctx context.Context, dsID string, idx *distributions.UniqueIndex) error {
	panic("CreateUniqueIndex unimplemented")
}

// DropUniqueIndex implements [EntityMgr].
func (t *TranEntityManager) DropUniqueIndex(ctx context.Context, idxID string) error {
	panic("DropUniqueIndex unimplemented")
}

// ListDistributionIndexes implements [EntityMgr].
func (t *TranEntityManager) ListDistributionIndexes(ctx context.Context, dsID string) (map[string]*distributions.UniqueIndex, error) {
	panic("ListDistributionIndexes unimplemented")
}

// ListRelationIndexes implements [EntityMgr].
func (t *TranEntityManager) ListRelationIndexes(ctx context.Context, relName *rfqn.RelationFQN) (map[string]*distributions.UniqueIndex, error) {
	panic("ListRelationIndexes unimplemented")
}

// ListUniqueIndexes implements [EntityMgr].
func (t *TranEntityManager) ListUniqueIndexes(ctx context.Context) (map[string]*distributions.UniqueIndex, error) {
	panic("ListUniqueIndexes unimplemented")
}

type MetaEntityList[T any] struct {
	existsItems  map[string]T
	deletedItems map[string]struct{}
}

// Returns saved items
//
// Returns:
// - saved items of type T
func (mList *MetaEntityList[T]) Items() map[string]T {
	return mList.existsItems
}

// Returns deleted items
//
// Returns:
// - deleted items of type T
func (mList *MetaEntityList[T]) DeletedItems() map[string]struct{} {
	return mList.deletedItems
}

// Returns struct for saving changes of type T data
//
// Returns:
// - struct for saving changes of type T data
func NewMetaEntityList[T any]() *MetaEntityList[T] {
	return &MetaEntityList[T]{existsItems: make(map[string]T), deletedItems: make(map[string]struct{})}
}

// Save T in storage of exists items
//
// Parameters:
// - key (string): id of struct T
// - item (T): struct of type T for save
func (mList *MetaEntityList[T]) Save(key string, item T) {
	mList.existsItems[key] = item
	delete(mList.deletedItems, key)
}

// Mark that T with key T is deleted
//
// Parameters:
// - key (string): id of struct T
func (mList *MetaEntityList[T]) Delete(key string) {
	mList.deletedItems[key] = struct{}{}
	delete(mList.existsItems, key)
}
