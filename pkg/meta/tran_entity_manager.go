package meta

import (
	"context"
	"fmt"

	"github.com/pg-sharding/spqr/pkg/models/distributions"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/pkg/models/spqrerror"
	"github.com/pg-sharding/spqr/pkg/models/topology"
	meta_transaction "github.com/pg-sharding/spqr/pkg/models/transaction"
	proto "github.com/pg-sharding/spqr/pkg/protos"
)

// no thread safe struct
type TransactionState struct {
	// prevents double execution
	isCommitted bool

	Chunk       *meta_transaction.MetaTransactionChunk
	Transaction *meta_transaction.MetaTransaction
}

func NewTransactionState() TransactionState {
	return TransactionState{isCommitted: false}
}

func (s *TransactionState) SetTransaction(transaction *meta_transaction.MetaTransaction) error {
	if s.Transaction != nil {
		return fmt.Errorf("corrupted transaction state (setTransaction)")
	}
	if s.Chunk != nil {
		return fmt.Errorf("transaction state begins with no transaction flow")
	}
	s.Transaction = transaction
	return nil
}
func (s *TransactionState) Append(commands []*proto.MetaTransactionGossipCommand) error {
	if s.Transaction != nil && s.Chunk != nil {
		return fmt.Errorf("corrupted transaction state (appendChunk)")
	}
	if s.Transaction != nil {
		return s.Transaction.Operations.Append(commands)
	}
	if s.Chunk == nil {
		s.Chunk = meta_transaction.NewEmptyMetaTransactionChunk()
	}
	return s.Chunk.Append(commands)
}

func (s *TransactionState) CanCommit() bool {
	return !s.isCommitted
}

func (s *TransactionState) SetCommitted() {
	s.isCommitted = true
}

// interface for read only methods of EntityMgr
type EntityMgrReader interface {
	GetShard(ctx context.Context, shardID string) (*topology.DataShard, error)
	GetKeyRange(ctx context.Context, krId string) (*kr.KeyRange, error)
	GetDistribution(ctx context.Context, id string) (*distributions.Distribution, error)
	ListKeyRanges(ctx context.Context, distribution string) ([]*kr.KeyRange, error)
}

// Wrapper of EntityManager. Keeps track of changes made during a transaction that have not yet been committed.
// It's NO NOT THREAD-SAFE because process in single console thread.
// WARNING: ALL implemented methods MUST have 100% test coverage
type TranEntityManager struct {
	EntityMgr
	distributions MetaEntityList[*distributions.Distribution]
	keyRanges     MetaEntityList[*kr.KeyRange]
	state         TransactionState
}

// NewTranEntityManager creates a new instance of the TranEntityManager struct.
//
// Parameters:
// - mngr (EntityMgr): Underlying entity manager that performs metadata operations
//
// Returns:
// - a pointer to an TranEntityManager object.
func NewTranEntityManager(mngr EntityMgr) *TranEntityManager {
	distrList := NewMetaEntityList[*distributions.Distribution]()
	keyRangesList := NewMetaEntityList[*kr.KeyRange]()
	return &TranEntityManager{
		EntityMgr:     mngr,
		distributions: *distrList,
		keyRanges:     *keyRangesList,
		state:         NewTransactionState(),
	}
}

// BeginTran implements [EntityMgr].
func (t *TranEntityManager) BeginTran(ctx context.Context) error {
	transaction, err := t.EntityMgr.BeginTran(ctx)
	if err != nil {
		return err
	}
	transaction.Operations = meta_transaction.NewEmptyMetaTransactionChunk()
	return t.state.SetTransaction(transaction)
}

// CommitTran implements [EntityMgr].
func (t *TranEntityManager) CommitTran(ctx context.Context) error {
	if t.state.CanCommit() {
		err := t.EntityMgr.CommitTran(ctx, t.state.Transaction)
		if err != nil {
			return err
		}
		t.state.SetCommitted()
		return nil
	}
	return fmt.Errorf("can't double transaction")
}

// CreateDistribution implements [EntityMgr].
func (t *TranEntityManager) CreateDistribution(ctx context.Context, ds *distributions.Distribution) error {
	commands := []*proto.MetaTransactionGossipCommand{
		{CreateDistribution: &proto.CreateDistributionGossip{
			Distributions: []*proto.Distribution{distributions.DistributionToProto(ds)},
		}},
	}
	if err := t.state.Append(commands); err != nil {
		return err
	}
	t.distributions.Save(ds.Id, ds)
	return nil
}

// CreateKeyRange implements [EntityMgr].
func (t *TranEntityManager) CreateKeyRange(ctx context.Context, kr *kr.KeyRange) error {
	commands := []*proto.MetaTransactionGossipCommand{
		{CreateKeyRange: &proto.CreateKeyRangeGossip{
			KeyRangeInfo: kr.ToProto(),
		}},
	}
	if err := t.state.Append(commands); err != nil {
		return err
	}
	if _, ok := t.keyRanges.Items()[kr.ID]; ok {
		return fmt.Errorf("key range %s already present in qdb", kr.ID)
	}
	t.keyRanges.Save(kr.ID, kr)
	return nil
}

func (t *TranEntityManager) DropKeyRange(ctx context.Context, idKeyRange string) error {
	commands := []*proto.MetaTransactionGossipCommand{
		{DropKeyRange: &proto.DropKeyRangeGossip{
			Id: []string{idKeyRange},
		}},
	}
	if err := t.state.Append(commands); err != nil {
		return err
	}
	t.keyRanges.Delete(idKeyRange)
	return nil
}

// DropDistribution implements [EntityMgr].
func (t *TranEntityManager) DropDistribution(ctx context.Context, id string) error {
	t.distributions.Delete(id)
	return nil
}

// ExecNoTran implements [EntityMgr].
func (t *TranEntityManager) ExecNoTran(ctx context.Context) error {
	if t.state.Chunk == nil {
		return fmt.Errorf("invalid state for ExecNoTran")
	}
	if t.state.CanCommit() {
		err := t.EntityMgr.ExecNoTran(ctx, t.state.Chunk)
		if err != nil {
			return err
		}
		t.state.SetCommitted()
		return nil
	}
	return fmt.Errorf("can't double execute chunk")
}

// GetDistribution implements [EntityMgr].
func (t *TranEntityManager) GetDistribution(ctx context.Context, id string) (*distributions.Distribution, error) {
	if _, ok := t.distributions.DeletedItems()[id]; ok {
		return nil, spqrerror.Newf(spqrerror.SPQR_OBJECT_NOT_EXIST, "distribution \"%s\" not found", id)
	}
	if savedDs, ok := t.distributions.Items()[id]; ok {
		return savedDs, nil
	}

	if distribution, err := t.EntityMgr.GetDistribution(ctx, id); err != nil {
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
	if keyRangeFromQdb, err := t.EntityMgr.GetKeyRange(ctx, krId); err != nil {
		return nil, err
	} else {
		return keyRangeFromQdb, nil
	}
}

// GetShard implements [EntityMgr].
func (t *TranEntityManager) GetShard(ctx context.Context, shardID string) (*topology.DataShard, error) {
	// TODO convert track change behaviour
	return t.EntityMgr.GetShard(ctx, shardID)
}

// ListDistributions implements [EntityMgr].
func (t *TranEntityManager) ListDistributions(ctx context.Context) ([]*distributions.Distribution, error) {
	if list, err := t.EntityMgr.ListDistributions(ctx); err != nil {
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

// TODO: ADD more tests when altering key range will be realized
// ListKeyRanges implements [EntityMgr].
func (t *TranEntityManager) ListKeyRanges(ctx context.Context, distribution string) ([]*kr.KeyRange, error) {
	list, err := t.EntityMgr.ListKeyRanges(ctx, distribution)
	if err != nil {
		return nil, err
	}
	result := make([]*kr.KeyRange, 0, len(list)+len(t.keyRanges.Items()))
	for _, keyRange := range t.keyRanges.Items() {
		if keyRange.Distribution == distribution {
			result = append(result, keyRange)
		}
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
