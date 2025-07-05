package coord

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/datatransfers"
	"github.com/pg-sharding/spqr/pkg/meta"
	"github.com/pg-sharding/spqr/pkg/models/distributions"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/pkg/models/rrelation"
	"github.com/pg-sharding/spqr/pkg/models/spqrerror"
	"github.com/pg-sharding/spqr/pkg/models/tasks"
	"github.com/pg-sharding/spqr/pkg/models/topology"
	"github.com/pg-sharding/spqr/pkg/spqrlog"
	"github.com/pg-sharding/spqr/qdb"
	"github.com/pg-sharding/spqr/qdb/ops"
	"github.com/pg-sharding/spqr/router/cache"
	"github.com/pg-sharding/spqr/router/rfqn"
)

type Coordinator struct {
	qdb qdb.XQDB
}

var _ meta.EntityMgr = &Coordinator{}

func NewCoordinator(qdb qdb.XQDB) Coordinator {
	return Coordinator{
		qdb: qdb,
	}
}

// SyncReferenceRelations implements meta.EntityMgr.
func (lc *Coordinator) SyncReferenceRelations(ctx context.Context, ids []string, destShard string) error {
	for _, id := range ids {
		rel, err := lc.GetReferenceRelation(ctx, id)
		if err != nil {
			return err
		}

		if len(rel.ShardId) == 0 {
			// XXX: should we error-our here?
			return fmt.Errorf("failed to sync reference relation with no storage shards: %v", id)
		}
		fromShard := rel.ShardId[0]

		if err = datatransfers.SyncReferenceRelation(ctx, fromShard, destShard, rel, lc.qdb); err != nil {
			return err
		}
	}

	return nil
}

// AddDataShard implements meta.EntityMgr.
func (lc *Coordinator) AddDataShard(ctx context.Context, shard *topology.DataShard) error {
	return lc.qdb.AddShard(ctx, topology.DataShardToDB(shard))
}

// AddWorldShard implements meta.EntityMgr.
func (lc *Coordinator) AddWorldShard(ctx context.Context, shard *topology.DataShard) error {
	panic("unimplemented")
}

// AlterDistributedRelation implements meta.EntityMgr.
func (lc *Coordinator) AlterDistributedRelation(ctx context.Context, id string, rel *distributions.DistributedRelation) error {
	if !rel.ReplicatedRelation && len(rel.ColumnSequenceMapping) > 0 {
		return spqrerror.Newf(spqrerror.SPQR_INVALID_REQUEST, "sequences are supported for replicated relations only")
	}
	qdbRel := distributions.DistributedRelationToDB(rel)
	if err := lc.qdb.AlterDistributedRelation(ctx, id, qdbRel); err != nil {
		return err
	}

	for colName, seqName := range rel.ColumnSequenceMapping {
		qualifiedName := rel.ToRFQN()
		if err := lc.qdb.AlterSequenceAttach(ctx, seqName, &qualifiedName, colName); err != nil {
			return err
		}
	}
	return nil
}

// BatchMoveKeyRange implements meta.EntityMgr.
func (lc *Coordinator) BatchMoveKeyRange(ctx context.Context, req *kr.BatchMoveKeyRange) error {
	panic("unimplemented")
}

// Cache implements meta.EntityMgr.
func (lc *Coordinator) Cache() *cache.SchemaCache {
	panic("unimplemented")
}

// CreateReferenceRelation implements meta.EntityMgr.
func (lc *Coordinator) CreateReferenceRelation(ctx context.Context, r *rrelation.ReferenceRelation, entry []*rrelation.AutoIncrementEntry) error {

	if _, err := lc.qdb.GetReferenceRelation(ctx, r.TableName); err == nil {
		return fmt.Errorf("reference relation %+v already exists", r.TableName)
	}

	selectedDistribId := distributions.REPLICATED

	if _, err := lc.GetDistribution(ctx, selectedDistribId); err != nil {
		err := lc.CreateDistribution(ctx, &distributions.Distribution{
			Id:       distributions.REPLICATED,
			ColTypes: nil,
		})
		if err != nil {
			spqrlog.Zero.Debug().Err(err).Msg("failed to setup REPLICATED distribution")
			return err
		}
	}

	ret := map[string]string{}
	for _, entry := range entry {
		ret[entry.Column] = distributions.SequenceName(r.TableName, entry.Column)

		if err := lc.qdb.CreateSequence(ctx, ret[entry.Column], int64(entry.Start)); err != nil {
			return err
		}
		qualifiedName := rfqn.RelationFQN{RelationName: r.TableName}
		if err := lc.qdb.AlterSequenceAttach(ctx, ret[entry.Column], &qualifiedName, entry.Column); err != nil {
			return err
		}
	}

	r.ColumnSequenceMapping = ret

	if r.ShardId == nil {
		// default is all shards
		shs, err := lc.ListShards(ctx)
		if err != nil {
			return err
		}
		for _, sh := range shs {
			r.ShardId = append(r.ShardId, sh.ID)
		}
	}

	if err := lc.qdb.CreateReferenceRelation(ctx, rrelation.RefRelationToDB(r)); err != nil {
		return err
	}

	return lc.AlterDistributionAttach(ctx, selectedDistribId, []*distributions.DistributedRelation{
		{
			Name:                  r.TableName,
			ReplicatedRelation:    true,
			ColumnSequenceMapping: ret,
		},
	})
}

// CurrVal implements meta.EntityMgr.
func (lc *Coordinator) CurrVal(ctx context.Context, seqName string) (int64, error) {
	return lc.qdb.CurrVal(ctx, seqName)
}

// DropDistribution implements meta.EntityMgr.
func (lc *Coordinator) DropDistribution(ctx context.Context, id string) error {
	if id == distributions.REPLICATED {
		ds, err := lc.qdb.GetDistribution(ctx, id)
		if err != nil {
			return err
		}
		for _, r := range ds.Relations {
			if err := lc.qdb.DropReferenceRelation(ctx, r.Name); err != nil {
				return err
			}
		}
	}
	return lc.qdb.DropDistribution(ctx, id)
}

// DropKeyRange implements meta.EntityMgr.
func (lc *Coordinator) DropKeyRange(ctx context.Context, id string) error {
	return lc.qdb.DropKeyRange(ctx, id)
}

// DropKeyRangeAll implements meta.EntityMgr.
func (lc *Coordinator) DropKeyRangeAll(ctx context.Context) error {
	return lc.qdb.DropKeyRangeAll(ctx)
}

// DropReferenceRelation implements meta.EntityMgr.
func (lc *Coordinator) DropReferenceRelation(ctx context.Context, id string) error {
	err := lc.qdb.DropReferenceRelation(ctx, id)
	if err != nil {
		return err
	}
	qualifiedName := &rfqn.RelationFQN{RelationName: id}
	return lc.AlterDistributionDetach(ctx, distributions.REPLICATED, qualifiedName)
}

// DropSequence implements meta.EntityMgr.
func (lc *Coordinator) DropSequence(ctx context.Context, name string) error {
	return lc.qdb.DropSequence(ctx, name)
}

// DropShard implements meta.EntityMgr.
func (lc *Coordinator) DropShard(ctx context.Context, shardId string) error {
	return lc.qdb.DropShard(ctx, shardId)
}

// GetBalancerTask implements meta.EntityMgr.
func (lc *Coordinator) GetBalancerTask(ctx context.Context) (*tasks.BalancerTask, error) {
	taskDb, err := lc.qdb.GetBalancerTask(ctx)
	if err != nil {
		return nil, err
	}
	return tasks.BalancerTaskFromDb(taskDb), nil
}

// GetCoordinator implements meta.EntityMgr.
func (lc *Coordinator) GetCoordinator(ctx context.Context) (string, error) {
	return lc.qdb.GetCoordinator(ctx)
}

// GetShard implements meta.EntityMgr.
func (lc *Coordinator) GetShard(ctx context.Context, shardID string) (*topology.DataShard, error) {
	sh, err := lc.qdb.GetShard(ctx, shardID)
	if err != nil {
		return nil, err
	}
	return topology.DataShardFromDB(sh), nil
}

// ListAllKeyRanges implements meta.EntityMgr.
func (lc *Coordinator) ListAllKeyRanges(ctx context.Context) ([]*kr.KeyRange, error) {
	keyRanges, err := lc.qdb.ListAllKeyRanges(ctx)
	if err != nil {
		return nil, err
	}

	keyr := make([]*kr.KeyRange, 0, len(keyRanges))
	for _, keyRange := range keyRanges {
		ds, err := lc.qdb.GetDistribution(ctx, keyRange.DistributionId)
		if err != nil {
			return nil, err
		}
		keyr = append(keyr, kr.KeyRangeFromDB(keyRange, ds.ColTypes))
	}

	return keyr, nil
}

// ListReferenceRelations implements meta.EntityMgr.
func (lc *Coordinator) ListReferenceRelations(ctx context.Context) ([]*rrelation.ReferenceRelation, error) {
	var ret []*rrelation.ReferenceRelation
	rrs, err := lc.qdb.ListReferenceRelations(ctx)
	if err != nil {
		return nil, err
	}

	for _, r := range rrs {
		ret = append(ret, rrelation.RefRelationFromDB(r))
	}

	return ret, nil
}

// ListRouters implements meta.EntityMgr.
func (lc *Coordinator) ListRouters(ctx context.Context) ([]*topology.Router, error) {
	resp, err := lc.qdb.ListRouters(ctx)
	if err != nil {
		return nil, err
	}
	var retRouters []*topology.Router

	for _, v := range resp {
		retRouters = append(retRouters, &topology.Router{
			ID:      v.ID,
			Address: v.Address,
			State:   v.State,
		})
	}

	return retRouters, nil
}

// Move implements meta.EntityMgr.
func (lc *Coordinator) Move(ctx context.Context, move *kr.MoveKeyRange) error {
	panic("unimplemented")
}

// NextVal implements meta.EntityMgr.
func (lc *Coordinator) NextVal(ctx context.Context, seqName string) (int64, error) {
	return lc.qdb.NextVal(ctx, seqName)
}

// QDB implements meta.EntityMgr.
func (lc *Coordinator) QDB() qdb.QDB {
	return lc.qdb
}

// RedistributeKeyRange implements meta.EntityMgr.
func (lc *Coordinator) RedistributeKeyRange(ctx context.Context, req *kr.RedistributeKeyRange) error {
	panic("unimplemented")
}

// RegisterRouter implements meta.EntityMgr.
func (lc *Coordinator) RegisterRouter(ctx context.Context, r *topology.Router) error {
	return lc.qdb.AddRouter(ctx, qdb.NewRouter(r.Address, r.ID, qdb.OPENED))
}

// RemoveBalancerTask implements meta.EntityMgr.
func (lc *Coordinator) RemoveBalancerTask(ctx context.Context) error {
	return lc.qdb.RemoveBalancerTask(ctx)
}

// RenameKeyRange implements meta.EntityMgr.
func (lc *Coordinator) RenameKeyRange(ctx context.Context, krId string, krIdNew string) error {
	if _, err := lc.GetKeyRange(ctx, krId); err != nil {
		return err
	}
	if _, err := lc.LockKeyRange(ctx, krId); err != nil {
		return err
	}
	if _, err := lc.GetKeyRange(ctx, krIdNew); err == nil {
		return spqrerror.New(spqrerror.SPQR_KEYRANGE_ERROR, fmt.Sprintf("key range '%s' already exists", krIdNew))
	}
	if err := lc.qdb.RenameKeyRange(ctx, krId, krIdNew); err != nil {
		return err
	}
	return nil
}

// RetryMoveTaskGroup implements meta.EntityMgr.
func (lc *Coordinator) RetryMoveTaskGroup(ctx context.Context) error {
	panic("unimplemented")
}

// SyncRouterCoordinatorAddress implements meta.EntityMgr.
func (lc *Coordinator) SyncRouterCoordinatorAddress(ctx context.Context, router *topology.Router) error {
	panic("unimplemented")
}

// SyncRouterMetadata implements meta.EntityMgr.
func (lc *Coordinator) SyncRouterMetadata(ctx context.Context, router *topology.Router) error {
	panic("unimplemented")
}

// UnregisterRouter implements meta.EntityMgr.
func (lc *Coordinator) UnregisterRouter(ctx context.Context, rID string) error {
	spqrlog.Zero.Debug().
		Str("router", rID).
		Msg("unregister router")
	return lc.qdb.DeleteRouter(ctx, rID)
}

// UpdateCoordinator implements meta.EntityMgr.
func (lc *Coordinator) UpdateCoordinator(ctx context.Context, address string) error {
	return lc.qdb.UpdateCoordinator(ctx, address)
}

// WriteBalancerTask implements meta.EntityMgr.
func (lc *Coordinator) WriteBalancerTask(ctx context.Context, task *tasks.BalancerTask) error {
	return lc.qdb.WriteBalancerTask(ctx, tasks.BalancerTaskToDb(task))
}

// GetMoveTaskGroup retrieves the MoveTask group from the local coordinator's QDB.
//
// Parameters:
// - ctx (context.Context): the context.Context object for managing the request's lifetime.
//
// Returns:
// - *tasks.MoveTaskGroup: the retrieved task group, or nil if an error occurred.
// - error: an error if the retrieval process fails.
func (lc *Coordinator) GetMoveTaskGroup(ctx context.Context) (*tasks.MoveTaskGroup, error) {
	groupDB, err := lc.qdb.GetMoveTaskGroup(ctx)
	if err != nil {
		return nil, err
	}
	taskMap := make(map[string]*qdb.MoveTask, len(groupDB.TaskIDs))
	for _, id := range groupDB.TaskIDs[groupDB.CurrentTaskInd:] {
		task, err := lc.qdb.GetMoveTask(ctx, id)
		if err != nil {
			return nil, err
		}
		taskMap[id] = task
	}
	return tasks.TaskGroupFromDb(groupDB, taskMap)
}

// GetMoveTask retrieves the MoveTask from the coordinator's QDB.
//
// Parameters:
// - ctx (context.Context): the context.Context object for managing the request's lifetime.
// - id (string): the ID of the task to be retrieved
//
// Returns:
// - *tasks.MoveTask: the retrieved move task, or nil if an error occurred.
// - error: an error if the retrieval process fails.
func (qc *Coordinator) GetMoveTask(ctx context.Context, id string) (*tasks.MoveTask, error) {
	task, err := qc.qdb.GetMoveTask(ctx, id)
	if err != nil {
		return nil, err
	}
	return tasks.TaskFromDb(task), err
}

// GetDistribution retrieves info about distribution from QDB
// TODO: unit tests
func (lc *Coordinator) GetDistribution(ctx context.Context, id string) (*distributions.Distribution, error) {
	ret, err := lc.qdb.GetDistribution(ctx, id)
	if err != nil {
		return nil, err
	}
	ds := distributions.DistributionFromDB(ret)
	for relName := range ds.Relations {
		qualifiedName, err := rfqn.ParseFQN(relName)
		if err != nil {
			return nil, err
		}
		mapping, err := lc.qdb.GetRelationSequence(ctx, qualifiedName)
		if err != nil {
			return nil, err
		}
		ds.Relations[relName].ColumnSequenceMapping = mapping
	}
	return ds, nil
}

// GetReference relations retrieves info about ref relation from QDB
// TODO: unit tests
func (lc *Coordinator) GetReferenceRelation(ctx context.Context, tableName string) (*rrelation.ReferenceRelation, error) {
	ret, err := lc.qdb.GetReferenceRelation(ctx, tableName)
	if err != nil {
		return nil, err
	}
	return rrelation.RefRelationFromDB(ret), nil
}

// GetRelationDistribution retrieves info about distribution attached to relation from QDB
// TODO: unit tests
func (lc *Coordinator) GetRelationDistribution(ctx context.Context, relation *rfqn.RelationFQN) (*distributions.Distribution, error) {
	ret, err := lc.qdb.GetRelationDistribution(ctx, relation)
	if err != nil {
		return nil, err
	}
	ds := distributions.DistributionFromDB(ret)
	for relName := range ds.Relations {
		qualifiedName, err := rfqn.ParseFQN(relName)
		if err != nil {
			return nil, err
		}
		mapping, err := lc.qdb.GetRelationSequence(ctx, qualifiedName)
		if err != nil {
			return nil, err
		}
		ds.Relations[relName].ColumnSequenceMapping = mapping
	}
	return ds, nil
}

func (lc *Coordinator) ListShards(ctx context.Context) ([]*topology.DataShard, error) {
	resp, err := lc.qdb.ListShards(ctx)
	if err != nil {
		return nil, err
	}
	var retShards []*topology.DataShard

	for _, sh := range resp {
		retShards = append(retShards, &topology.DataShard{
			ID: sh.ID,
			Cfg: &config.Shard{
				RawHosts: sh.RawHosts,
			},
		})
	}
	return retShards, nil
}

// TODO unit tests

// GetKeyRange gets key range by id
// GetKeyRange retrieves a key range identified by krId from the LocalCoordinator.
//
// Parameters:
// - ctx (context.Context): the context of the operation.
// - krId (string): the ID of the key range to retrieve.
//
// Returns:
// - *kr.KeyRange: the KeyRange object retrieved.
// - error: an error if the retrieval encounters any issues.
func (qc *Coordinator) GetKeyRange(ctx context.Context, krId string) (*kr.KeyRange, error) {
	krDb, err := qc.qdb.GetKeyRange(ctx, krId)
	if err != nil {
		return nil, err
	}
	ds, err := qc.qdb.GetDistribution(ctx, krDb.DistributionId)
	if err != nil {
		return nil, err
	}
	return kr.KeyRangeFromDB(krDb, ds.ColTypes), nil
}

// TODO : unit tests

// ListKeyRanges retrieves a list of key ranges associated with the specified distribution from the LocalCoordinator.
//
// Parameters:
// - ctx: the context of the operation.
// - distribution: the distribution to filter the key ranges by.
//
// Returns:
// - []*kr.KeyRange: a slice of KeyRange objects retrieved.
// - error: an error if the retrieval encounters any issues.
func (qc *Coordinator) ListKeyRanges(ctx context.Context, distribution string) ([]*kr.KeyRange, error) {
	keyRanges, err := qc.qdb.ListKeyRanges(ctx, distribution)
	if err != nil {
		return nil, err
	}

	keyr := make([]*kr.KeyRange, 0, len(keyRanges))
	for _, keyRange := range keyRanges {
		ds, err := qc.qdb.GetDistribution(ctx, keyRange.DistributionId)
		if err != nil {
			return nil, err
		}
		keyr = append(keyr, kr.KeyRangeFromDB(keyRange, ds.ColTypes))
	}

	return keyr, nil
}

// WriteMoveTaskGroup writes the given task group to the coordinator's QDB.
//
// Parameters:
// - ctx (context.Context): the context.Context object for managing the request's lifetime.
// - taskGroup (*tasks.MoveTaskGroup): the task group to be written to the QDB.
//
// Returns:
// - error: an error if the write operation fails.
func (qc *Coordinator) WriteMoveTaskGroup(ctx context.Context, taskGroup *tasks.MoveTaskGroup) error {
	if err := qc.qdb.WriteMoveTaskGroup(ctx, tasks.TaskGroupToDb(taskGroup)); err != nil {
		return err
	}
	for _, task := range taskGroup.Tasks[taskGroup.CurrentTaskIndex:] {
		if err := qc.qdb.CreateMoveTask(ctx, tasks.MoveTaskToDb(task)); err != nil {
			return err
		}
	}
	return nil
}

// CreateMoveTask writes the given move task to the coordinator's QDB if task doesn't already exists
//
// Parameters:
// - ctx (context.Context): the context.Context object for managing the request's lifetime.
// - task (*tasks.MoveTask): the task to be written to the QDB.
//
// Returns:
// - error: an error if the write operation fails.
func (qc *Coordinator) CreateMoveTask(ctx context.Context, task *tasks.MoveTask) error {
	return qc.qdb.CreateMoveTask(ctx, tasks.MoveTaskToDb(task))
}

// UpdateMoveTask writes the given move task to the coordinator's QDB if task already exists
//
// Parameters:
// - ctx (context.Context): the context.Context object for managing the request's lifetime.
// - task (*tasks.MoveTask): the task to be written to the QDB.
//
// Returns:
// - error: an error if the write operation fails.
func (qc *Coordinator) UpdateMoveTask(ctx context.Context, task *tasks.MoveTask) error {
	return qc.qdb.UpdateMoveTask(ctx, tasks.MoveTaskToDb(task))
}

// RemoveMoveTaskGroup removes the task group from the local coordinator's QDB.
//
// Parameters:
// - ctx (context.Context): the context.Context object for managing the request's lifetime.
//
// Returns:
// - error: an error if the removal operation fails.
func (qc *Coordinator) RemoveMoveTaskGroup(ctx context.Context) error {
	return qc.qdb.RemoveMoveTaskGroup(ctx)
}

// TODO : unit tests

// ListDistributions retrieves a list of distributions from the local coordinator's QDB.
//
// Parameters:
// - ctx (context.Context): the context.Context object for managing the request's lifetime.
//
// Returns:
// - []*distributions.Distribution: a slice of distributions.Distribution objects representing the retrieved distributions.
// - error: an error if the retrieval operation fails.
func (qc *Coordinator) ListDistributions(ctx context.Context) ([]*distributions.Distribution, error) {
	distrs, err := qc.qdb.ListDistributions(ctx)
	if err != nil {
		return nil, err
	}
	res := make([]*distributions.Distribution, 0)
	for _, ds := range distrs {
		ret := distributions.DistributionFromDB(ds)
		for relName := range ds.Relations {
			qualifiedName, err := rfqn.ParseFQN(relName)
			if err != nil {
				return nil, err
			}
			mapping, err := qc.qdb.GetRelationSequence(ctx, qualifiedName)
			if err != nil {
				return nil, err
			}
			ret.Relations[relName].ColumnSequenceMapping = mapping
		}
		res = append(res, ret)
	}
	return res, nil
}

func (qc *Coordinator) CreateDistribution(ctx context.Context, ds *distributions.Distribution) error {
	if len(ds.ColTypes) == 0 && ds.Id != distributions.REPLICATED {
		return fmt.Errorf("empty distributions are disallowed")
	}
	for _, rel := range ds.Relations {
		for colName, SeqName := range rel.ColumnSequenceMapping {

			if err := qc.qdb.CreateSequence(ctx, SeqName, 0); err != nil {
				return err
			}
			qualifiedName := rel.ToRFQN()
			err := qc.qdb.AlterSequenceAttach(ctx, SeqName, &qualifiedName, colName)
			if err != nil {
				return err
			}
		}
	}
	return qc.qdb.CreateDistribution(ctx, distributions.DistributionToDB(ds))
}

// TODO : unit tests

// AlterDistributionDetach alters the distribution by detaching a specific distributed relation.
//
// Parameters:
// - ctx (context.Context): the context.Context object for managing the request's lifetime.
// - id (string): the ID of the distribution to be altered.
// - relName (string): the name of the distributed relation to be detached.
//
// Returns:
// - error: an error if the alteration operation fails.
func (qc *Coordinator) AlterDistributionDetach(ctx context.Context, id string, relName *rfqn.RelationFQN) error {
	return qc.qdb.AlterDistributionDetach(ctx, id, relName)
}

// ShareKeyRange shares a key range with the LocalCoordinator.
//
// Parameters:
// - id (string): The ID of the key range to be shared.
//
// Returns:
// - error: An error indicating the sharing status.
func (qc *Coordinator) ShareKeyRange(id string) error {
	return qc.qdb.ShareKeyRange(id)
}

// CreateKeyRange creates a new key range in the LocalCoordinator.
//
// Parameters:
// - ctx (context.Context): The context of the operation.
// - kr (*kr.KeyRange): The key range object to be created.
//
// Returns:
// - error: An error if the creation encounters any issues.
func (lc *Coordinator) CreateKeyRange(ctx context.Context, kr *kr.KeyRange) error {
	return ops.CreateKeyRangeWithChecks(ctx, lc.qdb, kr)
}

// TODO : unit tests

// LockKeyRange locks a key range identified by krid and returns the corresponding KeyRange object.
//
// Parameters:
// - ctx (context.Context): The context.Context object for managing the request's lifetime.
// - krid (string): the ID of the key range to lock.
//
// Returns:
// - *kr.KeyRange: the locked KeyRange object.
// - error: an error if the lock operation encounters any issues.
func (qc *Coordinator) LockKeyRange(ctx context.Context, keyRangeID string) (*kr.KeyRange, error) {
	keyRangeDB, err := qc.qdb.LockKeyRange(ctx, keyRangeID)
	if err != nil {
		return nil, err
	}
	ds, err := qc.qdb.GetDistribution(ctx, keyRangeDB.DistributionId)
	if err != nil {
		_ = qc.qdb.UnlockKeyRange(ctx, keyRangeID)
		return nil, err
	}

	return kr.KeyRangeFromDB(keyRangeDB, ds.ColTypes), nil
}

// TODO : unit tests

// UnlockKeyRange unlocks a key range identified by krid.
//
// Parameters:
// - ctx (context.Context): The context.Context object for managing the request's lifetime.
// - krid (string): the ID of the key range to lock.
//
// Returns:
// - error: an error if the unlock operation encounters any issues.
func (qc *Coordinator) UnlockKeyRange(ctx context.Context, keyRangeID string) error {
	return qc.qdb.UnlockKeyRange(ctx, keyRangeID)
}

// TODO : unit tests

// AlterDistributionAttach alters the distribution by attaching additional distributed relations.
//
// Parameters:
// - ctx (context.Context): the context.Context object for managing the request's lifetime.
// - id (string): the ID of the distribution to be altered.
// - rels ([]*distributions.DistributedRelation): the slice of distributions.DistributedRelation objects representing the relations to be attached.
//
// Returns:
// - error: an error if the alteration operation fails.
func (lc *Coordinator) AlterDistributionAttach(ctx context.Context, id string, rels []*distributions.DistributedRelation) error {
	ds, err := lc.qdb.GetDistribution(ctx, id)
	if err != nil {
		return err
	}

	dRels := []*qdb.DistributedRelation{}
	for _, r := range rels {
		if !r.ReplicatedRelation && len(r.DistributionKey) != len(ds.ColTypes) {
			return fmt.Errorf("cannot attach relation %v to distribution %v: number of column mismatch", r.Name, ds.ID)
		}
		if !r.ReplicatedRelation && len(r.ColumnSequenceMapping) > 0 {
			return spqrerror.Newf(spqrerror.SPQR_INVALID_REQUEST, "sequence are supported for replicated relations only")
		}
		dRels = append(dRels, distributions.DistributedRelationToDB(r))
	}

	err = lc.qdb.AlterDistributionAttach(ctx, id, dRels)
	if err != nil {
		return err
	}

	return nil
}

// TODO : unit tests

// Unite merges two key ranges identified by req.BaseKeyRangeId and req.AppendageKeyRangeId into a single key range.
//
// Parameters:
// - ctx (context.Context): The context.Context object for managing the request's lifetime.
// - req (*kr.UniteKeyRange): a pointer to a UniteKeyRange object containing the necessary information for the unite operation.
//
// Returns:
// - error: an error if the unite operation encounters any issues.
func (lc *Coordinator) Unite(ctx context.Context, uniteKeyRange *kr.UniteKeyRange) error {
	krBaseDb, err := lc.qdb.LockKeyRange(ctx, uniteKeyRange.BaseKeyRangeId)
	if err != nil {
		return err
	}

	defer func() {
		if err := lc.qdb.UnlockKeyRange(ctx, uniteKeyRange.BaseKeyRangeId); err != nil {
			spqrlog.Zero.Error().Err(err).Msg("")
		}
	}()

	ds, err := lc.qdb.GetDistribution(ctx, krBaseDb.DistributionId)
	if err != nil {
		return err
	}

	krAppendageDb, err := lc.qdb.GetKeyRange(ctx, uniteKeyRange.AppendageKeyRangeId)
	if err != nil {
		return err
	}

	krBase := kr.KeyRangeFromDB(krBaseDb, ds.ColTypes)
	krAppendage := kr.KeyRangeFromDB(krAppendageDb, ds.ColTypes)

	if krBase.ShardID != krAppendage.ShardID {
		return spqrerror.New(spqrerror.SPQR_KEYRANGE_ERROR, "failed to unite key ranges routing different shards")
	}
	if krBase.Distribution != krAppendage.Distribution {
		return spqrerror.New(spqrerror.SPQR_KEYRANGE_ERROR, "failed to unite key ranges of different distributions")
	}
	// TODO: check all types when composite keys are supported
	krLeft, krRight := krBase, krAppendage
	if kr.CmpRangesLess(krRight.LowerBound, krLeft.LowerBound, ds.ColTypes) {
		krLeft, krRight = krRight, krLeft
	}

	krs, err := lc.ListKeyRanges(ctx, ds.ID)
	if err != nil {
		return err
	}
	for _, kRange := range krs {
		if kRange.ID != krLeft.ID &&
			kRange.ID != krRight.ID &&
			kr.CmpRangesLessEqual(krLeft.LowerBound, kRange.LowerBound, ds.ColTypes) &&
			kr.CmpRangesLessEqual(kRange.LowerBound, krRight.LowerBound, ds.ColTypes) {
			return spqrerror.New(spqrerror.SPQR_KEYRANGE_ERROR, "failed to unite non-adjacent key ranges")
		}
	}

	if err := lc.qdb.DropKeyRange(ctx, krAppendage.ID); err != nil {
		return spqrerror.Newf(spqrerror.SPQR_KEYRANGE_ERROR, "failed to drop an old key range: %s", err.Error())
	}

	if krLeft.ID != krBase.ID {
		krBase.LowerBound = krAppendage.LowerBound
	}

	if err := ops.ModifyKeyRangeWithChecks(ctx, lc.qdb, krBase); err != nil {
		return spqrerror.Newf(spqrerror.SPQR_KEYRANGE_ERROR, "failed to update a new key range: %s", err.Error())
	}
	return nil
}

// Caller should lock key range
// TODO : unit tests

// Split splits an existing key range identified by req.SourceID into two new key ranges.
//
// Parameters:
// - ctx (context.Context): The context.Context object for managing the request's lifetime.
// - req (*kr.SplitKeyRange): a pointer to a SplitKeyRange object containing the necessary information for the split operation.
//
// Returns:
// - error: an error if the split operation encounters any issues.
func (qc *Coordinator) Split(ctx context.Context, req *kr.SplitKeyRange) error {
	spqrlog.Zero.Debug().
		Str("krid", req.Krid).
		Interface("bound", req.Bound).
		Str("source-id", req.SourceID).
		Msg("split request is")

	if _, err := qc.qdb.GetKeyRange(ctx, req.Krid); err == nil {
		return spqrerror.Newf(spqrerror.SPQR_KEYRANGE_ERROR, "key range %v already present in qdb", req.Krid)
	}

	krOldDB, err := qc.qdb.LockKeyRange(ctx, req.SourceID)
	if err != nil {
		return err
	}

	if !req.SplitLeft {
		defer func() {
			if err := qc.qdb.UnlockKeyRange(ctx, req.SourceID); err != nil {
				spqrlog.Zero.Error().Err(err).Msg("")
			}
		}()
	}

	ds, err := qc.qdb.GetDistribution(ctx, krOldDB.DistributionId)

	if err != nil {
		return err
	}

	krOld := kr.KeyRangeFromDB(krOldDB, ds.ColTypes)

	eph := kr.KeyRangeFromBytes(req.Bound, ds.ColTypes)

	if kr.CmpRangesEqual(krOld.LowerBound, eph.LowerBound, ds.ColTypes) {
		return spqrerror.New(spqrerror.SPQR_KEYRANGE_ERROR, "failed to split because bound equals lower of the key range")
	}

	if kr.CmpRangesLess(eph.LowerBound, krOld.LowerBound, ds.ColTypes) {
		return spqrerror.New(spqrerror.SPQR_KEYRANGE_ERROR, "failed to split because bound is out of key range")
	}

	krs, err := qc.ListKeyRanges(ctx, ds.ID)
	if err != nil {
		return err
	}
	for _, kRange := range krs {
		if kr.CmpRangesLess(krOld.LowerBound, kRange.LowerBound, ds.ColTypes) && kr.CmpRangesLessEqual(kRange.LowerBound, eph.LowerBound, ds.ColTypes) {
			return spqrerror.Newf(spqrerror.SPQR_KEYRANGE_ERROR, "failed to split because bound intersects with \"%s\" key range", kRange.ID)
		}
	}

	krTemp := kr.KeyRangeFromDB(
		&qdb.KeyRange{
			// fix multidim case
			LowerBound:     req.Bound,
			KeyRangeID:     uuid.NewString(),
			ShardID:        krOld.ShardID,
			DistributionId: krOld.Distribution,
		},
		ds.ColTypes,
	)

	if err := ops.CreateKeyRangeWithChecks(ctx, qc.qdb, krTemp); err != nil {
		return spqrerror.Newf(spqrerror.SPQR_KEYRANGE_ERROR, "failed to add a new key range: %s", err.Error())
	}

	spqrlog.Zero.Debug().
		Bytes("lower-bound", krTemp.Raw()[0]).
		Str("shard-id", krTemp.ShardID).
		Str("id", krTemp.ID).
		Msg("new key range")

	rightKr := req.Krid
	if req.SplitLeft {
		rightKr = krOld.ID
		if err := qc.qdb.RenameKeyRange(ctx, krOld.ID, req.Krid); err != nil {
			return err
		}
	}

	if err := qc.qdb.RenameKeyRange(ctx, krTemp.ID, rightKr); err != nil {
		return err
	}

	return nil
}

func (lc *Coordinator) ListSequences(ctx context.Context) ([]string, error) {
	return lc.qdb.ListSequences(ctx)
}
