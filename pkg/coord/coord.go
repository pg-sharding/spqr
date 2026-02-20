package coord

import (
	"context"
	"fmt"
	"slices"

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
	mtran "github.com/pg-sharding/spqr/pkg/models/transaction"
	"github.com/pg-sharding/spqr/pkg/spqrlog"
	"github.com/pg-sharding/spqr/qdb"
	"github.com/pg-sharding/spqr/router/cache"
	"github.com/pg-sharding/spqr/router/rfqn"
	"github.com/sethvargo/go-retry"
)

type Coordinator struct {
	qdb qdb.XQDB
	dcs qdb.DCStateKeeper
}

var _ meta.EntityMgr = &Coordinator{}

func NewCoordinator(q qdb.XQDB, d qdb.DCStateKeeper) Coordinator {
	return Coordinator{
		qdb: q,
		dcs: d,
	}
}

// AlterReferenceRelationStorage implements meta.EntityMgr.
func (lc *Coordinator) AlterReferenceRelationStorage(ctx context.Context, relName *rfqn.RelationFQN, shs []string) error {
	return lc.qdb.AlterReferenceRelationStorage(ctx, relName, shs)
}

// SyncReferenceRelations implements meta.EntityMgr.
func (lc *Coordinator) SyncReferenceRelations(ctx context.Context, relNames []*rfqn.RelationFQN, destShard string) error {
	for _, qualName := range relNames {
		rel, err := lc.GetReferenceRelation(ctx, qualName)
		if err != nil {
			return err
		}

		if len(rel.ShardIds) == 0 {
			// XXX: should we error-our here?
			return fmt.Errorf("failed to sync reference relation with no storage shards: %v", qualName)
		}
		fromShard := rel.ShardIds[0]

		// XXX: should we ignore the command/error here?
		destShards := rel.ShardIds
		if !slices.Contains(rel.ShardIds, destShard) {
			destShards = append(rel.ShardIds, destShard)
		}

		if err = datatransfers.SyncReferenceRelation(ctx, fromShard, destShard, rel, lc.qdb); err != nil {
			return err
		}

		if err := lc.qdb.AlterReferenceRelationStorage(ctx, qualName, destShards); err != nil {
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

// AlterDistributedRelationDistributionKey implements meta.EntityMgr.
func (lc *Coordinator) AlterDistributedRelationDistributionKey(ctx context.Context, id string, relName string, distributionKey []distributions.DistributionKeyEntry) error {
	if id == distributions.REPLICATED {
		return fmt.Errorf("setting distribution key is forbidden for reference relations")
	}
	ds, err := lc.GetDistribution(ctx, id)
	if err != nil {
		return err
	}
	if len(ds.ColTypes) != len(distributionKey) {
		return fmt.Errorf("cannot alter relation \"%s\" distribution key: numbers of columns mismatch", relName)
	}
	return lc.qdb.AlterDistributedRelationDistributionKey(ctx, id, relName, distributions.DistributionKeyToDB(distributionKey))
}

// AlterDistributedRelationSchema implements meta.EntityMgr.
func (lc *Coordinator) AlterDistributedRelationSchema(ctx context.Context, id string, relName string, schemaName string) error {
	if id == distributions.REPLICATED {
		return lc.qdb.AlterReplicatedRelationSchema(ctx, distributions.REPLICATED, relName, schemaName)
	}
	return lc.qdb.AlterDistributedRelationSchema(ctx, id, relName, schemaName)
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
	/* XXX: fix this */

	relName := &rfqn.RelationFQN{
		RelationName: r.TableName,
	}

	if _, err := lc.qdb.GetReferenceRelation(ctx, relName); err == nil {
		return fmt.Errorf("reference relation %+v already exists", r.TableName)
	}

	selectedDistribId := distributions.REPLICATED

	if _, err := lc.GetDistribution(ctx, selectedDistribId); err != nil {
		statements, err := lc.CreateDistribution(ctx, &distributions.Distribution{
			Id:       distributions.REPLICATED,
			ColTypes: nil,
		})
		if err != nil {
			spqrlog.Zero.Debug().Err(err).Msg("failed to setup REPLICATED distribution (prepare phase)")
			return err
		}
		err = lc.qdb.ExecNoTransaction(ctx, statements)
		if err != nil {
			spqrlog.Zero.Debug().Err(err).Msg("failed to setup REPLICATED distribution (exec phase)")
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

	if r.ShardIds == nil {
		// default is all shards
		shs, err := lc.ListShards(ctx)
		if err != nil {
			return err
		}
		for _, sh := range shs {
			r.ShardIds = append(r.ShardIds, sh.ID)
		}
	}

	if err := lc.qdb.CreateReferenceRelation(ctx, rrelation.RefRelationToDB(r)); err != nil {
		return err
	}

	return lc.AlterDistributionAttach(ctx, selectedDistribId, []*distributions.DistributedRelation{
		{
			Name:                  r.TableName,
			SchemaName:            r.SchemaName,
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
			if err := lc.qdb.DropReferenceRelation(ctx, r.QualifiedName()); err != nil {
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
func (lc *Coordinator) DropReferenceRelation(ctx context.Context, relName *rfqn.RelationFQN) error {
	err := lc.qdb.DropReferenceRelation(ctx, relName)
	if err != nil {
		return err
	}
	return lc.AlterDistributionDetach(ctx, distributions.REPLICATED, relName)
}

// DropSequence implements meta.EntityMgr.
func (lc *Coordinator) DropSequence(ctx context.Context, name string, force bool) error {
	return lc.qdb.DropSequence(ctx, name, force)
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

	krs := make([]*kr.KeyRange, 0, len(keyRanges))
	dsIdColTypes := make(map[string][]string)
	for _, keyRange := range keyRanges {
		var colTypes []string
		ok := false
		if colTypes, ok = dsIdColTypes[keyRange.DistributionId]; !ok {
			ds, err := lc.qdb.GetDistribution(ctx, keyRange.DistributionId)
			if err != nil {
				return nil, err
			}
			colTypes = ds.ColTypes
			dsIdColTypes[ds.ID] = ds.ColTypes
		}
		kRange, err := kr.KeyRangeFromDB(keyRange, colTypes)
		if err != nil {
			return nil, err
		}
		krs = append(krs, kRange)
	}

	return krs, nil
}

func (lc *Coordinator) ListKeyRangeLocks(ctx context.Context) ([]string, error) {
	return lc.qdb.ListLockedKeyRanges(ctx)
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
func (lc *Coordinator) NextRange(ctx context.Context, seqName string, rangeSize uint64) (*qdb.SequenceIdRange, error) {
	return lc.qdb.NextRange(ctx, seqName, rangeSize)
}

// QDB implements meta.EntityMgr.
func (lc *Coordinator) QDB() qdb.QDB {
	return lc.qdb
}

// DCStateKeeper implements meta.EntityMgr.
func (lc *Coordinator) DCStateKeeper() qdb.DCStateKeeper {
	/* this is actually used by router, so we have to provide one */
	return lc.dcs
}

// RedistributeKeyRange implements meta.EntityMgr.
func (lc *Coordinator) RedistributeKeyRange(ctx context.Context, req *kr.RedistributeKeyRange) error {
	panic("unimplemented")
}

// RegisterRouter implements meta.EntityMgr.
func (lc *Coordinator) RegisterRouter(ctx context.Context, r *topology.Router) error {
	return lc.qdb.AddRouter(ctx, qdb.NewRouter(r.Address, r.ID, qdb.OPENED))
}

// DropBalancerTask implements meta.EntityMgr.
func (lc *Coordinator) DropBalancerTask(ctx context.Context) error {
	return lc.qdb.DropBalancerTask(ctx)
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
func (lc *Coordinator) RetryMoveTaskGroup(ctx context.Context, id string) error {
	panic("unimplemented")
}

// StopMoveTaskGroup implements meta.EntityMgr
func (lc *Coordinator) StopMoveTaskGroup(ctx context.Context, id string) error {
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
	if rID == "*" {
		return lc.qdb.DeleteRouterAll(ctx)
	}
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
func (lc *Coordinator) ListMoveTaskGroups(ctx context.Context) (map[string]*tasks.MoveTaskGroup, error) {
	taskGroupsDb, err := lc.qdb.ListTaskGroups(ctx)
	if err != nil {
		return nil, err
	}
	res := map[string]*tasks.MoveTaskGroup{}
	for id, tgDb := range taskGroupsDb {
		task, err := lc.qdb.GetMoveTaskByGroup(ctx, id)
		if err != nil {
			return nil, err
		}
		totalKeys, err := lc.qdb.GetMoveTaskGroupTotalKeys(ctx, id)
		if err != nil {
			return nil, err
		}

		res[id] = tasks.TaskGroupFromDb(id, tgDb, task, totalKeys)
	}
	return res, nil
}

// GetMoveTaskGroup retrieves the MoveTask group from the local coordinator's QDB.
//
// Parameters:
// - ctx (context.Context): the context.Context object for managing the request's lifetime.
//
// Returns:
// - *tasks.MoveTaskGroup: the retrieved task group, or nil if an error occurred.
// - error: an error if the retrieval process fails.
func (lc *Coordinator) GetMoveTaskGroup(ctx context.Context, id string) (*tasks.MoveTaskGroup, error) {
	group, err := lc.qdb.GetMoveTaskGroup(ctx, id)
	if err != nil {
		return nil, err
	}
	task, err := lc.qdb.GetMoveTaskByGroup(ctx, id)
	if err != nil {
		return nil, err
	}
	totalKeys, err := lc.qdb.GetMoveTaskGroupTotalKeys(ctx, id)
	if err != nil {
		return nil, err
	}

	return tasks.TaskGroupFromDb(id, group, task, totalKeys), nil
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

func (qc *Coordinator) ListMoveTasks(ctx context.Context) (map[string]*tasks.MoveTask, error) {
	tasksDB, err := qc.qdb.ListMoveTasks(ctx)
	if err != nil {
		return nil, err
	}
	res := make(map[string]*tasks.MoveTask)
	for id, taskDB := range tasksDB {
		res[id] = tasks.TaskFromDb(taskDB)
	}
	return res, nil
}

func (qc *Coordinator) DropMoveTask(ctx context.Context, id string) error {
	task, err := qc.GetMoveTask(ctx, id)
	if err != nil {
		return err
	}
	status, err := qc.GetTaskGroupStatus(ctx, task.TaskGroupID)
	if err != nil {
		return err
	}
	if status != nil && status.State != tasks.TaskGroupError {
		return fmt.Errorf("cannot remove move task: it's forbidden to remove move tasks when its task group is being executed")
	}
	return qc.qdb.DropMoveTask(ctx, id)
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
func (lc *Coordinator) GetReferenceRelation(ctx context.Context, relName *rfqn.RelationFQN) (*rrelation.ReferenceRelation, error) {
	ret, err := lc.qdb.GetReferenceRelation(ctx, relName)
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
	return kr.KeyRangeFromDB(krDb, ds.ColTypes)
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

	ds, err := qc.qdb.GetDistribution(ctx, distribution)
	if err != nil {
		return nil, err
	}
	krs := make([]*kr.KeyRange, 0, len(keyRanges))
	for _, keyRange := range keyRanges {
		kRange, err := kr.KeyRangeFromDB(keyRange, ds.ColTypes)
		if err != nil {
			return nil, err
		}
		krs = append(krs, kRange)
	}

	return krs, nil
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

	if err := qc.qdb.WriteMoveTaskGroup(ctx, taskGroup.ID, tasks.TaskGroupToDb(taskGroup), taskGroup.TotalKeys, tasks.MoveTaskToDb(taskGroup.CurrentTask)); err != nil {
		spqrlog.Zero.Error().Err(err).Msg("failed to write move task group")
		return err
	}
	return nil
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

// DropMoveTaskGroup removes the task group from the local coordinator's QDB.
//
// Parameters:
// - ctx (context.Context): the context.Context object for managing the request's lifetime.
//
// Returns:
// - error: an error if the removal operation fails.
func (qc *Coordinator) DropMoveTaskGroup(ctx context.Context, id string) error {
	task, err := qc.qdb.GetMoveTaskByGroup(ctx, id)
	if err != nil {
		return err
	}
	if task != nil {
		if err := qc.qdb.DropMoveTask(ctx, task.ID); err != nil {
			return err
		}
	}
	return qc.qdb.DropMoveTaskGroup(ctx, id)
}

func (qc *Coordinator) GetMoveTaskGroupBoundsCache(ctx context.Context, id string) ([][][]byte, int, error) {
	return nil, 0, ErrNotCoordinator
}

// GetTaskGroupStatus gets the status of the task group from coordinator's QDB.
//
// Parameters:
// - ctx (context.Context): the context.Context object for managing the request's lifetime.
// - id  (string):          ID of the task group
//
// Returns:
// - *tasks.MoveTaskGroupStatus: the status of the task group
// - error: an error if the removal operation fails.
func (qc *Coordinator) GetTaskGroupStatus(ctx context.Context, id string) (*tasks.MoveTaskGroupStatus, error) {
	status, err := qc.qdb.GetTaskGroupStatus(ctx, id)
	return tasks.MoveTaskGroupStatusFromDb(status), err
}

// GetAllTaskGroupStatuses gets statuses of all task groups from coordinator's QDB.
//
// Parameters:
// - ctx (context.Context): the context.Context object for managing the request's lifetime.
//
// Returns:
// - map[string]*tasks.MoveTaskGroupStatus: the statuses of the task group by ID
// - error: an error if the removal operation fails.
func (qc *Coordinator) GetAllTaskGroupStatuses(ctx context.Context) (map[string]*tasks.MoveTaskGroupStatus, error) {
	statuses, err := qc.qdb.GetAllTaskGroupStatuses(ctx)
	if err != nil {
		return nil, err
	}
	res := make(map[string]*tasks.MoveTaskGroupStatus)
	for id, status := range statuses {
		res[id] = tasks.MoveTaskGroupStatusFromDb(status)
	}
	return res, nil
}

func (qc *Coordinator) ListRedistributeTasks(ctx context.Context) ([]*tasks.RedistributeTask, error) {
	tasksDb, err := qc.qdb.ListRedistributeTasks(ctx)
	if err != nil {
		return nil, err
	}
	res := make([]*tasks.RedistributeTask, len(tasksDb))
	for i, taskDb := range tasksDb {
		res[i] = tasks.RedistributeTaskFromDB(taskDb)
	}
	return res, nil
}

func (qc *Coordinator) DropRedistributeTask(ctx context.Context, id string) error {
	task, err := qc.qdb.GetRedistributeTask(ctx, id)
	if err != nil {
		return err
	}
	return qc.qdb.DropRedistributeTask(ctx, task)
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

func (qc *Coordinator) CreateDistribution(ctx context.Context, ds *distributions.Distribution) ([]qdb.QdbStatement, error) {
	if len(ds.ColTypes) == 0 && ds.Id != distributions.REPLICATED {
		return nil, fmt.Errorf("empty distributions are disallowed")
	}
	for _, rel := range ds.Relations {
		for colName, SeqName := range rel.ColumnSequenceMapping {

			if err := qc.qdb.CreateSequence(ctx, SeqName, 0); err != nil {
				return nil, err
			}
			qualifiedName := rel.ToRFQN()
			err := qc.qdb.AlterSequenceAttach(ctx, SeqName, &qualifiedName, colName)
			if err != nil {
				return nil, err
			}
		}
	}
	stmts, err := qc.qdb.CreateDistribution(ctx, distributions.DistributionToDB(ds))
	if err != nil {
		return nil, err
	}
	return stmts, nil
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
	ds, err := qc.GetDistribution(ctx, id)
	if err != nil {
		return err
	}
	rel, ok := ds.Relations[relName.RelationName]
	if !ok {
		return fmt.Errorf("relation \"%s\" not found in distribution \"%s\"", relName.RelationName, ds.Id)
	}
	if len(rel.UniqueIndexesByColumn) > 0 {
		return fmt.Errorf("cannot detach relation \"%s\" because there are unique indexes depending on it\nHINT: Use DROP ... CASCADE to drop unique indexes automatically", relName.RelationName)
	}
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
// - []qdb.QdbStatement: qdb statements to apply changes
// - error: An error if the creation encounters any issues.
func (lc *Coordinator) CreateKeyRange(ctx context.Context, kr *kr.KeyRange) ([]qdb.QdbStatement, error) {
	return lc.qdb.CreateKeyRange(ctx, kr.ToDB())
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
		_ = qc.UnlockKeyRange(ctx, keyRangeID)
		return nil, err
	}

	return kr.KeyRangeFromDB(keyRangeDB, ds.ColTypes)
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
	return retry.Do(ctx, retry.NewFibonacci(qdb.LockRetryStep),
		func(ctx context.Context) error {
			return qc.qdb.UnlockKeyRange(ctx, keyRangeID)
		})
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
		if err := distributions.CheckRelationKeys(ds, r); err != nil {
			return err
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
	spqrlog.Zero.Debug().Str("base id", uniteKeyRange.BaseKeyRangeId).Str("appendage id", uniteKeyRange.AppendageKeyRangeId).Msg("unite key ranges")
	krBaseDb, err := lc.qdb.LockKeyRange(ctx, uniteKeyRange.BaseKeyRangeId)
	if err != nil {
		return err
	}

	defer func() {
		// TODO: after convert unite command into etcd transaction we no need in embracing "lock" "unlock".
		// We'll just check existing lock at the start.
		if err := lc.UnlockKeyRange(ctx, uniteKeyRange.BaseKeyRangeId); err != nil {
			spqrlog.Zero.Error().Err(err).Msg("failed to unlock key range in Unite")
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

	krBase, err := kr.KeyRangeFromDB(krBaseDb, ds.ColTypes)
	if err != nil {
		return err
	}
	krAppendage, err := kr.KeyRangeFromDB(krAppendageDb, ds.ColTypes)
	if err != nil {
		return err
	}

	if krBase.ShardID != krAppendage.ShardID {
		return spqrerror.New(spqrerror.SPQR_KEYRANGE_ERROR, "failed to unite key ranges routing different shards")
	}
	if krBase.Distribution != krAppendage.Distribution {
		return spqrerror.New(spqrerror.SPQR_KEYRANGE_ERROR, "failed to unite key ranges of different distributions")
	}
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
	// TODO: move check to meta layer
	if err := meta.ValidateKeyRangeForModify(ctx, lc, krBase); err != nil {
		return err
	}
	if err := lc.qdb.UpdateKeyRange(ctx, krBase.ToDB()); err != nil {
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

	if kRange, err := qc.GetKeyRange(ctx, req.Krid); err == nil {
		ds, err := qc.qdb.GetDistribution(ctx, kRange.Distribution)
		if err != nil {
			return err
		}

		var reqRange *kr.KeyRange
		var cmpRange *kr.KeyRange
		if req.SplitLeft {
			sourceKr, err := qc.GetKeyRange(ctx, req.SourceID)
			if err != nil {
				return err
			}
			cmpRange = sourceKr
			reqRange, err = kr.KeyRangeFromBytes(req.Bound, ds.ColTypes)
			if err != nil {
				return err
			}
		} else {
			cmpRange = kRange
			reqRange, err = kr.KeyRangeFromBytes(req.Bound, ds.ColTypes)
			if err != nil {
				return err
			}
		}
		if kr.CmpRangesEqual(cmpRange.LowerBound, reqRange.LowerBound, ds.ColTypes) {
			// already split, so no-op
			return nil
		}
		return spqrerror.Newf(spqrerror.SPQR_KEYRANGE_ERROR, "key range %v already present in qdb", req.Krid)
	}

	krOldDB, err := qc.qdb.NoWaitLockKeyRange(ctx, req.SourceID)
	if err != nil {
		return err
	}

	defer func() {
		if err := qc.UnlockKeyRange(ctx, req.SourceID); err != nil {
			spqrlog.Zero.Error().Err(err).Msg("failed to unlock key range in Split")
		}
	}()

	ds, err := qc.qdb.GetDistribution(ctx, krOldDB.DistributionId)

	if err != nil {
		return err
	}

	krOld, err := kr.KeyRangeFromDB(krOldDB, ds.ColTypes)
	if err != nil {
		return err
	}

	eph, err := kr.KeyRangeFromBytes(req.Bound, ds.ColTypes)
	if err != nil {
		return err
	}

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

	krTemp, err := kr.KeyRangeFromDB(
		&qdb.KeyRange{
			// fix multidim case
			LowerBound:     req.Bound,
			KeyRangeID:     uuid.NewString(),
			ShardID:        krOld.ShardID,
			DistributionId: krOld.Distribution,
		},
		ds.ColTypes,
	)
	if err != nil {
		return err
	}

	tranMngr := meta.NewTranEntityManager(qc)

	if err := meta.ValidateKeyRangeForCreate(ctx, tranMngr, krTemp); err != nil {
		return err
	}

	if req.SplitLeft {
		krTemp.ID = req.SourceID
		krOld.ID = req.Krid
		if krOld.IsLocked == nil {
			return fmt.Errorf("unexpected nil isLocked value in Split")
		}
		*krOld.IsLocked = false
		err = tranMngr.CreateKeyRange(ctx, krTemp)
		if err != nil {
			return fmt.Errorf("could not update source key range in left key range split: %s", err)
		}
		err = tranMngr.CreateKeyRange(ctx, krOld)
		if err != nil {
			return fmt.Errorf("could not create new key range in left key range split: %s", err)
		}
	} else {
		krTemp.ID = req.Krid
		err = tranMngr.CreateKeyRange(ctx, krTemp)
		if err != nil {
			return fmt.Errorf("could not create new key range in right key range split: %s", err)
		}
	}
	if err = tranMngr.ExecNoTran(ctx); err != nil {
		return spqrerror.Newf(spqrerror.SPQR_KEYRANGE_ERROR, "failed to commit a new key range: %s", err.Error())
	}
	return nil
}

func (lc *Coordinator) ListSequences(ctx context.Context) ([]string, error) {
	return lc.qdb.ListSequences(ctx)
}

func (lc *Coordinator) ListRelationSequences(ctx context.Context, rel *rfqn.RelationFQN) (map[string]string, error) {
	return lc.qdb.GetRelationSequence(ctx, rel)
}

func (lc *Coordinator) GetSequenceRelations(ctx context.Context, seqName string) ([]*rfqn.RelationFQN, error) {
	return lc.qdb.GetSequenceRelations(ctx, seqName)
}

func (lc *Coordinator) AlterSequenceDetachRelation(ctx context.Context, rel *rfqn.RelationFQN) error {
	return lc.qdb.AlterSequenceDetachRelation(ctx, rel)
}

func (lc *Coordinator) ExecNoTran(ctx context.Context, chunk *mtran.MetaTransactionChunk) error {
	qdbStatements, err := transactionChunkToQdbStatements(ctx, lc, chunk)
	if err != nil {
		return err
	}
	return lc.qdb.ExecNoTransaction(ctx, qdbStatements)
}

func (lc *Coordinator) CommitTran(ctx context.Context, transaction *mtran.MetaTransaction) error {
	qdbStatements, err := transactionChunkToQdbStatements(ctx, lc, transaction.Operations)
	if err != nil {
		return err
	}
	qdbTran := qdb.NewTransactionWithCmd(transaction.TransactionId, qdbStatements)
	return lc.qdb.CommitTransaction(ctx, qdbTran)
}

func (lc *Coordinator) BeginTran(ctx context.Context) (*mtran.MetaTransaction, error) {
	if qdbTran, err := qdb.NewTransaction(); err != nil {
		return nil, err
	} else {
		if err := lc.qdb.BeginTransaction(ctx, qdbTran); err != nil {
			return nil, err
		}
		return mtran.NewMetaTransaction(*qdbTran), nil
	}
}

// CreateUniqueIndex implements meta.EntityMgr.
func (lc *Coordinator) CreateUniqueIndex(ctx context.Context, dsId string, idx *distributions.UniqueIndex) error {
	ds, err := lc.GetRelationDistribution(ctx, idx.RelationName)
	if err != nil {
		return err
	}
	if _, ok := ds.UniqueIndexesByID[idx.ID]; ok {
		return fmt.Errorf("unique index with ID \"%s\" already exists", idx.ID)
	}
	rel, ok := ds.Relations[idx.RelationName.RelationName]
	if !ok {
		return fmt.Errorf("no relation \"%s\" found in distribution \"%s\"", idx.RelationName.RelationName, ds.Id)
	}

	/* Current implementation restriction. */
	if len(ds.ColTypes) != 1 {
		return fmt.Errorf("unique indexes are supported only for single-column distributions, but %s has %d", ds.Id, len(ds.ColTypes))
	}

	switch ds.ColTypes[0] {
	/* only UUID or hashed types */
	case qdb.ColumnTypeInteger, qdb.ColumnTypeVarchar:
		return fmt.Errorf("unique indexes are not supported non-hashed distribution")
	}

	/* Is this a problem? */
	for _, col := range idx.Columns {
		if _, ok := rel.UniqueIndexesByColumn[col]; ok {
			return fmt.Errorf("unique index for table \"%s\", column \"%s\" already exists", idx.RelationName.String(), col)
		}
	}
	return lc.qdb.CreateUniqueIndex(ctx, distributions.UniqueIndexToDB(dsId, idx))
}

// DropUniqueIndex implements meta.EntityMgr.
func (lc *Coordinator) DropUniqueIndex(ctx context.Context, idxId string) error {
	return lc.qdb.DropUniqueIndex(ctx, idxId)
}

// ListDistributionIndexes implements meta.EntityMgr.
func (lc *Coordinator) ListDistributionIndexes(ctx context.Context, dsId string) (map[string]*distributions.UniqueIndex, error) {
	idxs, err := lc.qdb.ListUniqueIndexes(ctx)
	if err != nil {
		return nil, err
	}
	res := make(map[string]*distributions.UniqueIndex)
	for id, idx := range idxs {
		if idx.DistributionId == dsId {
			res[id] = distributions.UniqueIndexFromDB(idx)
		}
	}
	return res, nil
}

// ListDistributionIndexes implements meta.EntityMgr.
func (lc *Coordinator) ListUniqueIndexes(ctx context.Context) (map[string]*distributions.UniqueIndex, error) {
	idxs, err := lc.qdb.ListUniqueIndexes(ctx)
	if err != nil {
		return nil, err
	}
	res := make(map[string]*distributions.UniqueIndex)
	for id, idx := range idxs {
		res[id] = distributions.UniqueIndexFromDB(idx)
	}
	return res, nil
}

// ListRelationIndexes implements meta.EntityMgr.
func (lc *Coordinator) ListRelationIndexes(ctx context.Context, relName *rfqn.RelationFQN) (map[string]*distributions.UniqueIndex, error) {
	idxs, err := lc.qdb.ListRelationIndexes(ctx, relName)
	if err != nil {
		return nil, err
	}
	res := make(map[string]*distributions.UniqueIndex)
	for id, idx := range idxs {
		res[id] = distributions.UniqueIndexFromDB(idx)
	}
	return res, nil
}
