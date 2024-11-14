package local

import (
	"context"
	"fmt"
	"math/rand"
	"sync"

	"github.com/pg-sharding/spqr/pkg/models/spqrerror"
	"github.com/pg-sharding/spqr/pkg/models/tasks"

	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/meta"
	"github.com/pg-sharding/spqr/pkg/models/datashards"
	"github.com/pg-sharding/spqr/pkg/models/distributions"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/pkg/models/topology"
	"github.com/pg-sharding/spqr/pkg/spqrlog"
	"github.com/pg-sharding/spqr/qdb"
	"github.com/pg-sharding/spqr/qdb/ops"
	"github.com/pg-sharding/spqr/router/routingstate"
)

type LocalCoordinator struct {
	mu sync.Mutex

	ColumnMapping map[string]struct{}
	LocalTables   map[string]struct{}

	// shards
	DataShardCfgs  map[string]*config.Shard
	WorldShardCfgs map[string]*config.Shard

	// not extended QDB, since the router does not need to track the installation topology
	qdb qdb.QDB
}

// GetMoveTaskGroup retrieves the MoveTask group from the local coordinator's QDB.
//
// Parameters:
// - ctx (context.Context): the context.Context object for managing the request's lifetime.
//
// Returns:
// - *tasks.MoveTaskGroup: the retrieved task group, or nil if an error occurred.
// - error: an error if the retrieval process fails.
func (lc *LocalCoordinator) GetMoveTaskGroup(ctx context.Context) (*tasks.MoveTaskGroup, error) {
	group, err := lc.qdb.GetMoveTaskGroup(ctx)
	if err != nil {
		return nil, err
	}
	return tasks.TaskGroupFromDb(group), nil
}

// WriteMoveTaskGroup writes the given task group to the local coordinator's QDB.
//
// Parameters:
// - ctx (context.Context): the context.Context object for managing the request's lifetime.
// - taskGroup (*tasks.MoveTaskGroup): the task group to be written to the QDB.
//
// Returns:
// - error: an error if the write operation fails.
func (lc *LocalCoordinator) WriteMoveTaskGroup(ctx context.Context, taskGroup *tasks.MoveTaskGroup) error {
	return lc.qdb.WriteMoveTaskGroup(ctx, tasks.TaskGroupToDb(taskGroup))
}

// RemoveMoveTaskGroup removes the task group from the local coordinator's QDB.
//
// Parameters:
// - ctx (context.Context): the context.Context object for managing the request's lifetime.
//
// Returns:
// - error: an error if the removal operation fails.
func (lc *LocalCoordinator) RemoveMoveTaskGroup(ctx context.Context) error {
	return lc.qdb.RemoveMoveTaskGroup(ctx)
}

// GetBalancerTask is disabled in LocalCoordinator
func (lc *LocalCoordinator) GetBalancerTask(context.Context) (*tasks.BalancerTask, error) {
	return nil, ErrNotCoordinator
}

// WriteBalancerTask is disabled in LocalCoordinator
func (lc *LocalCoordinator) WriteBalancerTask(context.Context, *tasks.BalancerTask) error {
	return ErrNotCoordinator
}

// RemoveBalancerTask is disabled in LocalCoordinator
func (lc *LocalCoordinator) RemoveBalancerTask(context.Context) error {
	return ErrNotCoordinator
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
func (lc *LocalCoordinator) ListDistributions(ctx context.Context) ([]*distributions.Distribution, error) {
	lc.mu.Lock()
	defer lc.mu.Unlock()

	resp, err := lc.qdb.ListDistributions(ctx)
	if err != nil {
		return nil, err
	}
	var retDsp []*distributions.Distribution

	for _, dsp := range resp {
		retDsp = append(retDsp, distributions.DistributionFromDB(dsp))
	}
	return retDsp, nil
}

// TODO : unit tests

// CreateDistribution creates a distribution in the local coordinator's QDB.
//
// Parameters:
// - ctx (context.Context): the context.Context object for managing the request's lifetime.
// - ds (*distributions.Distribution): the distributions.Distribution object to be created.
//
// Returns:
// - error: an error if the creation operation fails.
func (lc *LocalCoordinator) CreateDistribution(ctx context.Context, ds *distributions.Distribution) error {
	lc.mu.Lock()
	defer lc.mu.Unlock()
	if len(ds.ColTypes) == 0 {
		return fmt.Errorf("empty distributions are disallowed")
	}
	return lc.qdb.CreateDistribution(ctx, distributions.DistributionToDB(ds))
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
func (lc *LocalCoordinator) AlterDistributionAttach(ctx context.Context, id string, rels []*distributions.DistributedRelation) error {
	lc.mu.Lock()
	defer lc.mu.Unlock()

	ds, err := lc.qdb.GetDistribution(ctx, id)
	if err != nil {
		return err
	}

	dRels := []*qdb.DistributedRelation{}
	for _, r := range rels {
		if len(r.DistributionKey) != len(ds.ColTypes) {
			return fmt.Errorf("cannot attach relation %v to this dataspace: number of column mismatch", r.Name)
		}
		dRels = append(dRels, distributions.DistributedRelationToDB(r))
	}

	return lc.qdb.AlterDistributionAttach(ctx, id, dRels)
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
func (lc *LocalCoordinator) AlterDistributionDetach(ctx context.Context, id string, relName string) error {
	lc.mu.Lock()
	defer lc.mu.Unlock()

	return lc.qdb.AlterDistributionDetach(ctx, id, relName)
}

// TODO : unit tests

// GetDistribution retrieves a distribution from the local coordinator's QDB by its ID.
//
// Parameters:
// - ctx (context.Context): the context.Context object for managing the request's lifetime.
// - id (string): the ID of the distribution to retrieve.
//
// Returns:
// - *distributions.Distribution: a pointer to the distributions.Distribution object representing the retrieved distribution.
// - error: an error if the retrieval operation fails.
func (lc *LocalCoordinator) GetDistribution(ctx context.Context, id string) (*distributions.Distribution, error) {
	lc.mu.Lock()
	defer lc.mu.Unlock()

	ret, err := lc.qdb.GetDistribution(ctx, id)
	if err != nil {
		return nil, err
	}
	return distributions.DistributionFromDB(ret), nil
}

// GetRelationDistribution retrieves a distribution based on the given relation from the local coordinator's QDB.
//
// Parameters:
// - ctx (context.Context): The context.Context object for managing the request's lifetime.
// - relation (string): The name of the relation for which to retrieve the distribution.
//
// Returns:
// - *distributions.Distribution: A pointer to the distributions.Distribution object representing the retrieved distribution.
// - error: An error if the retrieval operation fails.
func (lc *LocalCoordinator) GetRelationDistribution(ctx context.Context, relation string) (*distributions.Distribution, error) {
	lc.mu.Lock()
	defer lc.mu.Unlock()

	ret, err := lc.qdb.GetRelationDistribution(ctx, relation)
	if err != nil {
		return nil, err
	}
	return distributions.DistributionFromDB(ret), nil
}

// TODO : unit tests

// ListDataShards retrieves a list of data shards from the local coordinator.
//
// Parameters:
// - ctx (context.Context): The context.Context object for managing the request's lifetime.
//
// Returns:
// - []*datashards.DataShard: A slice of datashards.DataShard objects representing the list of data shards.
func (lc *LocalCoordinator) ListDataShards(ctx context.Context) []*datashards.DataShard {
	lc.mu.Lock()
	defer lc.mu.Unlock()

	var ret []*datashards.DataShard
	for id, cfg := range lc.DataShardCfgs {
		ret = append(ret, datashards.NewDataShard(id, cfg))
	}
	return ret
}

// TODO : unit tests

// DropDistribution removes a distribution with the specified ID from the local coordinator's QDB.
//
// Parameters:
// - ctx (context.Context): The context.Context object for managing the request's lifetime.
// - id (string): The ID of the distribution to be dropped.
//
// Returns:
// - error: An error if the removal operation fails.
func (lc *LocalCoordinator) DropDistribution(ctx context.Context, id string) error {
	lc.mu.Lock()
	defer lc.mu.Unlock()
	return lc.qdb.DropDistribution(ctx, id)
}

// TODO : unit tests

// ListShards retrieves a list of data shards from the local coordinator's QDB.
//
// Parameters:
// - ctx (context.Context): The context.Context object for managing the request's lifetime.
//
// Returns:
// - []*datashards.DataShard: A slice of datashards.DataShard objects representing the list of data shards.
// - error: An error if the retrieval operation fails.
func (lc *LocalCoordinator) ListShards(ctx context.Context) ([]*datashards.DataShard, error) {
	resp, err := lc.qdb.ListShards(ctx)
	if err != nil {
		return nil, err
	}
	var retShards []*datashards.DataShard

	for _, sh := range resp {
		retShards = append(retShards, &datashards.DataShard{
			ID: sh.ID,
			Cfg: &config.Shard{
				RawHosts: sh.RawHosts,
			},
		})
	}
	return retShards, nil
}

// AddWorldShard adds a world shard to the LocalCoordinator.
//
// Parameters:
// - ctx (context.Context): The context.Context object for managing the request's lifetime.
// - ds (*datashards.DataShard): The datashards.DataShard object representing the world shard to be added.
//
// Returns:
// - error: An error if the addition of the world shard fails.
func (lc *LocalCoordinator) AddWorldShard(ctx context.Context, ds *datashards.DataShard) error {
	lc.mu.Lock()
	defer lc.mu.Unlock()

	spqrlog.Zero.Info().
		Str("shard", ds.ID).
		Msg("adding world datashard")
	lc.WorldShardCfgs[ds.ID] = ds.Cfg

	return nil
}

// DropShard drops a shard from the LocalCoordinator.
//
// Parameters:
// - ctx (context.Context): The context.Context object for managing the request's lifetime.
// - shardId (string): The ID of the shard to be dropped.
//
// Returns:
// - error: An error if the dropping of the shard fails.
func (lc *LocalCoordinator) DropShard(ctx context.Context, shardId string) error {
	lc.mu.Lock()
	defer lc.mu.Unlock()

	delete(lc.DataShardCfgs, shardId)
	delete(lc.WorldShardCfgs, shardId)

	return lc.qdb.DropShard(ctx, shardId)
}

// TODO : unit tests

// DropKeyRange drops a key range from the LocalCoordinator.
//
// Parameters:
// - ctx (context.Context): The context.Context object for managing the request's lifetime.
// - id (string): The ID of the key range to be dropped.
//
// Returns:
// - error: An error if the dropping of the key range fails.
func (lc *LocalCoordinator) DropKeyRange(ctx context.Context, id string) error {
	lc.mu.Lock()
	defer lc.mu.Unlock()

	spqrlog.Zero.Info().
		Str("kr", id).
		Msg("dropping key range")
	return lc.qdb.DropKeyRange(ctx, id)
}

// TODO : unit tests

// DropKeyRangeAll drops all key ranges from the LocalCoordinator.
//
// Parameters:
// - ctx (context.Context): The context.Context object for managing the request's lifetime.
//
// Returns:
// - error: An error if the dropping of all key ranges fails.
func (lc *LocalCoordinator) DropKeyRangeAll(ctx context.Context) error {
	lc.mu.Lock()
	defer lc.mu.Unlock()

	spqrlog.Zero.Info().Msg("dropping all key ranges")
	return lc.qdb.DropKeyRangeAll(ctx)
}

// TODO : unit tests

// DataShardsRoutes returns a slice of DataShardRoute objects representing the data shards routes.
//
// Parameters:
// - None
//
// Returns:
// - []*routingstate.DataShardRoute: A slice of DataShardRoute objects representing the data shards routes.
func (lc *LocalCoordinator) DataShardsRoutes() []*routingstate.DataShardRoute {
	lc.mu.Lock()
	defer lc.mu.Unlock()

	var ret []*routingstate.DataShardRoute

	for name := range lc.DataShardCfgs {
		ret = append(ret, &routingstate.DataShardRoute{
			Shkey: kr.ShardKey{
				Name: name,
				RW:   true,
			},
		})
	}

	return ret
}

// TODO : unit tests

// WorldShardsRoutes returns a slice of DataShardRoute objects representing the world shards routes in a round-robin fashion.
//
// Parameters:
// - None
//
// Returns:
// - []*routingstate.DataShardRoute: A slice of DataShardRoute objects representing the world shards routes after applying round-robin.
func (lc *LocalCoordinator) WorldShardsRoutes() []*routingstate.DataShardRoute {
	lc.mu.Lock()
	defer lc.mu.Unlock()

	var ret []*routingstate.DataShardRoute

	for name := range lc.WorldShardCfgs {
		ret = append(ret, &routingstate.DataShardRoute{
			Shkey: kr.ShardKey{
				Name: name,
				RW:   true,
			},
		})
	}

	// a sort of round robin

	rand.Shuffle(len(ret), func(i, j int) {
		ret[i], ret[j] = ret[j], ret[i]
	})
	return ret
}

// WorldShards returns a slice of strings containing the names of the world shards
// stored in the LocalCoordinator.
//
// Parameters:
// - None.
//
// Returns:
// - []string: a slice of strings containing the names of the world shards.
func (lc *LocalCoordinator) WorldShards() []string {
	lc.mu.Lock()
	defer lc.mu.Unlock()

	var ret []string

	for name := range lc.WorldShardCfgs {
		ret = append(ret, name)
	}

	return ret
}

// Caller should lock key range
// TODO : unit tests

// Move moves a key range identified by req.Krid to a new shard specified by req.ShardId
//
// Parameters:
// - ctx (context.Context): The context.Context object for managing the request's lifetime.
// - req (*kr.MoveKeyRange): a pointer to a MoveKeyRange object containing the necessary information for the move operation.
//
// Returns:
// - error: an error if the move operation encounters any issues.
func (lc *LocalCoordinator) Move(ctx context.Context, req *kr.MoveKeyRange) error {
	var krmv *qdb.KeyRange
	var err error
	if krmv, err = lc.qdb.CheckLockedKeyRange(ctx, req.Krid); err != nil {
		return err
	}

	ds, err := lc.qdb.GetDistribution(ctx, krmv.DistributionId)
	if err != nil {
		return err
	}

	var reqKr = kr.KeyRangeFromDB(krmv, ds.ColTypes)
	reqKr.ShardID = req.ShardId
	return ops.ModifyKeyRangeWithChecks(ctx, lc.qdb, reqKr)
}

// BatchMoveKeyRange is disabled in LocalCoordinator
func (lc *LocalCoordinator) BatchMoveKeyRange(_ context.Context, _ *kr.BatchMoveKeyRange) error {
	return ErrNotCoordinator
}

// RedistributeKeyRange is disabled in LocalCoordinator
func (lc *LocalCoordinator) RedistributeKeyRange(_ context.Context, _ *kr.RedistributeKeyRange) error {
	return ErrNotCoordinator
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
func (lc *LocalCoordinator) Unite(ctx context.Context, req *kr.UniteKeyRange) error {
	var krBase *qdb.KeyRange
	var krAppendage *qdb.KeyRange
	var err error

	if krBase, err = lc.qdb.LockKeyRange(ctx, req.BaseKeyRangeId); err != nil { //nolint:all TODO
		return err
	}

	defer func(qdb qdb.QDB, ctx context.Context, keyRangeID string) {
		err := qdb.UnlockKeyRange(ctx, keyRangeID)
		if err != nil {
			spqrlog.Zero.Error().Err(err).Msg("")
			return
		}
	}(lc.qdb, ctx, req.BaseKeyRangeId)

	ds, err := lc.qdb.GetDistribution(ctx, krBase.DistributionId)
	if err != nil {
		return err
	}

	// TODO: krRight seems to be empty.
	if krAppendage, err = lc.qdb.GetKeyRange(ctx, req.AppendageKeyRangeId); err != nil {
		return err
	}

	if err = lc.qdb.DropKeyRange(ctx, krAppendage.KeyRangeID); err != nil {
		return err
	}

	newBound := krBase.LowerBound
	if kr.CmpRangesLess(kr.KeyRangeFromDB(krAppendage, ds.ColTypes).LowerBound, kr.KeyRangeFromDB(krBase, ds.ColTypes).LowerBound, ds.ColTypes) {
		newBound = krAppendage.LowerBound
	}

	krBaseCopy := krBase
	krBaseCopy.LowerBound = newBound
	united := kr.KeyRangeFromDB(krBaseCopy, ds.ColTypes)

	return ops.ModifyKeyRangeWithChecks(ctx, lc.qdb, united)
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
func (lc *LocalCoordinator) Split(ctx context.Context, req *kr.SplitKeyRange) error {
	var krOld *qdb.KeyRange
	var err error

	spqrlog.Zero.Debug().
		Str("krid", req.Krid).
		Interface("bound", req.Bound).
		Str("source-id", req.SourceID).
		Msg("split request is")

	if krOld, err = lc.qdb.LockKeyRange(ctx, req.SourceID); err != nil {
		return err
	}

	defer func() {
		if err := lc.qdb.UnlockKeyRange(ctx, req.SourceID); err != nil {
			spqrlog.Zero.Error().Err(err).Msg("")
		}
	}()

	ds, err := lc.qdb.GetDistribution(ctx, krOld.DistributionId)
	if err != nil {
		return err
	}

	krNew := kr.KeyRangeFromDB(
		&qdb.KeyRange{
			LowerBound: func() [][]byte {
				if req.SplitLeft {
					return krOld.LowerBound
				}
				return req.Bound // fix multidim case !
			}(),
			KeyRangeID:     req.Krid,
			ShardID:        krOld.ShardID,
			DistributionId: krOld.DistributionId,
		},
		ds.ColTypes,
	)

	spqrlog.Zero.Debug().
		Bytes("lower-bound", krNew.Raw()[0]).
		Str("shard-id", krNew.ShardID).
		Str("id", krNew.ID).
		Msg("new key range")

	if req.SplitLeft {
		krOld.LowerBound = req.Bound // TODO: fix
	}

	if err := ops.ModifyKeyRangeWithChecks(ctx, lc.qdb, kr.KeyRangeFromDB(krOld, ds.ColTypes)); err != nil {
		return err
	}

	if err := ops.CreateKeyRangeWithChecks(ctx, lc.qdb, krNew); err != nil {
		return fmt.Errorf("failed to add a new key range: %w", err)
	}

	return nil
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
func (lc *LocalCoordinator) LockKeyRange(ctx context.Context, krid string) (*kr.KeyRange, error) {
	keyRangeDB, err := lc.qdb.LockKeyRange(ctx, krid)
	if err != nil {
		return nil, err
	}

	ds, err := lc.qdb.GetDistribution(ctx, keyRangeDB.DistributionId)
	if err != nil {
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
func (lc *LocalCoordinator) UnlockKeyRange(ctx context.Context, krid string) error {
	return lc.qdb.UnlockKeyRange(ctx, krid)
}

// RenameKeyRange is disabled in LocalCoordinator
func (lc *LocalCoordinator) RenameKeyRange(ctx context.Context, krId, krIdNew string) error {
	return lc.qdb.RenameKeyRange(ctx, krId, krIdNew)
}

// TODO : unit tests

// AddDataShard adds a new data shard to the LocalCoordinator.
//
// Parameters:
// - ctx (context.Context): The context.Context object for managing the request's lifetime.
// - ds: a pointer to a DataShard object containing the information of the data shard to be added.
//
// Returns:
// - error: an error if the operation encounters any issues.
func (lc *LocalCoordinator) AddDataShard(ctx context.Context, ds *datashards.DataShard) error {
	spqrlog.Zero.Info().
		Str("node", ds.ID).
		Msg("adding node")

	lc.DataShardCfgs[ds.ID] = ds.Cfg

	return lc.qdb.AddShard(ctx, &qdb.Shard{
		ID:       ds.ID,
		RawHosts: ds.Cfg.RawHosts,
	})
}

// TODO : unit tests

// Shards returns a slice of strings containing the names of the data shards stored in the LocalCoordinator.
//
// Parameters:
// - None.
//
// Returns:
// - []string: a slice of strings containing the names of the data shards.
func (lc *LocalCoordinator) Shards() []string {
	var ret []string

	for name := range lc.DataShardCfgs {
		ret = append(ret, name)
	}

	return ret
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
func (lc *LocalCoordinator) GetKeyRange(ctx context.Context, krId string) (*kr.KeyRange, error) {
	krDb, err := lc.qdb.GetKeyRange(ctx, krId)
	if err != nil {
		return nil, err
	}
	ds, err := lc.qdb.GetDistribution(ctx, krDb.DistributionId)
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
func (lc *LocalCoordinator) ListKeyRanges(ctx context.Context, distribution string) ([]*kr.KeyRange, error) {
	var ret []*kr.KeyRange
	if krs, err := lc.qdb.ListKeyRanges(ctx, distribution); err != nil {
		return nil, err
	} else {
		for _, keyRange := range krs {
			ds, err := lc.qdb.GetDistribution(ctx, keyRange.DistributionId)

			if err != nil {
				return nil, err
			}

			ret = append(ret, kr.KeyRangeFromDB(keyRange, ds.ColTypes))
		}
	}

	return ret, nil
}

// TODO : unit tests

// ListAllKeyRanges retrieves a list of all key ranges stored in the LocalCoordinator.
//
// Parameters:
// - ctx: the context of the operation.
//
// Returns:
// - []*kr.KeyRange: a slice of KeyRange objects representing all key ranges.
// - error: an error if the retrieval encounters any issues.
func (lc *LocalCoordinator) ListAllKeyRanges(ctx context.Context) ([]*kr.KeyRange, error) {
	if krs, err := lc.qdb.ListAllKeyRanges(ctx); err != nil {
		return nil, err
	} else {
		var ret []*kr.KeyRange
		cache := map[string]*qdb.Distribution{}

		for _, keyRange := range krs {
			var ds *qdb.Distribution
			var err error
			var ok bool
			if ds, ok = cache[keyRange.DistributionId]; !ok {
				ds, err = lc.qdb.GetDistribution(ctx, keyRange.DistributionId)
				if err != nil {
					return nil, err
				}
				cache[keyRange.DistributionId] = ds
			}

			ret = append(ret, kr.KeyRangeFromDB(keyRange, ds.ColTypes))
		}
		return ret, nil
	}
}

// TODO : unit tests

// ListRouters retrieves a list of routers stored in the LocalCoordinator.
//
// Parameters:
// - ctx: the context of the operation.
//
// Returns:
// - []*topology.Router: a slice of Router objects representing all routers.
// - error: an error if the retrieval encounters any issues.
func (lc *LocalCoordinator) ListRouters(ctx context.Context) ([]*topology.Router, error) {
	return []*topology.Router{{
		ID: "local",
	}}, nil
}

// CreateKeyRange creates a new key range in the LocalCoordinator.
//
// Parameters:
// - ctx (context.Context): The context of the operation.
// - kr (*kr.KeyRange): The key range object to be created.
//
// Returns:
// - error: An error if the creation encounters any issues.
func (lc *LocalCoordinator) CreateKeyRange(ctx context.Context, kr *kr.KeyRange) error {
	return ops.CreateKeyRangeWithChecks(ctx, lc.qdb, kr)
}

// MoveKeyRange is disabled in LocalCoordinator
//
// Returns:
// - error: SPQR_INVALID_REQUEST error
func (lc *LocalCoordinator) MoveKeyRange(_ context.Context, _ *kr.KeyRange) error {
	return spqrerror.New(spqrerror.SPQR_INVALID_REQUEST, "MoveKeyRange is not available in local coordinator")
}

var ErrNotCoordinator = fmt.Errorf("request is unprocessable in router")

// RegisterRouter registers a router in the local coordinator.
//
// Parameters:
// - ctx (context.Context): The context of the operation.
// - r (*topology.Router): The router to be registered.
//
// Returns:
// - error: An error indicating the registration status.
func (lc *LocalCoordinator) RegisterRouter(ctx context.Context, r *topology.Router) error {
	return ErrNotCoordinator
}

// UnregisterRouter unregisters a router in the local coordinator.
//
// Parameters:
// - ctx (context.Context): The context of the operation.
// - id (string): The ID of the router to be unregistered.
//
// Returns:
// - error: An error indicating the unregistration status.
func (lc *LocalCoordinator) UnregisterRouter(ctx context.Context, id string) error {
	return ErrNotCoordinator
}

// SyncRouterMetadata synchronizes the metadata of a router in the local coordinator.
//
// Parameters:
// - ctx (context.Context): The context of the operation.
// - router (*topology.Router): The router whose metadata needs to be synchronized.
//
// Returns:
// - error: An error indicating the synchronization status. In this case, it returns ErrNotCoordinator.
func (lc *LocalCoordinator) SyncRouterMetadata(ctx context.Context, router *topology.Router) error {
	return ErrNotCoordinator
}

// SyncRouterCoordinatorAddress updates the coordinator address for the specified router.
//
// Parameters:
// - ctx (context.Context): The context of the operation.
// - router (*topology.Router): The router for which the coordinator address needs to be updated.
//
// Returns:
// - error: An error indicating the update status.
func (lc *LocalCoordinator) SyncRouterCoordinatorAddress(ctx context.Context, router *topology.Router) error {
	return ErrNotCoordinator
}

// UpdateCoordinator updates the coordinator address in the local coordinator.
//
// Parameters:
// - ctx (context.Context): The context of the operation.
// - addr (string): The new address of the coordinator.
//
// Returns:
// - error: An error indicating the update status.
func (lc *LocalCoordinator) UpdateCoordinator(ctx context.Context, addr string) error {
	return lc.qdb.UpdateCoordinator(ctx, addr)
}

// GetCoordinator retrieves the coordinator address from the local coordinator.
//
// Parameters:
// - ctx (context.Context): The context of the operation.
//
// Returns:
// - string: The address of the coordinator.
// - error: An error indicating the retrieval status.
func (lc *LocalCoordinator) GetCoordinator(ctx context.Context) (string, error) {
	addr, err := lc.qdb.GetCoordinator(ctx)
	spqrlog.Zero.Debug().Str("address", addr).Msg("resp local coordiantor: get coordinator")
	return addr, err
}

// GetShard retrieves a DataShard by its ID from the LocalCoordinator.
//
// Parameters:
// - ctx (context.Context): The context of the operation.
// - shardID (string): The ID of the DataShard to retrieve.
//
// Returns:
// - *datashards.DataShard: The retrieved DataShard, or nil if it doesn't exist.
// - error: An error indicating the retrieval status, or ErrNotCoordinator if the operation is not supported by the LocalCoordinator.
func (lc *LocalCoordinator) GetShard(ctx context.Context, shardID string) (*datashards.DataShard, error) {
	return nil, ErrNotCoordinator
}

// ShareKeyRange shares a key range with the LocalCoordinator.
//
// Parameters:
// - id (string): The ID of the key range to be shared.
//
// Returns:
// - error: An error indicating the sharing status.
func (lc *LocalCoordinator) ShareKeyRange(id string) error {
	return lc.qdb.ShareKeyRange(id)
}

// QDB returns the QDB instance associated with the LocalCoordinator.
//
// Parameters:
// - None.
//
// Returns:
// - qdb.QDB: The QDB instance.
func (lc *LocalCoordinator) QDB() qdb.QDB {
	return lc.qdb
}

// NewLocalCoordinator creates a new LocalCoordinator instance.
//
// Parameters:
// - db (qdb.QDB): The QDB instance to associate with the LocalCoordinator.
//
// Returns:
// - meta.EntityMgr: The newly created LocalCoordinator instance.
func NewLocalCoordinator(db qdb.QDB) meta.EntityMgr {
	return &LocalCoordinator{
		DataShardCfgs:  map[string]*config.Shard{},
		WorldShardCfgs: map[string]*config.Shard{},
		qdb:            db,
	}
}
