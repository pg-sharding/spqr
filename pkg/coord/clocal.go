package coord

import (
	"context"
	"fmt"

	"github.com/pg-sharding/spqr/pkg/meta"
	"github.com/pg-sharding/spqr/pkg/models/distributions"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/pkg/models/spqrerror"
	"github.com/pg-sharding/spqr/pkg/models/tasks"
	"github.com/pg-sharding/spqr/pkg/models/topology"
	"github.com/pg-sharding/spqr/pkg/spqrlog"
	"github.com/pg-sharding/spqr/qdb"
	"github.com/pg-sharding/spqr/qdb/ops"
	"github.com/pg-sharding/spqr/router/cache"
	"google.golang.org/grpc"
)

type LocalInstanceMetadataMgr struct {
	Coordinator

	cache *cache.SchemaCache
}

// GetBalancerTask is disabled in LocalCoordinator
func (lc *LocalInstanceMetadataMgr) GetBalancerTask(context.Context) (*tasks.BalancerTask, error) {
	return nil, ErrNotCoordinator
}

// WriteBalancerTask is disabled in LocalCoordinator
func (lc *LocalInstanceMetadataMgr) WriteBalancerTask(context.Context, *tasks.BalancerTask) error {
	return ErrNotCoordinator
}

// RemoveBalancerTask is disabled in LocalCoordinator
func (lc *LocalInstanceMetadataMgr) RemoveBalancerTask(context.Context) error {
	return ErrNotCoordinator
}

// TODO : unit tests

// AlterDistributedRelation alters metadata of a distributed relation.
//
// Parameters:
// - ctx (context.Context): the context.Context object for managing the request's lifetime.
// - id (string): the ID of the distribution of the relation.
// - rel (*distributions.DistributedRelation): the metadata of the distributed relation.
//
// Returns:
// - error: an error if the alteration operation fails.
func (lc *LocalInstanceMetadataMgr) AlterDistributedRelation(ctx context.Context, id string, rel *distributions.DistributedRelation) error {
	ds, err := lc.Coordinator.QDB().GetDistribution(ctx, id)
	if err != nil {
		return err
	}

	if ds.ID != distributions.REPLICATED && len(rel.DistributionKey) != len(ds.ColTypes) {
		return fmt.Errorf("cannot attach relation %v to distribution %v: number of column mismatch", rel.Name, ds.ID)
	}
	if !rel.ReplicatedRelation && len(rel.ColumnSequenceMapping) > 0 {
		return spqrerror.Newf(spqrerror.SPQR_INVALID_REQUEST, "sequence are supported for replicated relations only")
	}

	err = lc.qdb.AlterDistributedRelation(ctx, id, distributions.DistributedRelationToDB(rel))
	if err != nil {
		return err
	}

	for colName, SeqName := range rel.ColumnSequenceMapping {
		if err := lc.qdb.CreateSequence(ctx, SeqName, 0); err != nil {
			return err
		}

		if err := lc.qdb.AlterSequenceAttach(ctx, SeqName, rel.Name, colName); err != nil {
			return err
		}
	}
	return nil
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
func (lc *LocalInstanceMetadataMgr) DropDistribution(ctx context.Context, id string) error {
	return lc.Coordinator.DropDistribution(ctx, id)
}

// AddWorldShard adds a world shard to the LocalCoordinator.
//
// Parameters:
// - ctx (context.Context): The context.Context object for managing the request's lifetime.
// - ds (*topology.DataShard): The topology.DataShard object representing the world shard to be added.
//
// Returns:
// - error: An error if the addition of the world shard fails.
func (lc *LocalInstanceMetadataMgr) AddWorldShard(ctx context.Context, ds *topology.DataShard) error {
	spqrlog.Zero.Info().
		Str("shard", ds.ID).
		Msg("adding world datashard, noop")

	return nil
}

// TODO : unit tests

// DataShardsRoutes returns a slice of DataShardRoute objects representing the data shards routes.
//
// Parameters:
// - None
//
// Returns:
// - []*routingstate.DataShardRoute: A slice of DataShardRoute objects representing the data shards routes.
func (lc *LocalInstanceMetadataMgr) DataShardsRoutes() []*kr.ShardKey {
	var ret []*kr.ShardKey

	/* currently, all shards are data shards */
	if shs, err := lc.qdb.ListShards(context.TODO()); err != nil {
		for _, sh := range shs {
			ret = append(ret, &kr.ShardKey{
				Name: sh.ID,
				RO:   false,
			})
		}
	} else {
		panic(err)
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
func (lc *LocalInstanceMetadataMgr) WorldShardsRoutes() []*kr.ShardKey {
	/*  XXX: world shard is not implemented */
	return nil
}

// WorldShards returns a slice of strings containing the names of the world shards
// stored in the LocalCoordinator.
//
// Parameters:
// - None.
//
// Returns:
// - []string: a slice of strings containing the names of the world shards.
func (lc *LocalInstanceMetadataMgr) WorldShards() []string {
	return nil
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
func (lc *LocalInstanceMetadataMgr) Move(ctx context.Context, req *kr.MoveKeyRange) error {
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
func (lc *LocalInstanceMetadataMgr) BatchMoveKeyRange(_ context.Context, _ *kr.BatchMoveKeyRange) error {
	return ErrNotCoordinator
}

// RedistributeKeyRange is disabled in LocalCoordinator
func (lc *LocalInstanceMetadataMgr) RedistributeKeyRange(_ context.Context, _ *kr.RedistributeKeyRange) error {
	return ErrNotCoordinator
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
func (lc *LocalInstanceMetadataMgr) AddDataShard(ctx context.Context, ds *topology.DataShard) error {
	spqrlog.Zero.Info().
		Str("node", ds.ID).
		Msg("adding datashard node in local coordinator")

	return lc.qdb.AddShard(ctx, &qdb.Shard{
		ID:       ds.ID,
		RawHosts: ds.Cfg.RawHosts,
	})
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
func (lc *LocalInstanceMetadataMgr) ListAllKeyRanges(ctx context.Context) ([]*kr.KeyRange, error) {
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
func (lc *LocalInstanceMetadataMgr) ListRouters(ctx context.Context) ([]*topology.Router, error) {
	return []*topology.Router{{
		ID: "local",
	}}, nil
}

// MoveKeyRange is disabled in LocalCoordinator
//
// Returns:
// - error: SPQR_INVALID_REQUEST error
func (lc *LocalInstanceMetadataMgr) MoveKeyRange(_ context.Context, _ *kr.KeyRange) error {
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
func (lc *LocalInstanceMetadataMgr) RegisterRouter(ctx context.Context, r *topology.Router) error {
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
func (lc *LocalInstanceMetadataMgr) UnregisterRouter(ctx context.Context, id string) error {
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
func (lc *LocalInstanceMetadataMgr) SyncRouterMetadata(ctx context.Context, router *topology.Router) error {
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
func (lc *LocalInstanceMetadataMgr) SyncRouterCoordinatorAddress(ctx context.Context, router *topology.Router) error {
	return ErrNotCoordinator
}

// GetShard retrieves a DataShard by its ID from the LocalCoordinator.
//
// Parameters:
// - ctx (context.Context): The context of the operation.
// - shardID (string): The ID of the DataShard to retrieve.
//
// Returns:
// - *topology.DataShard: The retrieved DataShard, or nil if it doesn't exist.
// - error: An error indicating the retrieval status, or ErrNotCoordinator if the operation is not supported by the LocalCoordinator.
func (lc *LocalInstanceMetadataMgr) GetShard(ctx context.Context, shardID string) (*topology.DataShard, error) {
	if lc.qdb != nil {
		sh, err := lc.qdb.GetShard(ctx, shardID)
		if err != nil {
			return nil, err
		}
		return topology.DataShardFromDB(sh), nil
	}
	return nil, ErrNotCoordinator

}

func (lc *LocalInstanceMetadataMgr) Cache() *cache.SchemaCache {
	return lc.cache
}

func (lc *LocalInstanceMetadataMgr) NextVal(ctx context.Context, seqName string) (int64, error) {
	coordAddr, err := lc.GetCoordinator(ctx)
	if err != nil {
		return -1, err
	}
	if coordAddr == "" {
		return lc.Coordinator.QDB().NextVal(ctx, seqName)
	}
	conn, err := grpc.NewClient(coordAddr, grpc.WithInsecure()) //nolint:all
	if err != nil {
		return -1, err
	}
	defer func() {
		if err := conn.Close(); err != nil {
			spqrlog.Zero.Debug().Err(err).Msg("failed to close connection")
		}
	}()
	mgr := NewAdapter(conn)
	return mgr.NextVal(ctx, seqName)
}

func (lc *LocalInstanceMetadataMgr) CurrVal(ctx context.Context, seqName string) (int64, error) {
	coordAddr, err := lc.GetCoordinator(ctx)
	if err != nil {
		return -1, err
	}
	if coordAddr == "" {
		return lc.Coordinator.QDB().CurrVal(ctx, seqName)
	}
	conn, err := grpc.NewClient(coordAddr, grpc.WithInsecure()) //nolint:all
	if err != nil {
		return -1, err
	}
	defer func() {
		if err := conn.Close(); err != nil {
			spqrlog.Zero.Debug().Err(err).Msg("failed to close connection")
		}
	}()
	mgr := NewAdapter(conn)
	return mgr.CurrVal(ctx, seqName)
}

func (lc *LocalInstanceMetadataMgr) RetryMoveTaskGroup(_ context.Context) error {
	return ErrNotCoordinator
}

// NewLocalInstanceMetadataMgr creates a new LocalCoordinator instance.
//
// Parameters:
// - db (qdb.QDB): The QDB instance to associate with the LocalCoordinator.
//
// Returns:
// - meta.EntityMgr: The newly created LocalCoordinator instance.
func NewLocalInstanceMetadataMgr(db qdb.XQDB, cache *cache.SchemaCache) meta.EntityMgr {
	return &LocalInstanceMetadataMgr{
		Coordinator: NewCoordinator(db),
		cache:       cache,
	}
}
