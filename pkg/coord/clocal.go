package coord

import (
	"context"
	"fmt"
	"math/rand"
	"sync"

	"github.com/pg-sharding/spqr/pkg/config"
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

type LocalCoordinator struct {
	Coordinator

	mu sync.Mutex

	ColumnMapping map[string]struct{}
	LocalTables   map[string]struct{}

	// shards
	DataShardCfgs  map[string]*config.Shard
	WorldShardCfgs map[string]*config.Shard

	// not extended QDB, since the router does not need to track the installation topology
	qdb qdb.QDB

	cache *cache.SchemaCache
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
	return lc.Coordinator.CreateDistribution(ctx, ds)
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
	return lc.Coordinator.AlterDistributionAttach(ctx, id, rels)
}

// AlterDistributionDetach detaches relation from distribution
func (lc *LocalCoordinator) AlterDistributionDetach(ctx context.Context, id string, relName string) error {
	lc.mu.Lock()
	defer lc.mu.Unlock()

	return lc.Coordinator.AlterDistributionDetach(ctx, id, relName)
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
func (lc *LocalCoordinator) AlterDistributedRelation(ctx context.Context, id string, rel *distributions.DistributedRelation) error {
	lc.mu.Lock()
	defer lc.mu.Unlock()

	ds, err := lc.qdb.GetDistribution(ctx, id)
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
	return lc.Coordinator.GetDistribution(ctx, id)
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
	return lc.Coordinator.GetRelationDistribution(ctx, relation)
}

// TODO : unit tests

// ListDataShards retrieves a list of data shards from the local coordinator.
//
// Parameters:
// - ctx (context.Context): The context.Context object for managing the request's lifetime.
//
// Returns:
// - []*topology.DataShard: A slice of topology.DataShard objects representing the list of data shards.
func (lc *LocalCoordinator) ListDataShards(ctx context.Context) []*topology.DataShard {
	lc.mu.Lock()
	defer lc.mu.Unlock()

	var ret []*topology.DataShard
	for id, cfg := range lc.DataShardCfgs {
		ret = append(ret, topology.NewDataShard(id, cfg))
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
// - []*topology.DataShard: A slice of topology.DataShard objects representing the list of data shards.
// - error: An error if the retrieval operation fails.
func (lc *LocalCoordinator) ListShards(ctx context.Context) ([]*topology.DataShard, error) {
	return lc.Coordinator.ListShards(ctx)
}

// AddWorldShard adds a world shard to the LocalCoordinator.
//
// Parameters:
// - ctx (context.Context): The context.Context object for managing the request's lifetime.
// - ds (*topology.DataShard): The topology.DataShard object representing the world shard to be added.
//
// Returns:
// - error: An error if the addition of the world shard fails.
func (lc *LocalCoordinator) AddWorldShard(ctx context.Context, ds *topology.DataShard) error {
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
func (lc *LocalCoordinator) DataShardsRoutes() []*kr.ShardKey {
	lc.mu.Lock()
	defer lc.mu.Unlock()

	var ret []*kr.ShardKey

	for name := range lc.DataShardCfgs {
		ret = append(ret, &kr.ShardKey{
			Name: name,
			RO:   false,
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
func (lc *LocalCoordinator) WorldShardsRoutes() []*kr.ShardKey {
	lc.mu.Lock()
	defer lc.mu.Unlock()

	var ret []*kr.ShardKey

	for name := range lc.WorldShardCfgs {
		ret = append(ret, &kr.ShardKey{
			Name: name,
			RO:   true,
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
func (lc *LocalCoordinator) AddDataShard(ctx context.Context, ds *topology.DataShard) error {
	spqrlog.Zero.Info().
		Str("node", ds.ID).
		Msg("adding datashard node in local coordinator")

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
	spqrlog.Zero.Debug().Str("address", addr).Msg("resp local coordinator: get coordinator")
	return addr, err
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
func (lc *LocalCoordinator) GetShard(ctx context.Context, shardID string) (*topology.DataShard, error) {
	return nil, ErrNotCoordinator
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

func (lc *LocalCoordinator) Cache() *cache.SchemaCache {
	return lc.cache
}

func (lc *LocalCoordinator) DropSequence(ctx context.Context, seqName string) error {
	return lc.qdb.DropSequence(ctx, seqName)
}

func (lc *LocalCoordinator) NextVal(ctx context.Context, seqName string) (int64, error) {
	coordAddr, err := lc.GetCoordinator(ctx)
	if err != nil {
		return -1, err
	}
	if coordAddr == "" {
		return lc.qdb.NextVal(ctx, seqName)
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

func (lc *LocalCoordinator) CurrVal(ctx context.Context, seqName string) (int64, error) {
	coordAddr, err := lc.GetCoordinator(ctx)
	if err != nil {
		return -1, err
	}
	if coordAddr == "" {
		return lc.qdb.CurrVal(ctx, seqName)
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

func (lc *LocalCoordinator) RetryMoveTaskGroup(_ context.Context) error {
	return ErrNotCoordinator
}

// NewLocalCoordinator creates a new LocalCoordinator instance.
//
// Parameters:
// - db (qdb.QDB): The QDB instance to associate with the LocalCoordinator.
//
// Returns:
// - meta.EntityMgr: The newly created LocalCoordinator instance.
func NewLocalCoordinator(db qdb.QDB, cache *cache.SchemaCache) meta.EntityMgr {
	return &LocalCoordinator{
		Coordinator:    NewCoordinator(db),
		DataShardCfgs:  map[string]*config.Shard{},
		WorldShardCfgs: map[string]*config.Shard{},
		qdb:            db,
		cache:          cache,
	}
}
