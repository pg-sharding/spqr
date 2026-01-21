package coord

import (
	"context"
	"fmt"

	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/meta"
	"github.com/pg-sharding/spqr/pkg/models/distributions"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/pkg/models/rrelation"
	"github.com/pg-sharding/spqr/pkg/models/spqrerror"
	"github.com/pg-sharding/spqr/pkg/models/tasks"
	"github.com/pg-sharding/spqr/pkg/models/topology"
	mtran "github.com/pg-sharding/spqr/pkg/models/transaction"
	proto "github.com/pg-sharding/spqr/pkg/protos"
	"github.com/pg-sharding/spqr/qdb"
	"github.com/pg-sharding/spqr/router/cache"
	"github.com/pg-sharding/spqr/router/rfqn"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
)

type Adapter struct {
	conn *grpc.ClientConn
}

var _ meta.EntityMgr = &Adapter{}

// NewAdapter creates a new instance of the Adapter struct.
//
// Parameters:
// - conn (*grpc.ClientConn): a pointer to a grpc.ClientConn object representing the gRPC client connection.
//
// Returns:
// - a pointer to an Adapter object.
func NewAdapter(conn *grpc.ClientConn) *Adapter {
	return &Adapter{
		conn: conn,
	}
}

// QDB returns the QDB object associated with the Adapter.

// Parameters:
// - None.
//
// Returns:
// - qdb.QDB: The QDB object.
func (a *Adapter) QDB() qdb.QDB {
	panic("Adapter.QDB not implemented")
}

func (a *Adapter) DCStateKeeper() qdb.DCStateKeeper {
	panic("Adapter.DCStateKeeper not implemented")
}

func (a *Adapter) Cache() *cache.SchemaCache {
	panic("Adapter.Cache not implemented")
}

// TODO : unit tests
// TODO : implement

// ShareKeyRange shares the key range for the given ID.
//
// Parameters:
// - id (string): The ID of the key range to be shared.
//
// Returns:
// - error: An error indicating if the key range sharing was successful or not.
func (a *Adapter) ShareKeyRange(id string) error {
	return spqrerror.New(spqrerror.SPQR_NOT_IMPLEMENTED, "ShareKeyRange not implemented")
}

// GetReferenceRelation implements meta.EntityMgr.
func (a *Adapter) GetReferenceRelation(ctx context.Context, relName *rfqn.RelationFQN) (*rrelation.ReferenceRelation, error) {
	return nil, spqrerror.New(spqrerror.SPQR_NOT_IMPLEMENTED, "GetReferenceRelation not implemented")
}

// GetSequenceColumns implements meta.EntityMgr.
func (a *Adapter) GetSequenceColumns(ctx context.Context, seqName string) ([]string, error) {
	return nil, spqrerror.New(spqrerror.SPQR_NOT_IMPLEMENTED, "GetSequenceColumns not implemented")
}

// GetSequenceColumns implements meta.EntityMgr.
func (a *Adapter) GetSequenceRelations(ctx context.Context, seqName string) ([]*rfqn.RelationFQN, error) {
	return nil, spqrerror.New(spqrerror.SPQR_NOT_IMPLEMENTED, "GetSequenceRelations not implemented")
}

// SyncReferenceRelations implements meta.EntityMgr.
func (a *Adapter) SyncReferenceRelations(ctx context.Context, ids []*rfqn.RelationFQN, destShard string) error {
	c := proto.NewReferenceRelationsServiceClient(a.conn)

	qRels := []*proto.QualifiedName{}
	for _, r := range ids {
		qRels = append(qRels, rfqn.RelationFQNToProto(r))
	}

	_, err := c.SyncReferenceRelations(ctx, &proto.SyncReferenceRelationsRequest{
		Relations: qRels,
		ShardId:   destShard,
	})
	return spqrerror.CleanGrpcError(err)
}

// AlterReferenceRelationStorage implements meta.EntityMgr.
func (a *Adapter) AlterReferenceRelationStorage(ctx context.Context, relName *rfqn.RelationFQN, shs []string) error {
	return fmt.Errorf("AlterReferenceRelationStorage should not be used in proxy adapter")
}

// CreateReferenceRelation implements meta.EntityMgr.
func (a *Adapter) CreateReferenceRelation(ctx context.Context, r *rrelation.ReferenceRelation, entry []*rrelation.AutoIncrementEntry) error {
	c := proto.NewReferenceRelationsServiceClient(a.conn)
	_, err := c.CreateReferenceRelations(ctx, &proto.CreateReferenceRelationsRequest{
		Relation: rrelation.RefRelationToProto(r),
		Entries:  rrelation.AutoIncrementEntriesToProto(entry),
	})
	return spqrerror.CleanGrpcError(err)
}

// DropReferenceRelation implements meta.EntityMgr.
func (a *Adapter) DropReferenceRelation(ctx context.Context, relName *rfqn.RelationFQN) error {
	/* XXX: fix protos to new schema */
	c := proto.NewReferenceRelationsServiceClient(a.conn)
	_, err := c.DropReferenceRelations(ctx, &proto.DropReferenceRelationsRequest{
		Relations: []*proto.QualifiedName{
			rfqn.RelationFQNToProto(relName),
		},
	})
	return spqrerror.CleanGrpcError(err)
}

// ListReferenceRelations implements meta.EntityMgr.
func (a *Adapter) ListReferenceRelations(ctx context.Context) ([]*rrelation.ReferenceRelation, error) {
	var res []*rrelation.ReferenceRelation
	c := proto.NewReferenceRelationsServiceClient(a.conn)
	ret, err := c.ListReferenceRelations(ctx, &emptypb.Empty{})
	if err != nil {
		return nil, err
	}

	for _, r := range ret.Relations {
		res = append(res, rrelation.RefRelationFromProto(r))
	}

	return res, nil
}

// GetKeyRange gets key range by id
// TODO unit tests

// GetKeyRange retrieves the key range for the given ID.
//
// Parameters:
// - ctx (context.Context): The context for the request.
// - krId (string): The ID of the key range to retrieve.
//
// Returns:
// - *kr.KeyRange: The retrieved key range.
// - error: An error if the retrieval was unsuccessful.
func (a *Adapter) GetKeyRange(ctx context.Context, krId string) (*kr.KeyRange, error) {
	c := proto.NewKeyRangeServiceClient(a.conn)
	dc := proto.NewDistributionServiceClient(a.conn)
	reply, err := c.GetKeyRange(ctx, &proto.GetKeyRangeRequest{
		Ids: []string{krId},
	})
	if err != nil {
		return nil, err
	}
	// what if len > 1 ?
	if len(reply.KeyRangesInfo) == 0 {
		return nil, nil
	}
	ds, err := dc.GetDistribution(ctx, &proto.GetDistributionRequest{
		Id: reply.KeyRangesInfo[0].DistributionId,
	})

	if err != nil {
		return nil, err
	}

	return kr.KeyRangeFromProto(reply.KeyRangesInfo[0], ds.Distribution.ColumnTypes)
}

// TODO : unit tests

// ListKeyRanges lists the key ranges based on the specified distribution.
//
// Parameters:
// - ctx (context.Context): The context for the request.
// - distribution (string): The distribution for filtering key ranges.
//
// Returns:
// - []*kr.KeyRange: A list of key ranges based on the distribution.
// - error: An error if listing the key ranges was unsuccessful.
func (a *Adapter) ListKeyRanges(ctx context.Context, distribution string) ([]*kr.KeyRange, error) {
	c := proto.NewKeyRangeServiceClient(a.conn)
	reply, err := c.ListKeyRange(ctx, &proto.ListKeyRangeRequest{
		Distribution: distribution,
	})
	if err != nil {
		return nil, spqrerror.CleanGrpcError(err)
	}

	dc := proto.NewDistributionServiceClient(a.conn)
	ds, err := dc.GetDistribution(ctx, &proto.GetDistributionRequest{Id: distribution})
	if err != nil {
		return nil, spqrerror.CleanGrpcError(err)
	}

	krs := make([]*kr.KeyRange, len(reply.KeyRangesInfo))
	for i, keyRange := range reply.KeyRangesInfo {
		krs[i], err = kr.KeyRangeFromProto(keyRange, ds.Distribution.ColumnTypes)
		if err != nil {
			return nil, err
		}
	}

	return krs, nil
}

// ListKeyRangeLocks lists the locked key ranges idents .
//
// Parameters:
// - ctx (context.Context): The context for the request.
//
// Returns:
// - []string: A list locked key range idents.
// - error: An error if listing the key ranges was unsuccessful.
func (a *Adapter) ListKeyRangeLocks(ctx context.Context) ([]string, error) {
	client := proto.NewKeyRangeServiceClient(a.conn)
	reply, err := client.ListKeyRangeLocks(ctx, nil)
	if err != nil {
		return nil, err
	}
	return reply.KeyRangesLocks, nil
}

// TODO : unit tests

// ListAllKeyRanges lists all key ranges available.
//
// Parameters:
// - ctx (context.Context): The context for the request.
//
// Returns:
// - []*kr.KeyRange: A list of all available key ranges.
// - error: An error if listing all key ranges was unsuccessful.
func (a *Adapter) ListAllKeyRanges(ctx context.Context) ([]*kr.KeyRange, error) {
	c := proto.NewKeyRangeServiceClient(a.conn)
	reply, err := c.ListAllKeyRanges(ctx, nil)
	if err != nil {
		return nil, err
	}

	dc := proto.NewDistributionServiceClient(a.conn)

	krs := make([]*kr.KeyRange, len(reply.KeyRangesInfo))
	for i, keyRange := range reply.KeyRangesInfo {
		ds, err := dc.GetDistribution(ctx, &proto.GetDistributionRequest{Id: keyRange.DistributionId})
		if err != nil {
			return nil, err
		}
		krs[i], err = kr.KeyRangeFromProto(keyRange, ds.Distribution.ColumnTypes)
		if err != nil {
			return nil, err
		}
	}

	return krs, nil
}

// TODO : unit tests

// CreateKeyRange creates a new key range.
//
// Parameters:
// - ctx (context.Context): The context for the request.
// - kr (*kr.KeyRange): The key range object to be created.
//
// Returns:
// - error: An error if creating the key range was unsuccessful.
func (a *Adapter) CreateKeyRange(ctx context.Context, kr *kr.KeyRange) error {
	c := proto.NewKeyRangeServiceClient(a.conn)
	_, err := c.CreateKeyRange(ctx, &proto.CreateKeyRangeRequest{
		KeyRangeInfo: kr.ToProto(),
	})

	return spqrerror.CleanGrpcError(err)
}

// TODO : unit tests

// LockKeyRange locks the key range with the given ID.
//
// Parameters:
// - ctx (context.Context): The context for the request.
// - krid (string): The ID of the key range to lock.
//
// Returns:
// - *kr.KeyRange: The locked key range.
// - error: An error if locking the key range was unsuccessful.
func (a *Adapter) LockKeyRange(ctx context.Context, krid string) (*kr.KeyRange, error) {
	c := proto.NewKeyRangeServiceClient(a.conn)
	_, err := c.LockKeyRange(ctx, &proto.LockKeyRangeRequest{
		Id: []string{krid},
	})
	if err != nil {
		return nil, err
	}

	krs, err := a.ListAllKeyRanges(ctx)
	if err != nil {
		return nil, err
	}

	for _, kr := range krs {
		if kr.ID == krid {
			return kr, nil
		}
	}

	return nil, spqrerror.Newf(spqrerror.SPQR_KEYRANGE_ERROR, "key range with id %s not found", krid)
}

// TODO : unit tests

// UnlockKeyRange unlocks the key range with the given ID.
//
// Parameters:
// - ctx (context.Context): The context for the request.
// - krid (string): The ID of the key range to unlock.
//
// Returns:
// - error: An error if unlocking the key range was unsuccessful.
func (a *Adapter) UnlockKeyRange(ctx context.Context, krid string) error {
	c := proto.NewKeyRangeServiceClient(a.conn)
	_, err := c.UnlockKeyRange(ctx, &proto.UnlockKeyRangeRequest{
		Id: []string{krid},
	})
	if err != nil {
		return err
	}

	return nil
}

// TODO : unit tests

// Split splits a key range based on the provided split information.
//
// Parameters:
// - ctx (context.Context): The context for the request.
// - split (*kr.SplitKeyRange): The split information for dividing the key range.
//
// Returns:
// - error: An error if splitting the key range was unsuccessful.
func (a *Adapter) Split(ctx context.Context, split *kr.SplitKeyRange) error {
	krs, err := a.ListAllKeyRanges(ctx)
	if err != nil {
		return err
	}

	for _, keyRange := range krs {
		if keyRange.ID == split.SourceID {
			c := proto.NewKeyRangeServiceClient(a.conn)

			nkr := keyRange.ToProto()
			nkr.Krid = split.Krid

			_, err := c.SplitKeyRange(ctx, &proto.SplitKeyRangeRequest{
				Bound:     split.Bound[0], // fix multidim case
				SourceId:  split.SourceID,
				NewId:     split.Krid,
				SplitLeft: split.SplitLeft,
			})
			return spqrerror.CleanGrpcError(err)
		}
	}

	return spqrerror.Newf(spqrerror.SPQR_KEYRANGE_ERROR, "key range with id %s not found", split.Krid)
}

// TODO : unit tests

// Unite merges two key ranges based on the provided unite information.
//
// Parameters:
// - ctx (context.Context): The context for the request.
// - unite (*kr.UniteKeyRange): The unite information for merging the key ranges.
//
// Returns:
// - error: An error if merging the key ranges was unsuccessful.
func (a *Adapter) Unite(ctx context.Context, unite *kr.UniteKeyRange) error {
	krs, err := a.ListAllKeyRanges(ctx)
	if err != nil {
		return err
	}

	var left *kr.KeyRange
	var right *kr.KeyRange

	// Check for in-between key ranges
	for _, kr := range krs {
		if kr.ID == unite.BaseKeyRangeId {
			left = kr
		}
		if kr.ID == unite.AppendageKeyRangeId {
			right = kr
		}
	}

	if kr.CmpRangesLess(right.LowerBound, left.LowerBound, right.ColumnTypes) {
		left, right = right, left
	}

	for _, krCurr := range krs {
		if krCurr.ID == unite.BaseKeyRangeId || krCurr.ID == unite.AppendageKeyRangeId {
			continue
		}
		if kr.CmpRangesLess(krCurr.LowerBound, right.LowerBound, krCurr.ColumnTypes) && kr.CmpRangesLess(left.LowerBound, krCurr.LowerBound, krCurr.ColumnTypes) {
			return spqrerror.New(spqrerror.SPQR_KEYRANGE_ERROR, "unvalid unite request")
		}
	}

	if left == nil || right == nil || kr.CmpRangesLess(right.LowerBound, left.LowerBound, right.ColumnTypes) {
		return spqrerror.New(spqrerror.SPQR_KEYRANGE_ERROR, "key range on left or right was not found")
	}

	c := proto.NewKeyRangeServiceClient(a.conn)
	_, err = c.MergeKeyRange(ctx, &proto.MergeKeyRangeRequest{
		BaseId:      unite.BaseKeyRangeId,
		AppendageId: unite.AppendageKeyRangeId,
	})
	return spqrerror.CleanGrpcError(err)
}

// TODO : unit tests

// Move moves a key range to the specified shard.
//
// Parameters:
// - ctx (context.Context): The context for the request.
// - move (*kr.MoveKeyRange): The move information for moving the key range.
//
// Returns:
// - error: An error if moving the key range was unsuccessful.
func (a *Adapter) Move(ctx context.Context, move *kr.MoveKeyRange) error {
	krs, err := a.ListAllKeyRanges(ctx)
	if err != nil {
		return err
	}

	for _, keyRange := range krs {
		if keyRange.ID == move.Krid {
			c := proto.NewKeyRangeServiceClient(a.conn)
			_, err := c.MoveKeyRange(ctx, &proto.MoveKeyRangeRequest{
				Id:        keyRange.ID,
				ToShardId: move.ShardId,
			})
			return err
		}
	}

	return spqrerror.Newf(spqrerror.SPQR_KEYRANGE_ERROR, "key range with id %s not found", move.Krid)
}

// TODO : unit tests

// BatchMoveKeyRange moves a specified amount of keys from a key range to another shard.
//
// Parameters:
// - ctx (context.Context): The context for the request.
// - req (*kr.BatchMoveKeyRange): The move information for moving the data.
//
// Returns:
// - error: An error if moving the data was unsuccessful.
func (a *Adapter) BatchMoveKeyRange(ctx context.Context, req *kr.BatchMoveKeyRange) error {
	c := proto.NewKeyRangeServiceClient(a.conn)
	var limitType proto.RedistributeLimitType
	limit := int64(0)
	if req.Limit < 0 {
		limitType = proto.RedistributeLimitType_RedistributeAllKeys
	} else {
		limit = req.Limit
		limitType = proto.RedistributeLimitType_RedistributeKeysLimit
	}
	_, err := c.BatchMoveKeyRange(ctx, &proto.BatchMoveKeyRangeRequest{
		Id:        req.KrId,
		ToShardId: req.ShardId,
		ToKrId:    req.DestKrId,
		LimitType: limitType,
		Limit:     limit,
		BatchSize: int64(req.BatchSize),
		SplitType: func() proto.SplitType {
			switch req.Type {
			case tasks.SplitLeft:
				return proto.SplitType_SplitLeft
			case tasks.SplitRight:
				return proto.SplitType_SplitRight
			default:
				panic("unknown split type")
			}
		}(),
	})
	return err
}

// RedistributeKeyRange moves a key range to the specified shard.
// Data is moved in batches of a given size.
//
// Parameters:
//   - ctx (context.Context): The context for the request.
//   - req (*kr.RedistributeKeyRange): The move information for moving the key range.
//
// Returns:
// - error: An error if moving the key range was unsuccessful.
func (a *Adapter) RedistributeKeyRange(ctx context.Context, req *kr.RedistributeKeyRange) error {
	c := proto.NewKeyRangeServiceClient(a.conn)
	_, err := c.RedistributeKeyRange(ctx, &proto.RedistributeKeyRangeRequest{
		Id:        req.KrId,
		ShardId:   req.ShardId,
		BatchSize: int64(req.BatchSize),
		Check:     req.Check,
		Apply:     req.Apply,
	})
	return err
}

// RenameKeyRange renames a key range.
//
// Parameters:
//   - ctx (context.Context): The context for the request.
//   - krId (string): The ID of the key range to be renamed.
//   - krIdNew (string): The new ID for the specified key range.
//
// Returns:
// - error: An error if renaming key range was unsuccessful.
func (a *Adapter) RenameKeyRange(ctx context.Context, krId, krIdNew string) error {
	c := proto.NewKeyRangeServiceClient(a.conn)
	_, err := c.RenameKeyRange(ctx, &proto.RenameKeyRangeRequest{
		KeyRangeId:    krId,
		NewKeyRangeId: krIdNew,
	})
	return err
}

// TODO : unit tests

// DropKeyRange drops a key range using the provided ID.
//
// Parameters:
// - ctx (context.Context): The context for the request.
// - krid (string): The ID of the key range to unlock.
//
// Returns:
// - error: An error if the key range drop fails, otherwise nil.
func (a *Adapter) DropKeyRange(ctx context.Context, krid string) error {
	c := proto.NewKeyRangeServiceClient(a.conn)
	_, err := c.DropKeyRange(ctx, &proto.DropKeyRangeRequest{
		Id: []string{krid},
	})
	return err
}

// TODO : unit tests

// DropKeyRangeAll drops all key ranges.
//
// Parameters:
// - ctx (context.Context): The context for the request.
//
// Returns:
// - error: An error if the key range drop fails, otherwise nil.
func (a *Adapter) DropKeyRangeAll(ctx context.Context) error {
	c := proto.NewKeyRangeServiceClient(a.conn)
	_, err := c.DropAllKeyRanges(ctx, nil)
	return err
}

// TODO : unit tests

// RegisterRouter registers a router using the provided context and router instance.
//
// Parameters:
// - ctx (context.Context): The context for the request.
// - r (*topology.Router): The topology.Router instance to register.
//
// Returns:
// - error: An error if the router registration fails, otherwise nil.
func (a *Adapter) RegisterRouter(ctx context.Context, r *topology.Router) error {
	c := proto.NewRouterServiceClient(a.conn)
	_, err := c.AddRouter(ctx, &proto.AddRouterRequest{
		Router: topology.RouterToProto(r),
	})
	return err
}

// TODO : unit tests

// ListRouters lists all routers available.
//
// Parameters:
// - ctx (context.Context): The context for the request.
//
// Returns:
// - []*topology.Router: A list of router instances.
// - error: An error if listing routers fails, otherwise nil.
func (a *Adapter) ListRouters(ctx context.Context) ([]*topology.Router, error) {
	c := proto.NewRouterServiceClient(a.conn)
	resp, err := c.ListRouters(ctx, nil)
	if err != nil {
		return nil, err
	}
	routers := []*topology.Router{}
	for _, r := range resp.Routers {
		routers = append(routers, topology.RouterFromProto(r))
	}
	return routers, nil
}

// TODO : unit tests

// UnregisterRouter removes a router using the provided ID.
//
// Parameters:
// - ctx (context.Context): The context for the request.
// - id (string): The ID of the router to remove.
//
// Returns:
// - error: An error if the router removal fails, otherwise nil.
func (a *Adapter) UnregisterRouter(ctx context.Context, id string) error {
	c := proto.NewRouterServiceClient(a.conn)
	_, err := c.RemoveRouter(ctx, &proto.RemoveRouterRequest{
		Id: id,
	})
	return err
}

// TODO : unit tests

// SyncRouterMetadata synchronizes the metadata of a router.
//
// Parameters:
// - ctx (context.Context): The context for the request.
// - router (*topology.Router): The topology.Router instance to synchronize the metadata for.
//
// Returns:
// - error: An error if the metadata synchronization fails, otherwise nil.
func (a *Adapter) SyncRouterMetadata(ctx context.Context, router *topology.Router) error {
	c := proto.NewRouterServiceClient(a.conn)
	_, err := c.SyncMetadata(ctx, &proto.SyncMetadataRequest{
		Router: topology.RouterToProto(router),
	})
	return err
}

// SyncRouterCoordinatorAddress implements meta.EntityMgr.
// SyncRouterCoordinatorAddress synchronizes the coordinator address of a router.
//
// Parameters:
// - ctx (context.Context): The context for the request.
// - router (*topology.Router): The topology.Router instance to synchronize the coordinator address for.
//
// Returns:
// - error: An error if the synchronization of the coordinator address fails, otherwise nil.
func (a *Adapter) SyncRouterCoordinatorAddress(ctx context.Context, router *topology.Router) error {
	c := proto.NewRouterServiceClient(a.conn)
	_, err := c.SyncMetadata(ctx, &proto.SyncMetadataRequest{
		Router: topology.RouterToProto(router),
	})
	return err
}

// TODO : unit tests
// TODO : implement

// AddDataShard adds a data shard to the system.
//
// Parameters:
// - ctx (context.Context): The context for the request.
// - shard (*topology.DataShard): The topology.DataShard instance to add.
//
// Returns:
// - error: An error if the data shard addition fails, otherwise nil.
func (a *Adapter) AddDataShard(ctx context.Context, shard *topology.DataShard) error {
	return spqrerror.New(spqrerror.SPQR_NOT_IMPLEMENTED, "addDataShard not implemented")
}

// TODO : unit tests
// TODO : implement

// DropShard drops a data shard from the system.
//
// Parameters:
// - ctx (context.Context): The context for the request.
// - shardId (string): The ID of the data shard to drop.
//
// Returns:
// - error: An error if the data shard drop fails, otherwise nil.
func (a *Adapter) DropShard(ctx context.Context, shardId string) error {
	return spqrerror.New(spqrerror.SPQR_NOT_IMPLEMENTED, "DropShard not implemented")
}

// TODO : unit tests
// TODO : implement

// AddWorldShard adds a world shard to the system.
//
// Parameters:
// - ctx (context.Context): The context for the request.
// - shard (*topology.DataShard): The topology.DataShard instance to add.
//
// Returns:
// - error: An error if the world shard addition fails, otherwise nil.
func (a *Adapter) AddWorldShard(ctx context.Context, shard *topology.DataShard) error {
	return spqrerror.New(spqrerror.SPQR_NOT_IMPLEMENTED, "addWorldShard not implemented")
}

// TODO : unit tests

// ListShards retrieves a list of data shards from the system.
//
// Parameters:
// - ctx (context.Context): The context for the request.
//
// Returns:
// - []*topology.DataShard: A list of data shards.
// - error: An error if the retrieval of shards fails, otherwise nil.
func (a *Adapter) ListShards(ctx context.Context) ([]*topology.DataShard, error) {
	c := proto.NewShardServiceClient(a.conn)
	resp, err := c.ListShards(ctx, nil)
	shards := resp.Shards
	var ds []*topology.DataShard
	for _, shard := range shards {
		ds = append(ds, &topology.DataShard{
			ID:  shard.Id,
			Cfg: &config.Shard{RawHosts: shard.Hosts},
		})
	}
	return ds, err
}

// TODO : unit tests

// GetShard retrieves a specific data shard from the system.
//
// Parameters:
// - ctx (context.Context): The context for the request.
// - shardId (string): The ID of the data shard to retrieve.
//
// Returns:
// - *topology.DataShard: The retrieved data shard.
// - error: An error if the retrieval of the shard fails, otherwise nil.
func (a *Adapter) GetShard(ctx context.Context, shardID string) (*topology.DataShard, error) {
	c := proto.NewShardServiceClient(a.conn)
	if resp, err := c.GetShard(ctx, &proto.ShardRequest{Id: shardID}); err != nil {
		return nil, err
	} else {
		return &topology.DataShard{
			ID:  resp.Shard.Id,
			Cfg: &config.Shard{RawHosts: resp.Shard.Hosts},
		}, err
	}
}

// TODO : unit tests

// ListDistributions retrieves a list of distributions from the system.
//
// Parameters:
// - ctx (context.Context): The context for the request.
//
// Returns:
// - []*distributions.Distribution: A list of distributions.
// - error: An error if the retrieval of distributions fails, otherwise nil.
func (a *Adapter) ListDistributions(ctx context.Context) ([]*distributions.Distribution, error) {
	c := proto.NewDistributionServiceClient(a.conn)

	resp, err := c.ListDistributions(ctx, nil)
	if err != nil {
		return nil, err
	}

	dss := make([]*distributions.Distribution, len(resp.Distributions))
	for i, ds := range resp.Distributions {
		dss[i], err = distributions.DistributionFromProto(ds)
		if err != nil {
			return nil, err
		}
	}

	return dss, nil
}

// TODO : unit tests

// CreateDistribution creates a new distribution in the system.
//
// Parameters:
// - ctx (context.Context): The context for the request.
// - ds (*distributions.Distribution): The distributions.Distribution instance to create.
//
// Returns:
// - error: An error if the creation of the distribution fails, otherwise nil.
func (a *Adapter) CreateDistribution(ctx context.Context, ds *distributions.Distribution) (*mtran.MetaTransactionChunk, error) {
	c := proto.NewDistributionServiceClient(a.conn)

	if reply, err := c.CreateDistribution(ctx, &proto.CreateDistributionRequest{
		Distributions: []*proto.Distribution{
			distributions.DistributionToProto(ds),
		},
	}); err != nil {
		return nil, err
	} else {
		qdbCmds := make([]qdb.QdbStatement, 0, len(reply.CmdList))
		for _, cmd := range reply.CmdList {
			if qdbCmd, err := qdb.QdbStmtFromProto(cmd); err != nil {
				return nil, err
			} else {
				qdbCmds = append(qdbCmds, *qdbCmd)
			}
		}
		return mtran.NewMetaTransactionChunk(reply.MetaCmdList, qdbCmds)
	}
}

// TODO : unit tests

// DropDistribution removes a distribution from the system.
//
// Parameters:
// - ctx (context.Context): The context for the request.
// - id (string): The ID of the distribution to remove.
//
// Returns:
// - error: An error if the removal of the distribution fails, otherwise nil.
func (a *Adapter) DropDistribution(ctx context.Context, id string) error {
	c := proto.NewDistributionServiceClient(a.conn)

	_, err := c.DropDistribution(ctx, &proto.DropDistributionRequest{
		Ids: []string{id},
	})

	return spqrerror.CleanGrpcError(err)
}

// TODO : unit tests

// AlterDistributionAttach alters the attachments of a distribution in the system.
//
// Parameters:
// - ctx (context.Context): The context for the request.
// - id (string): The ID of the distribution to alter.
// - rels ([]*distributions.DistributedRelation): The list of distributions.DistributedRelation instances to attach.
//
// Returns:
// - error: An error if the alteration of the distribution's attachments fails, otherwise nil.
func (a *Adapter) AlterDistributionAttach(ctx context.Context, id string, rels []*distributions.DistributedRelation) error {
	c := proto.NewDistributionServiceClient(a.conn)

	dRels := []*proto.DistributedRelation{}
	for _, r := range rels {
		dRels = append(dRels, distributions.DistributedRelationToProto(r))
	}

	_, err := c.AlterDistributionAttach(ctx, &proto.AlterDistributionAttachRequest{
		Id:        id,
		Relations: dRels,
	})

	return spqrerror.CleanGrpcError(err)
}

// TODO : unit tests

// AlterDistributedRelation alters the metadata of a distributed relation.
//
// Parameters:
// - ctx (context.Context): The context for the request.
// - id (string): The ID of the distribution of the relation.
// - rel (*distributions.DistributedRelation): The metadata for the distributed relation.
//
// Returns:
// - error: An error if the alteration of the distribution's attachments fails, otherwise nil.
func (a *Adapter) AlterDistributedRelation(ctx context.Context, id string, rel *distributions.DistributedRelation) error {
	c := proto.NewDistributionServiceClient(a.conn)
	_, err := c.AlterDistributedRelation(ctx, &proto.AlterDistributedRelationRequest{
		Id:       id,
		Relation: distributions.DistributedRelationToProto(rel),
	})
	return spqrerror.CleanGrpcError(err)
}

// AlterDistributedRelationSchema alters the sequence name of a distributed relation.
//
// Parameters:
// - ctx (context.Context): The context for the request.
// - id (string): The ID of the distribution of the relation.
// - relName (string): The name of the relation.
// - schemaName (string): the new schema name for the relation.
//
// Returns:
// - error: An error if the alteration of the distribution's attachments fails, otherwise nil.
func (a *Adapter) AlterDistributedRelationSchema(ctx context.Context, id string, relName string, schemaName string) error {
	c := proto.NewDistributionServiceClient(a.conn)
	_, err := c.AlterDistributedRelationSchema(ctx, &proto.AlterDistributedRelationSchemaRequest{
		Id:           id,
		RelationName: relName,
		SchemaName:   schemaName,
	})
	return err
}

// AlterDistributedRelationDistributionKey alters the distribution key metadata of a distributed relation.
//
// Parameters:
// - ctx (context.Context): The context for the request.
// - id (string): The ID of the distribution of the relation.
// - relName (string): The name of the relation.
// - distributionKey ([]distributions.DistributionKeyEntry): the new distribution key for the relation.
//
// Returns:
// - error: An error if the alteration of the distribution's attachments fails, otherwise nil.
func (a *Adapter) AlterDistributedRelationDistributionKey(ctx context.Context, id string, relName string, distributionKey []distributions.DistributionKeyEntry) error {
	c := proto.NewDistributionServiceClient(a.conn)
	_, err := c.AlterDistributedRelationDistributionKey(ctx, &proto.AlterDistributedRelationDistributionKeyRequest{
		Id:              id,
		RelationName:    relName,
		DistributionKey: distributions.DistributionKeyToProto(distributionKey),
	})
	return spqrerror.CleanGrpcError(err)
}

// AlterDistributionDetach detaches a relation from a distribution using the provided ID and relation name.
//
// Parameters:
// - ctx (context.Context): The context for the request.
// - id (string): The ID of the distribution to detach from.
// - relName (*rfqn.RelationFQN): The qualified name of the relation to detach.
//
// Returns:
// - error: An error if the detachment fails, otherwise nil.
func (a *Adapter) AlterDistributionDetach(ctx context.Context, id string, relName *rfqn.RelationFQN) error {
	c := proto.NewDistributionServiceClient(a.conn)
	_, err := c.AlterDistributionDetach(ctx, &proto.AlterDistributionDetachRequest{
		Id:       id,
		RelNames: []*proto.QualifiedName{rfqn.RelationFQNToProto(relName)},
	})

	return err
}

// TODO : unit tests

// GetDistribution retrieves a specific distribution from the system.
//
// Parameters:
// - ctx (context.Context): The context for the request.
// - id (string): The ID of the distribution to retrieve.
//
// Returns:
// - *distributions.Distribution: The retrieved distribution.
// - error: An error if the retrieval of the distribution fails, otherwise nil.
func (a *Adapter) GetDistribution(ctx context.Context, id string) (*distributions.Distribution, error) {
	c := proto.NewDistributionServiceClient(a.conn)

	resp, err := c.GetDistribution(ctx, &proto.GetDistributionRequest{
		Id: id,
	})
	if err != nil {
		return nil, spqrerror.CleanGrpcError(err)
	}

	return distributions.DistributionFromProto(resp.Distribution)
}

// GetRelationDistribution retrieves the distribution related to a specific relation from the system.
//
// Parameters:
// - ctx (context.Context): The context for the request.
// - relationName (string): The ID of the relation (type: string).
//
// Returns:
// - *distributions.Distribution: The retrieved distribution related to the relation.
// - error: An error if the retrieval of the distribution fails, otherwise nil.
func (a *Adapter) GetRelationDistribution(ctx context.Context, relationName *rfqn.RelationFQN) (*distributions.Distribution, error) {
	c := proto.NewDistributionServiceClient(a.conn)
	resp, err := c.GetRelationDistribution(ctx, &proto.GetRelationDistributionRequest{
		Name:       relationName.RelationName,
		SchemaName: relationName.SchemaName,
	})
	if err != nil {
		return nil, err
	}

	return distributions.DistributionFromProto(resp.Distribution)
}

// ListMoveTasks retrieves all move tasks from the system.
//
// Parameters:
// - ctx (context.Context): The context for the request.
//
// Returns:
// - map[string]*tasks.MoveTask: Map of move task IDs to tasks themselves.
// - error: An error if the retrieval of tasks fails, otherwise nil.
func (a *Adapter) ListMoveTasks(ctx context.Context) (map[string]*tasks.MoveTask, error) {
	tasksService := proto.NewMoveTasksServiceClient(a.conn)
	reply, err := tasksService.ListMoveTasks(ctx, nil)
	if err != nil {
		return nil, err
	}
	res := make(map[string]*tasks.MoveTask)
	for _, taskProto := range reply.Tasks {
		taskGroup := tasks.MoveTaskFromProto(taskProto)
		res[taskGroup.ID] = taskGroup
	}
	return res, nil
}

// ListMoveTaskGroups retrieves all task groups from the system.
//
// Parameters:
// - ctx (context.Context): The context for the request.
//
// Returns:
// - map[string]*tasks.MoveTaskGroup: Map of task group IDs to task groups themselves.
// - error: An error if the retrieval of task groups fails, otherwise nil.
func (a *Adapter) ListMoveTaskGroups(ctx context.Context) (map[string]*tasks.MoveTaskGroup, error) {
	tasksService := proto.NewMoveTasksServiceClient(a.conn)
	reply, err := tasksService.ListMoveTaskGroups(ctx, nil)
	if err != nil {
		return nil, err
	}
	res := make(map[string]*tasks.MoveTaskGroup)
	for _, taskGroupProto := range reply.TaskGroups {
		taskGroup := tasks.TaskGroupFromProto(taskGroupProto)
		res[taskGroup.ID] = taskGroup
	}
	return res, nil
}

// GetMoveTaskGroup retrieves the task group from the system.
//
// Parameters:
// - ctx (context.Context): The context for the request.
// - id  (string): The ID of the task group to retrieve
//
// Returns:
// - *tasks.MoveTaskGroup: The retrieved task group.
// - error: An error if the retrieval of the task group fails, otherwise nil.
func (a *Adapter) GetMoveTaskGroup(ctx context.Context, id string) (*tasks.MoveTaskGroup, error) {
	tasksService := proto.NewMoveTasksServiceClient(a.conn)
	res, err := tasksService.GetMoveTaskGroup(ctx, &proto.MoveTaskGroupSelector{ID: id})
	if err != nil {
		return nil, err
	}
	return tasks.TaskGroupFromProto(res.TaskGroup), nil
}

// WriteMoveTaskGroup writes a task group to the system.
//
// Parameters:
// - ctx (context.Context): The context for the request.
// - taskGroup (*tasks.MoveTaskGroup): The task group to be written.
//
// Returns:
// - error: An error if the writing of the task group fails, otherwise nil.
func (a *Adapter) WriteMoveTaskGroup(ctx context.Context, taskGroup *tasks.MoveTaskGroup) error {
	tasksService := proto.NewMoveTasksServiceClient(a.conn)
	_, err := tasksService.WriteMoveTaskGroup(ctx, &proto.WriteMoveTaskGroupRequest{
		TaskGroup: tasks.TaskGroupToProto(taskGroup),
	})
	return err
}

// RemoveMoveTaskGroup removes a task group from the system.
//
// Parameters:
// - ctx (context.Context): The context for the request.
// - id  (string): The ID of the task group to remove.
//
// Returns:
// - error: An error if the removal of the task group fails, otherwise nil.
func (a *Adapter) RemoveMoveTaskGroup(ctx context.Context, id string) error {
	tasksService := proto.NewMoveTasksServiceClient(a.conn)
	_, err := tasksService.RemoveMoveTaskGroup(ctx, &proto.MoveTaskGroupSelector{ID: id})
	return err
}

// RetryMoveTaskGroup re-launches the current move task group.
// If no move task group is currently being executed, then nothing is done.
//
// Parameters:
// - ctx (context.Context): The context for the request.
// - id  (string): The ID of the task group to retry.
//
// Returns:
// - error: An error if the operation fails, otherwise nil.
func (a *Adapter) RetryMoveTaskGroup(ctx context.Context, id string) error {
	tasksService := proto.NewMoveTasksServiceClient(a.conn)
	_, err := tasksService.RetryMoveTaskGroup(ctx, &proto.MoveTaskGroupSelector{ID: id})
	return err
}

// StopMoveTaskGroup gracefully stops the execution of current move task group.
// When current move task is completed, move task group will be finished.
//
// Parameters:
// - ctx (context.Context): The context for the request.
// - id  (string): The ID of the task group to stop.
//
// Returns:
// - error: An error if the operation fails, otherwise nil.
func (a *Adapter) StopMoveTaskGroup(ctx context.Context, id string) error {
	tasksService := proto.NewMoveTasksServiceClient(a.conn)
	_, err := tasksService.StopMoveTaskGroup(ctx, &proto.MoveTaskGroupSelector{ID: id})
	return err
}

// GetTaskGroupStatus gets the status of the task group.
//
// Parameters:
// - ctx (context.Context): the context.Context object for managing the request's lifetime.
// - id  (string):          ID of the task group
//
// Returns:
// - *tasks.MoveTaskGroupStatus: the status of the task group
// - error: an error if the removal operation fails.
func (a *Adapter) GetTaskGroupStatus(ctx context.Context, id string) (*tasks.MoveTaskGroupStatus, error) {
	tasksService := proto.NewMoveTasksServiceClient(a.conn)
	status, err := tasksService.GetMoveTaskGroupStatus(ctx, &proto.MoveTaskGroupSelector{ID: id})
	return tasks.MoveTaskGroupStatusFromProto(status), err
}

// GetAllTaskGroupStatuses gets statuses of all task groups.
//
// Parameters:
// - ctx (context.Context): the context.Context object for managing the request's lifetime.
//
// Returns:
// - map[string]*tasks.MoveTaskGroupStatus: the statuses of the task group by ID
// - error: an error if the removal operation fails.
func (a *Adapter) GetAllTaskGroupStatuses(ctx context.Context) (map[string]*tasks.MoveTaskGroupStatus, error) {
	tasksService := proto.NewMoveTasksServiceClient(a.conn)
	ret, err := tasksService.GetAllMoveTaskGroupStatuses(ctx, nil)
	res := make(map[string]*tasks.MoveTaskGroupStatus)
	for id, status := range ret.Statuses {
		res[id] = tasks.MoveTaskGroupStatusFromProto(status)
	}
	return res, err
}

// GetBalancerTask retrieves current balancer task from the system.
//
// Parameters:
//   - ctx (context.Context): The context for the request.
//
// Returns:
//   - *tasks.BalancerTask: The retrieved balancer task.
//   - error: An error if the retrieval of the balancer task fails, otherwise nil.
func (a *Adapter) GetBalancerTask(ctx context.Context) (*tasks.BalancerTask, error) {
	tasksService := proto.NewBalancerTaskServiceClient(a.conn)
	res, err := tasksService.GetBalancerTask(ctx, nil)
	if err != nil {
		return nil, err
	}
	return tasks.BalancerTaskFromProto(res.Task), nil
}

// WriteBalancerTask writes a balancer task to the system.
//
// Parameters:
//   - ctx (context.Context): The context for the request.
//   - task (*tasks.BalancerTask): The balancer task to be written.
//
// Returns:
//   - error: An error if the writing of the balancer task fails, otherwise nil.
func (a *Adapter) WriteBalancerTask(ctx context.Context, task *tasks.BalancerTask) error {
	tasksService := proto.NewBalancerTaskServiceClient(a.conn)
	_, err := tasksService.WriteBalancerTask(ctx, &proto.WriteBalancerTaskRequest{Task: tasks.BalancerTaskToProto(task)})
	return err
}

// RemoveBalancerTask removes a balancer task from the system.
//
// Parameters:
//   - ctx (context.Context): The context for the request.
//
// Returns:
//   - error: An error if the removal of the balancer task fails, otherwise nil.
func (a *Adapter) RemoveBalancerTask(ctx context.Context) error {
	tasksService := proto.NewBalancerTaskServiceClient(a.conn)
	_, err := tasksService.RemoveBalancerTask(ctx, nil)
	return err
}

// TODO : unit tests

// UpdateCoordinator updates the coordinator with the given address.
//
// Parameters:
// - ctx (context.Context): The context for the request.
// - address (string): The address of the coordinator to update.
//
// Returns:
// - error: An error if the update operation fails, otherwise nil.
func (a *Adapter) UpdateCoordinator(ctx context.Context, address string) error {
	c := proto.NewTopologyServiceClient(a.conn)
	_, err := c.UpdateCoordinator(ctx, &proto.UpdateCoordinatorRequest{Address: address})
	return err
}

// TODO : unit tests

// GetCoordinator retrieves the address of the coordinator from the system.
//
// Parameters:
// - ctx (context.Context): The context for the request.
//
// Returns:
// - string: The address of the coordinator.
// - error: An error if the retrieval operation fails, otherwise nil.
func (a *Adapter) GetCoordinator(ctx context.Context) (string, error) {
	c := proto.NewTopologyServiceClient(a.conn)
	resp, err := c.GetCoordinator(ctx, nil)
	return resp.Address, err
}

func (a *Adapter) ListSequences(ctx context.Context) ([]string, error) {
	c := proto.NewDistributionServiceClient(a.conn)
	resp, err := c.ListSequences(ctx, nil)
	if err != nil {
		return nil, err
	}
	return resp.Names, nil
}

func (a *Adapter) DropSequence(ctx context.Context, seqName string, force bool) error {
	c := proto.NewDistributionServiceClient(a.conn)
	_, err := c.DropSequence(ctx, &proto.DropSequenceRequest{
		Name:  seqName,
		Force: force,
	})
	return spqrerror.CleanGrpcError(err)
}

func (a *Adapter) NextRange(ctx context.Context, seqName string, rangeSize uint64) (*qdb.SequenceIdRange, error) {
	c := proto.NewDistributionServiceClient(a.conn)
	resp, err := c.NextRange(ctx, &proto.NextRangeRequest{
		Seq:       seqName,
		RangeSize: int64(rangeSize),
	})
	if err != nil {
		return nil, err
	}
	return qdb.NewSequenceIdRange(resp.Left, resp.Right)
}

func (a *Adapter) CurrVal(ctx context.Context, seqName string) (int64, error) {
	c := proto.NewDistributionServiceClient(a.conn)
	resp, err := c.CurrVal(ctx, &proto.CurrValRequest{
		Seq: seqName,
	})
	if err != nil {
		return -1, err
	}
	return resp.Value, err
}

func (a *Adapter) ListRelationSequences(ctx context.Context, relName *rfqn.RelationFQN) (map[string]string, error) {
	c := proto.NewDistributionServiceClient(a.conn)
	resp, err := c.ListRelationSequences(ctx, &proto.ListRelationSequencesRequest{
		Name:       relName.RelationName,
		SchemaName: relName.SchemaName,
	})
	if err != nil {
		return nil, err
	}

	return resp.ColumnSequences, nil
}

func (a *Adapter) ExecNoTran(ctx context.Context, chunk *mtran.MetaTransactionChunk) error {
	conn := proto.NewMetaTransactionServiceClient(a.conn)
	request := &proto.ExecNoTranRequest{
		MetaCmdList: chunk.GossipRequests,
		CmdList:     qdb.SliceToProto(chunk.QdbStatements),
	}
	_, err := conn.ExecNoTran(ctx, request)
	return err
}

func (a *Adapter) CommitTran(ctx context.Context, transaction *mtran.MetaTransaction) error {
	conn := proto.NewMetaTransactionServiceClient(a.conn)
	request := &proto.MetaTransactionRequest{
		TransactionId: transaction.TransactionId.String(),
		MetaCmdList:   transaction.Operations.GossipRequests,
		CmdList:       qdb.SliceToProto(transaction.Operations.QdbStatements),
	}
	_, err := conn.CommitTran(ctx, request)
	return err
}

func (a *Adapter) BeginTran(ctx context.Context) (*mtran.MetaTransaction, error) {
	conn := proto.NewMetaTransactionServiceClient(a.conn)
	if transactionProto, err := conn.BeginTran(ctx, nil); err != nil {
		return nil, err
	} else {
		if transactionMeta, err := mtran.TransactionFromProto(transactionProto); err != nil {
			return nil, err
		} else {
			return transactionMeta, nil
		}
	}
}

// CreateUniqueIndex implements meta.EntityMgr.
func (a *Adapter) CreateUniqueIndex(ctx context.Context, dsId string, idx *distributions.UniqueIndex) error {
	c := proto.NewDistributionServiceClient(a.conn)
	_, err := c.CreateUniqueIndex(ctx, &proto.CreateUniqueIndexRequest{
		DistributionId: dsId,
		Idx:            distributions.UniqueIndexToProto(idx),
	})
	return err
}

// DropUniqueIndex implements meta.EntityMgr.
func (a *Adapter) DropUniqueIndex(ctx context.Context, idxId string) error {
	c := proto.NewDistributionServiceClient(a.conn)
	_, err := c.DropUniqueIndex(ctx, &proto.DropUniqueIndexRequest{
		IdxId: idxId,
	})
	return err
}

// ListDistributionIndexes implements meta.EntityMgr.
func (a *Adapter) ListDistributionIndexes(ctx context.Context, dsId string) (map[string]*distributions.UniqueIndex, error) {
	c := proto.NewDistributionServiceClient(a.conn)
	idxs, err := c.ListDistributionUniqueIndexes(ctx, &proto.ListDistributionUniqueIndexesRequest{
		DistributionId: dsId,
	})
	if err != nil {
		return nil, err
	}
	res := make(map[string]*distributions.UniqueIndex)
	for id, idx := range idxs.Indexes {
		res[id] = distributions.UniqueIndexFromProto(idx)
	}
	return res, nil
}

// ListDistributionIndexes implements meta.EntityMgr.
func (a *Adapter) ListUniqueIndexes(ctx context.Context) (map[string]*distributions.UniqueIndex, error) {
	c := proto.NewDistributionServiceClient(a.conn)
	idxs, err := c.ListUniqueIndexes(ctx, nil)
	if err != nil {
		return nil, err
	}
	res := make(map[string]*distributions.UniqueIndex)
	for id, idx := range idxs.Indexes {
		res[id] = distributions.UniqueIndexFromProto(idx)
	}
	return res, nil
}

// ListDistributionIndexes implements meta.EntityMgr.
func (a *Adapter) ListRelationIndexes(ctx context.Context, relName *rfqn.RelationFQN) (map[string]*distributions.UniqueIndex, error) {
	c := proto.NewDistributionServiceClient(a.conn)
	idxs, err := c.ListRelationUniqueIndexes(ctx, &proto.ListRelationUniqueIndexesRequest{RelationName: /* fix that */ relName.RelationName})
	if err != nil {
		return nil, err
	}
	res := make(map[string]*distributions.UniqueIndex)
	for id, idx := range idxs.Indexes {
		res[id] = distributions.UniqueIndexFromProto(idx)
	}
	return res, nil
}
