package coord

import (
	"context"

	"github.com/pg-sharding/spqr/pkg/models/tasks"

	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/meta"
	"github.com/pg-sharding/spqr/pkg/models/spqrerror"

	"github.com/pg-sharding/spqr/pkg/models/datashards"
	"github.com/pg-sharding/spqr/pkg/models/distributions"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/pkg/models/topology"
	proto "github.com/pg-sharding/spqr/pkg/protos"
	"github.com/pg-sharding/spqr/qdb"
	"google.golang.org/grpc"
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

//
// Parameters:
// - None.
//
// Returns:
// - qdb.QDB: The QDB object.
func (a *Adapter) QDB() qdb.QDB {
	panic("implement me?")
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

	return kr.KeyRangeFromProto(reply.KeyRangesInfo[0], ds.Distribution.ColumnTypes), nil
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
		return nil, err
	}

	dc := proto.NewDistributionServiceClient(a.conn)
	ds, err := dc.GetDistribution(ctx, &proto.GetDistributionRequest{Id: distribution})
	if err != nil {
		return nil, err
	}

	krs := make([]*kr.KeyRange, len(reply.KeyRangesInfo))
	for i, keyRange := range reply.KeyRangesInfo {
		krs[i] = kr.KeyRangeFromProto(keyRange, ds.Distribution.ColumnTypes)
	}

	return krs, nil
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
	reply, err := c.ListAllKeyRanges(ctx, &proto.ListAllKeyRangesRequest{})
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
		krs[i] = kr.KeyRangeFromProto(keyRange, ds.Distribution.ColumnTypes)
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
	return err
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
			return err
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
	return err
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
	_, err := c.DropAllKeyRanges(ctx, &proto.DropAllKeyRangesRequest{})
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
	resp, err := c.ListRouters(ctx, &proto.ListRoutersRequest{})
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
// - shard (*datashards.DataShard): The datashards.DataShard instance to add.
//
// Returns:
// - error: An error if the data shard addition fails, otherwise nil.
func (a *Adapter) AddDataShard(ctx context.Context, shard *datashards.DataShard) error {
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
// - shard (*datashards.DataShard): The datashards.DataShard instance to add.
//
// Returns:
// - error: An error if the world shard addition fails, otherwise nil.
func (a *Adapter) AddWorldShard(ctx context.Context, shard *datashards.DataShard) error {
	return spqrerror.New(spqrerror.SPQR_NOT_IMPLEMENTED, "addWorldShard not implemented")
}

// TODO : unit tests

// ListShards retrieves a list of data shards from the system.
//
// Parameters:
// - ctx (context.Context): The context for the request.
//
// Returns:
// - []*datashards.DataShard: A list of data shards.
// - error: An error if the retrieval of shards fails, otherwise nil.
func (a *Adapter) ListShards(ctx context.Context) ([]*datashards.DataShard, error) {
	c := proto.NewShardServiceClient(a.conn)
	resp, err := c.ListShards(ctx, &proto.ListShardsRequest{})
	shards := resp.Shards
	var ds []*datashards.DataShard
	for _, shard := range shards {
		ds = append(ds, &datashards.DataShard{
			ID:  shard.Id,
			Cfg: &config.Shard{Hosts: shard.Hosts},
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
// - *datashards.DataShard: The retrieved data shard.
// - error: An error if the retrieval of the shard fails, otherwise nil.
func (a *Adapter) GetShard(ctx context.Context, shardID string) (*datashards.DataShard, error) {
	c := proto.NewShardServiceClient(a.conn)
	resp, err := c.GetShard(ctx, &proto.ShardRequest{Id: shardID})
	return &datashards.DataShard{
		ID:  resp.Shard.Id,
		Cfg: &config.Shard{Hosts: resp.Shard.Hosts},
	}, err
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

	resp, err := c.ListDistributions(ctx, &proto.ListDistributionsRequest{})
	if err != nil {
		return nil, err
	}

	dss := make([]*distributions.Distribution, len(resp.Distributions))
	for i, ds := range resp.Distributions {
		dss[i] = distributions.DistributionFromProto(ds)
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
func (a *Adapter) CreateDistribution(ctx context.Context, ds *distributions.Distribution) error {
	c := proto.NewDistributionServiceClient(a.conn)

	_, err := c.CreateDistribution(ctx, &proto.CreateDistributionRequest{
		Distributions: []*proto.Distribution{
			distributions.DistributionToProto(ds),
		},
	})
	return err
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

	return err
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
		dRels = append(dRels, distributions.DistributedRelatitonToProto(r))
	}

	_, err := c.AlterDistributionAttach(ctx, &proto.AlterDistributionAttachRequest{
		Id:        id,
		Relations: dRels,
	})

	return err
}

// AlterDistributionDetach detaches a relation from a distribution using the provided ID and relation name.
//
// Parameters:
// - ctx (context.Context): The context for the request.
// - id (string): The ID of the distribution to detach from.
// - relName (string): The name of the relation to detach.
//
// Returns:
// - error: An error if the detachment fails, otherwise nil.
func (a *Adapter) AlterDistributionDetach(ctx context.Context, id string, relName string) error {
	c := proto.NewDistributionServiceClient(a.conn)

	_, err := c.AlterDistributionDetach(ctx, &proto.AlterDistributionDetachRequest{
		Id:       id,
		RelNames: []string{relName},
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
		return nil, err
	}

	return distributions.DistributionFromProto(resp.Distribution), nil
}

// GetRelationDistribution retrieves the distribution related to a specific relation from the system.
//
// Parameters:
// - ctx (context.Context): The context for the request.
// - id (string): The ID of the relation (type: string).
//
// Returns:
// - *distributions.Distribution: The retrieved distribution related to the relation.
// - error: An error if the retrieval of the distribution fails, otherwise nil.
func (a *Adapter) GetRelationDistribution(ctx context.Context, id string) (*distributions.Distribution, error) {
	c := proto.NewDistributionServiceClient(a.conn)

	resp, err := c.GetRelationDistribution(ctx, &proto.GetRelationDistributionRequest{
		Id: id,
	})
	if err != nil {
		return nil, err
	}

	return distributions.DistributionFromProto(resp.Distribution), nil
}

// GetTaskGroup retrieves the task group from the system.
//
// Parameters:
// - ctx (context.Context): The context for the request.
//
// Returns:
// - *tasks.TaskGroup: The retrieved task group.
// - error: An error if the retrieval of the task group fails, otherwise nil.
func (a *Adapter) GetTaskGroup(ctx context.Context) (*tasks.TaskGroup, error) {
	tasksService := proto.NewTasksServiceClient(a.conn)
	res, err := tasksService.GetTaskGroup(ctx, &proto.GetTaskGroupRequest{})
	if err != nil {
		return nil, err
	}
	return tasks.TaskGroupFromProto(res.TaskGroup), nil
}

// WriteTaskGroup writes a task group to the system.
//
// Parameters:
// - ctx (context.Context): The context for the request.
// - taskGroup (*tasks.TaskGroup): The task group to be written.
//
// Returns:
// - error: An error if the writing of the task group fails, otherwise nil.
func (a *Adapter) WriteTaskGroup(ctx context.Context, taskGroup *tasks.TaskGroup) error {
	tasksService := proto.NewTasksServiceClient(a.conn)
	_, err := tasksService.WriteTaskGroup(ctx, &proto.WriteTaskGroupRequest{
		TaskGroup: tasks.TaskGroupToProto(taskGroup),
	})
	return err
}


// RemoveTaskGroup removes a task group from the system.
//
// Parameters:
// - ctx (context.Context): The context for the request.
//
// Returns:
// - error: An error if the removal of the task group fails, otherwise nil.
func (a *Adapter) RemoveTaskGroup(ctx context.Context) error {
	tasksService := proto.NewTasksServiceClient(a.conn)
	_, err := tasksService.RemoveTaskGroup(ctx, &proto.RemoveTaskGroupRequest{})
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
	resp, err := c.GetCoordinator(ctx, &proto.GetCoordinatorRequest{})
	return resp.Address, err
}
