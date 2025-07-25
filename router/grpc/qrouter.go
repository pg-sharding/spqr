package grpc

import (
	"context"
	"fmt"

	"github.com/pg-sharding/spqr/pkg/client"
	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/meta"
	"github.com/pg-sharding/spqr/pkg/models/distributions"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/pkg/models/rrelation"
	"github.com/pg-sharding/spqr/pkg/models/spqrerror"
	"github.com/pg-sharding/spqr/pkg/models/tasks"
	"github.com/pg-sharding/spqr/pkg/models/topology"
	"github.com/pg-sharding/spqr/pkg/pool"
	protos "github.com/pg-sharding/spqr/pkg/protos"
	"github.com/pg-sharding/spqr/pkg/shard"
	"github.com/pg-sharding/spqr/router/qrouter"
	"github.com/pg-sharding/spqr/router/rfqn"
	"github.com/pg-sharding/spqr/router/rulerouter"
	"google.golang.org/grpc/reflection"
	"google.golang.org/protobuf/types/known/emptypb"
)

type LocalQrouterServer struct {
	protos.UnimplementedKeyRangeServiceServer
	protos.UnimplementedShardingRulesServiceServer
	protos.UnimplementedRouterServiceServer
	protos.UnimplementedTopologyServiceServer
	protos.UnimplementedClientInfoServiceServer
	protos.UnimplementedBackendConnectionsServiceServer
	protos.UnimplementedPoolServiceServer
	protos.UnimplementedDistributionServiceServer
	protos.UnimplementedMoveTasksServiceServer
	protos.UnimplementedShardServiceServer
	protos.UnimplementedBalancerTaskServiceServer
	protos.UnimplementedReferenceRelationsServiceServer

	qr  qrouter.QueryRouter
	mgr meta.EntityMgr
	rr  rulerouter.RuleRouter
}

// CreateReferenceRelations implements proto.ReferenceRelationsServiceServer.
func (l *LocalQrouterServer) CreateReferenceRelations(ctx context.Context, request *protos.CreateReferenceRelationsRequest) (*emptypb.Empty, error) {

	if err := l.mgr.CreateReferenceRelation(ctx,
		rrelation.RefRelationFromProto(request.Relation),
		rrelation.AutoIncrementEntriesFromProto(request.Entries)); err != nil {
		return nil, err
	}

	return nil, nil
}

// CreateReferenceRelations implements proto.ReferenceRelationsServiceServer.
func (l *LocalQrouterServer) AlterReferenceRelationStorage(ctx context.Context, request *protos.AlterReferenceRelationStorageRequest) (*emptypb.Empty, error) {

	if err := l.mgr.AlterReferenceRelationStorage(ctx, &rfqn.RelationFQN{
		RelationName: request.Relation.RelationName,
		SchemaName:   request.Relation.SchemaName,
	}, request.ShardIds); err != nil {
		return nil, err
	}

	return nil, nil
}

// DropReferenceRelations implements proto.ReferenceRelationsServiceServer.
func (l *LocalQrouterServer) DropReferenceRelations(ctx context.Context, r *protos.DropReferenceRelationsRequest) (*emptypb.Empty, error) {
	for _, qualName := range r.GetRelations() {
		/* XXX: fix this to support schema */
		if err := l.mgr.DropReferenceRelation(ctx, &rfqn.RelationFQN{RelationName: qualName.RelationName, SchemaName: qualName.SchemaName}); err != nil {
			return nil, err
		}
	}

	return nil, nil
}

// ListReferenceRelations implements proto.ReferenceRelationsServiceServer.
func (l *LocalQrouterServer) ListReferenceRelations(ctx context.Context, _ *emptypb.Empty) (*protos.ListReferenceRelationsReply, error) {
	var ret []*protos.ReferenceRelation

	res, err := l.mgr.ListReferenceRelations(ctx)
	if err != nil {
		return nil, err
	}

	for _, r := range res {
		ret = append(ret, rrelation.RefRelationToProto(r))
	}

	return &protos.ListReferenceRelationsReply{
		Relations: ret,
	}, nil
}

// SyncReferenceRelations implements proto.ReferenceRelationsServiceServer.
func (l *LocalQrouterServer) SyncReferenceRelations(context.Context, *protos.SyncReferenceRelationsRequest) (*emptypb.Empty, error) {
	return nil, fmt.Errorf("local query router in unable to sync reference relation")
}

func (l *LocalQrouterServer) ListShards(ctx context.Context, _ *emptypb.Empty) (*protos.ListShardsReply, error) {
	shards, err := l.mgr.ListShards(ctx)
	if err != nil {
		return nil, err
	}
	return &protos.ListShardsReply{
		Shards: func() []*protos.Shard {
			res := make([]*protos.Shard, len(shards))
			for i, sh := range shards {
				res[i] = topology.DataShardToProto(sh)
			}
			return res
		}(),
	}, nil
}

func (l *LocalQrouterServer) AddDataShard(ctx context.Context, request *protos.AddShardRequest) (*emptypb.Empty, error) {
	if err := l.mgr.AddDataShard(ctx, topology.DataShardFromProto(request.GetShard())); err != nil {
		return nil, err
	}
	return nil, nil
}

func (l *LocalQrouterServer) AddWorldShard(ctx context.Context, request *protos.AddWorldShardRequest) (*emptypb.Empty, error) {
	panic("LocalQrouterServer.AddWorldShard not implemented")
}

func (l *LocalQrouterServer) GetShard(ctx context.Context, request *protos.ShardRequest) (*protos.ShardReply, error) {
	sh, err := l.mgr.GetShard(ctx, request.Id)
	if err != nil {
		return nil, err
	}
	return &protos.ShardReply{
		Shard: topology.DataShardToProto(sh),
	}, nil
}

// CreateDistribution creates distribution in QDB
// TODO: unit tests
func (l *LocalQrouterServer) CreateDistribution(ctx context.Context, request *protos.CreateDistributionRequest) (*emptypb.Empty, error) {
	for _, ds := range request.GetDistributions() {
		if err := l.mgr.CreateDistribution(ctx, distributions.DistributionFromProto(ds)); err != nil {
			return nil, err
		}
	}
	return nil, nil
}

// DropDistribution deletes distribution from QDB
// TODO: unit tests
func (l *LocalQrouterServer) DropDistribution(ctx context.Context, request *protos.DropDistributionRequest) (*emptypb.Empty, error) {
	for _, id := range request.GetIds() {
		if err := l.mgr.DropDistribution(ctx, id); err != nil {
			return nil, err
		}
	}
	return nil, nil
}

// ListDistributions returns all distributions from QDB
// TODO: unit tests
func (l *LocalQrouterServer) ListDistributions(ctx context.Context, _ *emptypb.Empty) (*protos.ListDistributionsReply, error) {
	distrs, err := l.mgr.ListDistributions(ctx)
	if err != nil {
		return nil, err
	}
	return &protos.ListDistributionsReply{
		Distributions: func() []*protos.Distribution {
			res := make([]*protos.Distribution, len(distrs))
			for i, ds := range distrs {
				res[i] = distributions.DistributionToProto(ds)
			}
			return res
		}(),
	}, nil
}

// AlterDistributionAttach attaches relation to distribution
// TODO: unit tests
func (l *LocalQrouterServer) AlterDistributionAttach(ctx context.Context, request *protos.AlterDistributionAttachRequest) (*emptypb.Empty, error) {
	return nil, l.mgr.AlterDistributionAttach(
		ctx,
		request.GetId(),
		func() []*distributions.DistributedRelation {
			res := make([]*distributions.DistributedRelation, len(request.GetRelations()))
			for i, rel := range request.GetRelations() {
				res[i] = distributions.DistributedRelationFromProto(rel)
			}
			return res
		}(),
	)
}

// AlterDistributionDetach detaches relation from distribution
// TODO: unit tests
func (l *LocalQrouterServer) AlterDistributionDetach(ctx context.Context, request *protos.AlterDistributionDetachRequest) (*emptypb.Empty, error) {
	for _, relName := range request.GetRelNames() {
		qualifiedName := &rfqn.RelationFQN{RelationName: relName.RelationName, SchemaName: relName.SchemaName}
		if err := l.mgr.AlterDistributionDetach(ctx, request.GetId(), qualifiedName); err != nil {
			return nil, err
		}
	}
	return nil, nil
}

// AlterDistributedRelation alters the distributed relation
// TODO: unit tests
func (l *LocalQrouterServer) AlterDistributedRelation(ctx context.Context, request *protos.AlterDistributedRelationRequest) (*emptypb.Empty, error) {
	return nil, l.mgr.AlterDistributedRelation(ctx, request.GetId(), distributions.DistributedRelationFromProto(request.GetRelation()))
}

// GetDistribution retrieves info about distribution from QDB
// TODO: unit tests
func (l *LocalQrouterServer) GetDistribution(ctx context.Context, request *protos.GetDistributionRequest) (*protos.GetDistributionReply, error) {
	ds, err := l.mgr.GetDistribution(ctx, request.GetId())
	if err != nil {
		return nil, err
	}
	return &protos.GetDistributionReply{Distribution: distributions.DistributionToProto(ds)}, err
}

// GetRelationDistribution retrieves info about distribution attached to relation from QDB
// TODO: unit tests
func (l *LocalQrouterServer) GetRelationDistribution(ctx context.Context, request *protos.GetRelationDistributionRequest) (*protos.GetRelationDistributionReply, error) {
	qualifiedName := rfqn.RelationFQN{RelationName: request.Name, SchemaName: request.SchemaName}
	ds, err := l.mgr.GetRelationDistribution(ctx, &qualifiedName)
	if err != nil {
		return nil, err
	}
	return &protos.GetRelationDistributionReply{Distribution: distributions.DistributionToProto(ds)}, err
}

// TODO : unit tests
func (l *LocalQrouterServer) OpenRouter(ctx context.Context, _ *emptypb.Empty) (*emptypb.Empty, error) {
	l.qr.Initialize()
	return nil, nil
}

// TODO : unit tests
func (l *LocalQrouterServer) GetRouterStatus(ctx context.Context, _ *emptypb.Empty) (*protos.GetRouterStatusReply, error) {
	if l.qr.Initialized() {
		return &protos.GetRouterStatusReply{
			Status: protos.RouterStatus_OPENED,
		}, nil
	}
	return &protos.GetRouterStatusReply{
		Status: protos.RouterStatus_CLOSED,
	}, nil
}

// TODO : implement, unit tests
func (l *LocalQrouterServer) CloseRouter(ctx context.Context, _ *emptypb.Empty) (*emptypb.Empty, error) {
	panic("LocalQrouterServer.CloseRouter not implemented")
}

// TODO : unit tests
func (l *LocalQrouterServer) DropKeyRange(ctx context.Context, request *protos.DropKeyRangeRequest) (*protos.ModifyReply, error) {
	for _, id := range request.Id {
		err := l.mgr.DropKeyRange(ctx, id)
		if err != nil {
			return nil, err
		}
	}
	return &protos.ModifyReply{}, nil
}

// TODO : unit tests
func (l *LocalQrouterServer) DropAllKeyRanges(ctx context.Context, _ *emptypb.Empty) (*protos.DropAllKeyRangesResponse, error) {
	if err := l.mgr.DropKeyRangeAll(ctx); err != nil {
		return nil, err
	}
	return &protos.DropAllKeyRangesResponse{}, nil
}

// TODO : unit tests
func (l *LocalQrouterServer) MoveKeyRange(ctx context.Context, request *protos.MoveKeyRangeRequest) (*protos.ModifyReply, error) {
	err := l.mgr.Move(ctx, &kr.MoveKeyRange{Krid: request.Id, ShardId: request.ToShardId})
	if err != nil {
		return nil, err
	}

	return &protos.ModifyReply{}, nil
}

// TODO : unit tests
func (l *LocalQrouterServer) AddShardingRules(ctx context.Context, request *protos.AddShardingRuleRequest) (*emptypb.Empty, error) {
	return nil, spqrerror.ShardingRulesRemoved
}

// TODO : unit tests
func (l *LocalQrouterServer) ListShardingRules(ctx context.Context, request *protos.ListShardingRuleRequest) (*protos.ListShardingRuleReply, error) {
	return nil, spqrerror.ShardingRulesRemoved
}

// TODO : unit tests
func (l *LocalQrouterServer) DropShardingRules(ctx context.Context, request *protos.DropShardingRuleRequest) (*emptypb.Empty, error) {
	return nil, spqrerror.ShardingRulesRemoved
}

// TODO : unit tests
func (l *LocalQrouterServer) CreateKeyRange(ctx context.Context, request *protos.CreateKeyRangeRequest) (*protos.ModifyReply, error) {
	ds, err := l.mgr.GetDistribution(ctx, request.KeyRangeInfo.DistributionId)
	if err != nil {
		return nil, err
	}
	err = l.mgr.CreateKeyRange(ctx, kr.KeyRangeFromProto(request.KeyRangeInfo, ds.ColTypes))
	if err != nil {
		return nil, err
	}

	return &protos.ModifyReply{}, nil
}

// GetKeyRange gets key ranges with given ids
// TODO unit tests
func (l *LocalQrouterServer) GetKeyRange(ctx context.Context, request *protos.GetKeyRangeRequest) (*protos.KeyRangeReply, error) {
	res := make([]*protos.KeyRangeInfo, 0)
	for _, id := range request.Ids {
		krg, err := l.mgr.GetKeyRange(ctx, id)
		if err != nil {
			return nil, err
		}
		if krg != nil {
			res = append(res, krg.ToProto())
		}
	}
	return &protos.KeyRangeReply{KeyRangesInfo: res}, nil
}

// TODO : unit tests
func (l *LocalQrouterServer) ListKeyRange(ctx context.Context, request *protos.ListKeyRangeRequest) (*protos.KeyRangeReply, error) {
	var krs []*protos.KeyRangeInfo

	krsqdb, err := l.mgr.ListKeyRanges(ctx, request.Distribution)
	if err != nil {
		return nil, err
	}

	for _, keyRange := range krsqdb {
		krs = append(krs, keyRange.ToProto())
	}

	return &protos.KeyRangeReply{
		KeyRangesInfo: krs,
	}, nil
}

// TODO : unit tests
func (l *LocalQrouterServer) ListAllKeyRanges(ctx context.Context, _ *emptypb.Empty) (*protos.KeyRangeReply, error) {
	krsDb, err := l.mgr.ListAllKeyRanges(ctx)
	if err != nil {
		return nil, err
	}

	krs := make([]*protos.KeyRangeInfo, len(krsDb))

	for i, krg := range krsDb {
		krs[i] = krg.ToProto()
	}

	return &protos.KeyRangeReply{KeyRangesInfo: krs}, nil
}

// TODO : unit tests
func (l *LocalQrouterServer) LockKeyRange(ctx context.Context, request *protos.LockKeyRangeRequest) (*protos.ModifyReply, error) {
	for _, id := range request.Id {
		if _, err := l.mgr.LockKeyRange(ctx, id); err != nil {
			return nil, err
		}
	}
	return &protos.ModifyReply{}, nil
}

// TODO : unit tests
func (l *LocalQrouterServer) UnlockKeyRange(ctx context.Context, request *protos.UnlockKeyRangeRequest) (*protos.ModifyReply, error) {
	for _, id := range request.Id {
		if err := l.mgr.UnlockKeyRange(ctx, id); err != nil {
			return nil, err
		}
	}
	return &protos.ModifyReply{}, nil
}

// TODO : unit tests
func (l *LocalQrouterServer) SplitKeyRange(ctx context.Context, request *protos.SplitKeyRangeRequest) (*protos.ModifyReply, error) {
	if err := l.mgr.Split(ctx, &kr.SplitKeyRange{
		Krid:      request.NewId,
		SourceID:  request.SourceId,
		Bound:     [][]byte{request.Bound}, // TODO: fix
		SplitLeft: request.SplitLeft,
	}); err != nil {
		return nil, err
	}

	return &protos.ModifyReply{}, nil
}

// TODO : unit tests
func (l *LocalQrouterServer) MergeKeyRange(ctx context.Context, request *protos.MergeKeyRangeRequest) (*protos.ModifyReply, error) {
	if err := l.mgr.Unite(ctx, &kr.UniteKeyRange{
		BaseKeyRangeId:      request.GetBaseId(),
		AppendageKeyRangeId: request.GetAppendageId(),
	}); err != nil {
		return nil, err
	}

	return &protos.ModifyReply{}, nil
}

// TODO: unit tests
func (l *LocalQrouterServer) RenameKeyRange(ctx context.Context, request *protos.RenameKeyRangeRequest) (*emptypb.Empty, error) {
	return nil, l.mgr.RenameKeyRange(ctx, request.KeyRangeId, request.NewKeyRangeId)
}

// TODO : unit tests
func ClientToProto(cl client.ClientInfo) *protos.ClientInfo {
	clientInfo := &protos.ClientInfo{
		ClientId: uint64(cl.ID()),
		User:     cl.Usr(),
		Dbname:   cl.DB(),
		Shards:   make([]*protos.UsedShardInfo, 0, len(cl.Shards())),
	}
	for _, shard := range cl.Shards() {
		clientInfo.Shards = append(clientInfo.Shards, &protos.UsedShardInfo{
			Instance: &protos.DBInstanceInfo{
				Hostname: shard.Instance().Hostname(),
			},
		})
	}
	return clientInfo
}

// TODO : unit tests
func ShardToProto(sh shard.ShardHostCtl) *protos.BackendConnectionsInfo {
	shardInfo := &protos.BackendConnectionsInfo{
		BackendConnectionId: uint64(sh.ID()),
		ShardKeyName:        sh.ShardKeyName(),
		User:                sh.Usr(),
		Dbname:              sh.DB(),
		Hostname:            sh.InstanceHostname(),
		Sync:                sh.Sync(),
		TxServed:            sh.TxServed(),
		TxStatus:            int64(sh.TxStatus()),
	}
	return shardInfo
}

// TODO : unit tests
func PoolToProto(p pool.Pool, router string) *protos.PoolInfo {
	statistics := p.View()
	poolInfo := &protos.PoolInfo{
		Id:            fmt.Sprintf("%p", p),
		DB:            statistics.DB,
		Usr:           statistics.Usr,
		Host:          statistics.Hostname,
		RouterName:    router,
		ConnCount:     int64(statistics.UsedConnections),
		IdleConnCount: int64(statistics.IdleConnections),
		QueueSize:     int64(statistics.QueueResidualSize),
	}
	return poolInfo
}

// TODO : unit tests
func (l *LocalQrouterServer) ListClients(context.Context, *emptypb.Empty) (*protos.ListClientsReply, error) {
	reply := &protos.ListClientsReply{}

	err := l.rr.ClientPoolForeach(func(client client.ClientInfo) error {
		reply.Clients = append(reply.Clients, ClientToProto(client))
		return nil
	})
	return reply, err
}

// TODO : unit tests
func (l *LocalQrouterServer) ListBackendConnections(context.Context, *emptypb.Empty) (*protos.ListBackendConnectionsReply, error) {
	reply := &protos.ListBackendConnectionsReply{}

	err := l.rr.ForEach(func(sh shard.ShardHostCtl) error {
		reply.Conns = append(reply.Conns, ShardToProto(sh))
		return nil
	})
	return reply, err
}

// TODO : unit tests
func (l *LocalQrouterServer) ListPools(context.Context, *emptypb.Empty) (*protos.ListPoolsResponse, error) {
	reply := &protos.ListPoolsResponse{}

	err := l.rr.ForEachPool(func(p pool.Pool) error {
		reply.Pools = append(reply.Pools, PoolToProto(p, config.RouterConfig().Host))
		return nil
	})
	return reply, err
}

// TODO : unit tests
func (l *LocalQrouterServer) UpdateCoordinator(ctx context.Context, req *protos.UpdateCoordinatorRequest) (*emptypb.Empty, error) {
	return nil, l.mgr.UpdateCoordinator(ctx, req.Address)
}

func (l *LocalQrouterServer) GetCoordinator(ctx context.Context, _ *emptypb.Empty) (*protos.GetCoordinatorResponse, error) {
	reply := &protos.GetCoordinatorResponse{}
	re, err := l.mgr.GetCoordinator(ctx)
	reply.Address = re
	return reply, err
}

// TODO: unit tests
func (l *LocalQrouterServer) GetMoveTaskGroup(ctx context.Context, _ *emptypb.Empty) (*protos.GetMoveTaskGroupReply, error) {
	group, err := l.mgr.GetMoveTaskGroup(ctx)
	if err != nil {
		return nil, err
	}
	return &protos.GetMoveTaskGroupReply{
		TaskGroup: tasks.TaskGroupToProto(group),
	}, nil
}

// TODO: unit tests
func (l *LocalQrouterServer) WriteMoveTaskGroup(ctx context.Context, request *protos.WriteMoveTaskGroupRequest) (*emptypb.Empty, error) {
	return nil, l.mgr.WriteMoveTaskGroup(ctx, tasks.TaskGroupFromProto(request.TaskGroup))
}

// TODO: unit tests
func (l *LocalQrouterServer) RemoveMoveTaskGroup(ctx context.Context, _ *emptypb.Empty) (*emptypb.Empty, error) {
	return nil, l.mgr.RemoveMoveTaskGroup(ctx)
}

// TODO: unit tests
func (l *LocalQrouterServer) RetryMoveTaskGroup(ctx context.Context, _ *emptypb.Empty) (*emptypb.Empty, error) {
	return nil, l.mgr.RetryMoveTaskGroup(ctx)
}

// TODO: unit tests
func (l *LocalQrouterServer) GetBalancerTask(ctx context.Context, _ *emptypb.Empty) (*protos.GetBalancerTaskReply, error) {
	task, err := l.mgr.GetBalancerTask(ctx)
	if err != nil {
		return nil, err
	}
	return &protos.GetBalancerTaskReply{Task: tasks.BalancerTaskToProto(task)}, nil
}

// TODO: unit tests
func (l *LocalQrouterServer) WriteBalancerTask(ctx context.Context, request *protos.WriteBalancerTaskRequest) (*emptypb.Empty, error) {
	return nil, l.mgr.WriteBalancerTask(ctx, tasks.BalancerTaskFromProto(request.Task))
}

// TODO: unit tests
func (l *LocalQrouterServer) RemoveBalancerTask(ctx context.Context, _ *emptypb.Empty) (*emptypb.Empty, error) {
	return nil, l.mgr.RemoveBalancerTask(ctx)
}

// TODO : unit tests
func (l *LocalQrouterServer) DropSequence(ctx context.Context, request *protos.DropSequenceRequest) (*emptypb.Empty, error) {
	err := l.mgr.DropSequence(ctx, request.Name, request.Force)
	return nil, err
}

func Register(server reflection.GRPCServer, qrouter qrouter.QueryRouter, mgr meta.EntityMgr, rr rulerouter.RuleRouter) {

	lqr := &LocalQrouterServer{
		qr:  qrouter,
		mgr: mgr,
		rr:  rr,
	}

	reflection.Register(server)

	protos.RegisterKeyRangeServiceServer(server, lqr)
	protos.RegisterShardingRulesServiceServer(server, lqr)
	protos.RegisterRouterServiceServer(server, lqr)
	protos.RegisterTopologyServiceServer(server, lqr)
	protos.RegisterClientInfoServiceServer(server, lqr)
	protos.RegisterBackendConnectionsServiceServer(server, lqr)
	protos.RegisterPoolServiceServer(server, lqr)
	protos.RegisterDistributionServiceServer(server, lqr)
	protos.RegisterMoveTasksServiceServer(server, lqr)
	protos.RegisterBalancerTaskServiceServer(server, lqr)
	protos.RegisterReferenceRelationsServiceServer(server, lqr)
}

var _ protos.KeyRangeServiceServer = &LocalQrouterServer{}
var _ protos.ShardingRulesServiceServer = &LocalQrouterServer{}
var _ protos.RouterServiceServer = &LocalQrouterServer{}
var _ protos.ClientInfoServiceServer = &LocalQrouterServer{}
var _ protos.BackendConnectionsServiceServer = &LocalQrouterServer{}
var _ protos.PoolServiceServer = &LocalQrouterServer{}
var _ protos.DistributionServiceServer = &LocalQrouterServer{}
var _ protos.MoveTasksServiceServer = &LocalQrouterServer{}
var _ protos.BalancerTaskServiceServer = &LocalQrouterServer{}
var _ protos.ShardServiceServer = &LocalQrouterServer{}
var _ protos.ReferenceRelationsServiceServer = &LocalQrouterServer{}
