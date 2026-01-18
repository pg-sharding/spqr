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
	"github.com/pg-sharding/spqr/pkg/models/tasks"
	"github.com/pg-sharding/spqr/pkg/models/topology"
	mtran "github.com/pg-sharding/spqr/pkg/models/transaction"
	"github.com/pg-sharding/spqr/pkg/pool"
	protos "github.com/pg-sharding/spqr/pkg/protos"
	"github.com/pg-sharding/spqr/pkg/shard"
	"github.com/pg-sharding/spqr/qdb"
	"github.com/pg-sharding/spqr/router/qrouter"
	"github.com/pg-sharding/spqr/router/rfqn"
	"github.com/pg-sharding/spqr/router/rulerouter"
	"google.golang.org/grpc/reflection"
	"google.golang.org/protobuf/types/known/emptypb"
)

type LocalQrouterServer struct {
	protos.UnimplementedKeyRangeServiceServer
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
	protos.UnimplementedMetaTransactionGossipServiceServer

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

func (l *LocalQrouterServer) DropDataShard(ctx context.Context, request *protos.ShardRequest) (*emptypb.Empty, error) {
	return nil, l.mgr.DropShard(ctx, request.Id)
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

func (l *LocalQrouterServer) createDistributionPrepare(ctx context.Context, gossip *protos.CreateDistributionGossip) ([]qdb.QdbStatement, error) {
	result := make([]qdb.QdbStatement, 0, len(gossip.GetDistributions()))
	for _, ds := range gossip.GetDistributions() {
		mds, err := distributions.DistributionFromProto(ds)
		if err != nil {
			return nil, err
		}
		if tranChunk, err := l.mgr.CreateDistribution(ctx, mds); err != nil {
			return nil, err
		} else {
			if len(tranChunk.QdbStatements) == 0 {
				return nil, fmt.Errorf("transaction chunk must have a qdb statement (createDistributionPrepare)")
			}
			result = append(result, tranChunk.QdbStatements...)
		}
	}
	return result, nil
}

// CreateDistribution creates distribution in QDB
func (l *LocalQrouterServer) CreateDistribution(ctx context.Context, request *protos.CreateDistributionRequest) (*protos.CreateDistributionReply, error) {
	return nil, fmt.Errorf("DEPRECATED, remove after meta transaction implementation")
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

	res := make([]*distributions.DistributedRelation, len(request.GetRelations()))
	for i, rel := range request.GetRelations() {
		var err error
		res[i], err = distributions.DistributedRelationFromProto(rel, map[string]*distributions.UniqueIndex{})
		if err != nil {
			return nil, err
		}
	}
	return nil, l.mgr.AlterDistributionAttach(
		ctx,
		request.GetId(),
		res,
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
	ds, err := l.mgr.GetRelationDistribution(ctx, &rfqn.RelationFQN{RelationName: request.Relation.Name, SchemaName: request.Relation.SchemaName})
	if err != nil {
		return nil, err
	}
	curRel, ok := ds.Relations[request.Relation.Name]
	if !ok {
		return nil, fmt.Errorf("relation \"%s\" not found in distribution \"%s\"", request.Relation.Name, ds.Id)
	}
	rel, err := distributions.DistributedRelationFromProto(request.GetRelation(), curRel.UniqueIndexesByColumn)
	if err != nil {
		return nil, err
	}
	return nil, l.mgr.AlterDistributedRelation(ctx, request.GetId(), rel)
}

// AlterDistributedRelation alters the distributed relation
// TODO: unit tests
func (l *LocalQrouterServer) AlterDistributedRelationSchema(ctx context.Context, request *protos.AlterDistributedRelationSchemaRequest) (*emptypb.Empty, error) {
	return nil, l.mgr.AlterDistributedRelationSchema(ctx, request.GetId(), request.GetRelationName(), request.GetSchemaName())
}

// AlterDistributedRelation alters the distributed relation
// TODO: unit tests
func (l *LocalQrouterServer) AlterDistributedRelationDistributionKey(ctx context.Context, request *protos.AlterDistributedRelationDistributionKeyRequest) (*emptypb.Empty, error) {
	key, err := distributions.DistributionKeyFromProto(request.GetDistributionKey())
	if err != nil {
		return nil, err
	}
	return nil, l.mgr.AlterDistributedRelationDistributionKey(ctx, request.GetId(), request.GetRelationName(), key)
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

func (l *LocalQrouterServer) ListRouters(ctx context.Context, _ *emptypb.Empty) (*protos.ListRoutersReply, error) {
	return nil, fmt.Errorf("not a coordinator")
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
func (l *LocalQrouterServer) CreateKeyRange(ctx context.Context, request *protos.CreateKeyRangeRequest) (*protos.ModifyReply, error) {
	ds, err := l.mgr.GetDistribution(ctx, request.KeyRangeInfo.DistributionId)
	if err != nil {
		return nil, err
	}
	kRange, err := kr.KeyRangeFromProto(request.KeyRangeInfo, ds.ColTypes)
	if err != nil {
		return nil, err
	}
	err = l.mgr.CreateKeyRange(ctx, kRange)
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

func (l *LocalQrouterServer) ListMoveTasks(ctx context.Context, _ *emptypb.Empty) (*protos.MoveTasksReply, error) {
	taskList, err := l.mgr.ListMoveTasks(ctx)
	if err != nil {
		return nil, err
	}
	tasksProto := make([]*protos.MoveTask, 0, len(taskList))
	for _, taskProto := range taskList {
		tasksProto = append(tasksProto, tasks.MoveTaskToProto(taskProto))
	}
	return &protos.MoveTasksReply{Tasks: tasksProto}, nil
}

func (l *LocalQrouterServer) ListMoveTaskGroups(ctx context.Context, _ *emptypb.Empty) (*protos.ListMoveTaskGroupsReply, error) {
	groups, err := l.mgr.ListMoveTaskGroups(ctx)
	if err != nil {
		return nil, err
	}
	taskGroupsProto := make([]*protos.MoveTaskGroup, 0, len(groups))
	for _, groupProto := range groups {
		taskGroupsProto = append(taskGroupsProto, tasks.TaskGroupToProto(groupProto))
	}
	return &protos.ListMoveTaskGroupsReply{TaskGroups: taskGroupsProto}, nil
}

// TODO: unit tests
func (l *LocalQrouterServer) GetMoveTaskGroup(ctx context.Context, req *protos.MoveTaskGroupSelector) (*protos.GetMoveTaskGroupReply, error) {
	group, err := l.mgr.GetMoveTaskGroup(ctx, req.ID)
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
func (l *LocalQrouterServer) RemoveMoveTaskGroup(ctx context.Context, req *protos.MoveTaskGroupSelector) (*emptypb.Empty, error) {
	return nil, l.mgr.RemoveMoveTaskGroup(ctx, req.ID)
}

// TODO: unit tests
func (l *LocalQrouterServer) RetryMoveTaskGroup(ctx context.Context, req *protos.MoveTaskGroupSelector) (*emptypb.Empty, error) {
	return nil, l.mgr.RetryMoveTaskGroup(ctx, req.ID)
}

// TODO: unit tests
func (l *LocalQrouterServer) StopMoveTaskGroup(ctx context.Context, req *protos.MoveTaskGroupSelector) (*emptypb.Empty, error) {
	return nil, l.mgr.StopMoveTaskGroup(ctx, req.ID)
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

func (l *LocalQrouterServer) ApplyMeta(ctx context.Context, request *protos.MetaTransactionGossipRequest) (*emptypb.Empty, error) {
	toExecuteCmds := make([]qdb.QdbStatement, 0, len(request.Commands))
	for _, gossipCommand := range request.Commands {
		cmdType := mtran.GetGossipRequestType(gossipCommand)
		switch cmdType {
		case mtran.GR_CreateDistributionRequest:
			if cmdList, err := l.createDistributionPrepare(ctx, gossipCommand.CreateDistribution); err != nil {
				return nil, err
			} else {
				if len(cmdList) == 0 {
					return nil, fmt.Errorf("no QDB changes in gossip request:%d", cmdType)
				}
				toExecuteCmds = append(toExecuteCmds, cmdList...)
			}
		// TODO: run handlers converting gossip commands to chunk with qdb commands
		default:
			return nil, fmt.Errorf("invalid meta gossip request:%d", cmdType)
		}
	}
	if chunkCmd, err := mtran.NewMetaTransactionChunk(nil, toExecuteCmds); err != nil {
		return nil, err
	} else {
		return nil, l.mgr.ExecNoTran(ctx, chunkCmd)
	}
}

func (l *LocalQrouterServer) CurrVal(ctx context.Context, req *protos.CurrValRequest) (*protos.CurrValReply, error) {
	val, err := l.mgr.CurrVal(ctx, req.Seq)
	if err != nil {
		return nil, err
	}
	return &protos.CurrValReply{Value: val}, nil
}

func (l *LocalQrouterServer) ListSequences(ctx context.Context, _ *emptypb.Empty) (*protos.ListSequencesReply, error) {
	seqs, err := l.mgr.ListSequences(ctx)
	if err != nil {
		return nil, err
	}
	return &protos.ListSequencesReply{Names: seqs}, nil
}

func (l *LocalQrouterServer) ListRelationSequences(ctx context.Context, req *protos.ListRelationSequencesRequest) (*protos.ListRelationSequencesReply, error) {
	val, err := l.mgr.ListRelationSequences(ctx, rfqn.RelationFQNFromFullName(req.SchemaName, req.Name))
	if err != nil {
		return nil, err
	}
	return &protos.ListRelationSequencesReply{ColumnSequences: val}, nil
}

func (l *LocalQrouterServer) NextRange(ctx context.Context, req *protos.NextRangeRequest) (*protos.NextRangeReply, error) {
	val, err := l.mgr.NextRange(ctx, req.Seq, uint64(req.RangeSize))
	if err != nil {
		return nil, err
	}
	return &protos.NextRangeReply{Left: val.Left, Right: val.Right}, nil
}

func (l *LocalQrouterServer) CreateUniqueIndex(ctx context.Context, req *protos.CreateUniqueIndexRequest) (*emptypb.Empty, error) {
	return nil, l.mgr.CreateUniqueIndex(ctx, req.DistributionId, distributions.UniqueIndexFromProto(req.Idx))
}

func (l *LocalQrouterServer) DropUniqueIndex(ctx context.Context, req *protos.DropUniqueIndexRequest) (*emptypb.Empty, error) {
	return nil, l.mgr.DropUniqueIndex(ctx, req.IdxId)
}

func (l *LocalQrouterServer) ListUniqueIndexes(ctx context.Context, _ *emptypb.Empty) (*protos.ListUniqueIndexesReply, error) {
	idxs, err := l.mgr.ListUniqueIndexes(ctx)
	if err != nil {
		return nil, err
	}
	res := make(map[string]*protos.UniqueIndex)
	for id, idx := range idxs {
		res[id] = distributions.UniqueIndexToProto(idx)
	}
	return &protos.ListUniqueIndexesReply{Indexes: res}, nil
}

func (l *LocalQrouterServer) ListRelationUniqueIndexes(ctx context.Context, req *protos.ListRelationUniqueIndexesRequest) (*protos.ListUniqueIndexesReply, error) {
	idxs, err := l.mgr.ListRelationIndexes(ctx, &rfqn.RelationFQN{
		RelationName: req.RelationName,
	})
	if err != nil {
		return nil, err
	}
	res := make(map[string]*protos.UniqueIndex)
	for id, idx := range idxs {
		res[id] = distributions.UniqueIndexToProto(idx)
	}
	return &protos.ListUniqueIndexesReply{Indexes: res}, nil
}

func (l *LocalQrouterServer) ListDistributionUniqueIndexes(ctx context.Context, req *protos.ListDistributionUniqueIndexesRequest) (*protos.ListUniqueIndexesReply, error) {
	idxs, err := l.mgr.ListDistributionIndexes(ctx, req.DistributionId)
	if err != nil {
		return nil, err
	}
	res := make(map[string]*protos.UniqueIndex)
	for id, idx := range idxs {
		res[id] = distributions.UniqueIndexToProto(idx)
	}
	return &protos.ListUniqueIndexesReply{Indexes: res}, nil
}

func Register(server reflection.GRPCServer, qrouter qrouter.QueryRouter, mgr meta.EntityMgr, rr rulerouter.RuleRouter) {

	lqr := &LocalQrouterServer{
		qr:  qrouter,
		mgr: mgr,
		rr:  rr,
	}

	reflection.Register(server)

	protos.RegisterKeyRangeServiceServer(server, lqr)
	protos.RegisterRouterServiceServer(server, lqr)
	protos.RegisterTopologyServiceServer(server, lqr)
	protos.RegisterClientInfoServiceServer(server, lqr)
	protos.RegisterBackendConnectionsServiceServer(server, lqr)
	protos.RegisterPoolServiceServer(server, lqr)
	protos.RegisterDistributionServiceServer(server, lqr)
	protos.RegisterMoveTasksServiceServer(server, lqr)
	protos.RegisterBalancerTaskServiceServer(server, lqr)
	protos.RegisterReferenceRelationsServiceServer(server, lqr)
	protos.RegisterMetaTransactionGossipServiceServer(server, lqr)
}

var _ protos.KeyRangeServiceServer = &LocalQrouterServer{}
var _ protos.RouterServiceServer = &LocalQrouterServer{}
var _ protos.ClientInfoServiceServer = &LocalQrouterServer{}
var _ protos.BackendConnectionsServiceServer = &LocalQrouterServer{}
var _ protos.PoolServiceServer = &LocalQrouterServer{}
var _ protos.DistributionServiceServer = &LocalQrouterServer{}
var _ protos.MoveTasksServiceServer = &LocalQrouterServer{}
var _ protos.BalancerTaskServiceServer = &LocalQrouterServer{}
var _ protos.ShardServiceServer = &LocalQrouterServer{}
var _ protos.ReferenceRelationsServiceServer = &LocalQrouterServer{}
var _ protos.MetaTransactionGossipServiceServer = &LocalQrouterServer{}
