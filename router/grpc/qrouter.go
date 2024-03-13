package grpc

import (
	"context"
	"fmt"
	"github.com/pg-sharding/spqr/pkg/models/distributions"
	"github.com/pg-sharding/spqr/pkg/models/spqrerror"

	"github.com/pg-sharding/spqr/pkg/client"
	"github.com/pg-sharding/spqr/pkg/meta"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/pkg/pool"
	protos "github.com/pg-sharding/spqr/pkg/protos"
	"github.com/pg-sharding/spqr/pkg/shard"
	"github.com/pg-sharding/spqr/router/qrouter"
	"github.com/pg-sharding/spqr/router/rulerouter"
	"google.golang.org/grpc/reflection"
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
	qr  qrouter.QueryRouter
	mgr meta.EntityMgr
	rr  rulerouter.RuleRouter
}

// CreateDistribution creates distribution in QDB
// TODO: unit tests
func (l *LocalQrouterServer) CreateDistribution(ctx context.Context, request *protos.CreateDistributionRequest) (*protos.CreateDistributionReply, error) {
	for _, ds := range request.GetDistributions() {
		if err := l.mgr.CreateDistribution(ctx, distributions.DistributionFromProto(ds)); err != nil {
			return nil, err
		}
	}
	return &protos.CreateDistributionReply{}, nil
}

// DropDistribution deletes distribution from QDB
// TODO: unit tests
func (l *LocalQrouterServer) DropDistribution(ctx context.Context, request *protos.DropDistributionRequest) (*protos.DropDistributionReply, error) {
	for _, id := range request.GetIds() {
		if err := l.mgr.DropDistribution(ctx, id); err != nil {
			return nil, err
		}
	}
	return &protos.DropDistributionReply{}, nil
}

// ListDistributions returns all distributions from QDB
// TODO: unit tests
func (l *LocalQrouterServer) ListDistributions(ctx context.Context, _ *protos.ListDistributionsRequest) (*protos.ListDistributionsReply, error) {
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
func (l *LocalQrouterServer) AlterDistributionAttach(ctx context.Context, request *protos.AlterDistributionAttachRequest) (*protos.AlterDistributionAttachReply, error) {
	return &protos.AlterDistributionAttachReply{}, l.mgr.AlterDistributionAttach(
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
func (l *LocalQrouterServer) AlterDistributionDetach(ctx context.Context, request *protos.AlterDistributionDetachRequest) (*protos.AlterDistributionDetachReply, error) {
	for _, relName := range request.GetRelNames() {
		if err := l.mgr.AlterDistributionDetach(ctx, request.GetId(), relName); err != nil {
			return nil, err
		}
	}
	return &protos.AlterDistributionDetachReply{}, nil
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
	ds, err := l.mgr.GetRelationDistribution(ctx, request.GetId())
	if err != nil {
		return nil, err
	}
	return &protos.GetRelationDistributionReply{Distribution: distributions.DistributionToProto(ds)}, err
}

// TODO : unit tests
func (l *LocalQrouterServer) OpenRouter(ctx context.Context, request *protos.OpenRouterRequest) (*protos.OpenRouterReply, error) {
	l.qr.Initialize()
	return &protos.OpenRouterReply{}, nil
}

// TODO : unit tests
func (l *LocalQrouterServer) GetRouterStatus(ctx context.Context, request *protos.GetRouterStatusRequest) (*protos.GetRouterStatusReply, error) {
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
func (l *LocalQrouterServer) CloseRouter(ctx context.Context, request *protos.CloseRouterRequest) (*protos.CloseRouterReply, error) {
	//TODO implement me
	panic("implement me")
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
func (l *LocalQrouterServer) DropAllKeyRanges(ctx context.Context, _ *protos.DropAllKeyRangesRequest) (*protos.DropAllKeyRangesResponse, error) {
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
func (l *LocalQrouterServer) AddShardingRules(ctx context.Context, request *protos.AddShardingRuleRequest) (*protos.AddShardingRuleReply, error) {
	return nil, spqrerror.ShardingKeysRemoved
}

// TODO : unit tests
func (l *LocalQrouterServer) ListShardingRules(ctx context.Context, request *protos.ListShardingRuleRequest) (*protos.ListShardingRuleReply, error) {
	return nil, spqrerror.ShardingKeysRemoved
}

// TODO : unit tests
func (l *LocalQrouterServer) DropShardingRules(ctx context.Context, request *protos.DropShardingRuleRequest) (*protos.DropShardingRuleReply, error) {
	return nil, spqrerror.ShardingKeysRemoved
}

// TODO : unit tests
func (l *LocalQrouterServer) AddKeyRange(ctx context.Context, request *protos.AddKeyRangeRequest) (*protos.ModifyReply, error) {
	err := l.mgr.AddKeyRange(ctx, kr.KeyRangeFromProto(request.KeyRangeInfo))
	if err != nil {
		return nil, err
	}

	return &protos.ModifyReply{}, nil
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
func (l *LocalQrouterServer) ListAllKeyRanges(ctx context.Context, request *protos.ListAllKeyRangesRequest) (*protos.KeyRangeReply, error) {
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
		Bound:     request.Bound,
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
			Instance: &protos.DBInstaceInfo{
				Hostname: shard.Instance().Hostname(),
			},
		})
	}
	return clientInfo
}

// TODO : unit tests
func ShardToProto(sh shard.Shardinfo) *protos.BackendConnectionsInfo {
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
	poolInfo := &protos.PoolInfo{
		Id:            fmt.Sprintf("%p", p),
		DB:            p.Rule().DB,
		Usr:           p.Rule().Usr,
		Host:          p.Hostname(),
		RouterName:    router,
		ConnCount:     int64(p.UsedConnectionCount()),
		IdleConnCount: int64(p.IdleConnectionCount()),
		QueueSize:     int64(p.QueueResidualSize()),
	}
	return poolInfo
}

// TODO : unit tests
func (l *LocalQrouterServer) ListClients(context.Context, *protos.ListClientsRequest) (*protos.ListClientsReply, error) {
	reply := &protos.ListClientsReply{}

	err := l.rr.ClientPoolForeach(func(client client.ClientInfo) error {
		reply.Clients = append(reply.Clients, ClientToProto(client))
		return nil
	})
	return reply, err
}

// TODO : unit tests
func (l *LocalQrouterServer) ListBackendConnections(context.Context, *protos.ListBackendConnectionsRequest) (*protos.ListBackendConntionsReply, error) {
	reply := &protos.ListBackendConntionsReply{}

	err := l.rr.ForEach(func(sh shard.Shardinfo) error {
		reply.Conns = append(reply.Conns, ShardToProto(sh))
		return nil
	})
	return reply, err
}

// TODO : unit tests
func (l *LocalQrouterServer) ListPools(context.Context, *protos.ListPoolsRequest) (*protos.ListPoolsResponse, error) {
	reply := &protos.ListPoolsResponse{}

	err := l.rr.ForEachPool(func(p pool.Pool) error {
		reply.Pools = append(reply.Pools, PoolToProto(p, l.rr.Config().Host))
		return nil
	})
	return reply, err
}

// TODO : unit tests
func (l *LocalQrouterServer) UpdateCoordinator(ctx context.Context, req *protos.UpdateCoordinatorRequest) (*protos.UpdateCoordinatorResponse, error) {
	reply := &protos.UpdateCoordinatorResponse{}
	err := l.mgr.UpdateCoordinator(ctx, req.Address)
	return reply, err
}

func (l *LocalQrouterServer) GetCoordinator(ctx context.Context, req *protos.GetCoordinatorRequest) (*protos.GetCoordinatorResponse, error) {
	reply := &protos.GetCoordinatorResponse{}
	re, err := l.mgr.GetCoordinator(ctx)
	reply.Address = re
	return reply, err
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
}

var _ protos.KeyRangeServiceServer = &LocalQrouterServer{}
var _ protos.ShardingRulesServiceServer = &LocalQrouterServer{}
var _ protos.RouterServiceServer = &LocalQrouterServer{}
var _ protos.ClientInfoServiceServer = &LocalQrouterServer{}
var _ protos.BackendConnectionsServiceServer = &LocalQrouterServer{}
var _ protos.PoolServiceServer = &LocalQrouterServer{}
var _ protos.DistributionServiceServer = &LocalQrouterServer{}
