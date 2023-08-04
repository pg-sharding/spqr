package grpc

import (
	"context"
	"fmt"

	"github.com/pg-sharding/spqr/pkg/client"
	"github.com/pg-sharding/spqr/pkg/meta"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/pkg/models/shrule"
	protos "github.com/pg-sharding/spqr/pkg/protos"
	"github.com/pg-sharding/spqr/pkg/shard"
	"github.com/pg-sharding/spqr/pkg/spqrlog"
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
	qr  qrouter.QueryRouter
	mgr meta.EntityMgr
	rr  rulerouter.RuleRouter
}

func (l *LocalQrouterServer) OpenRouter(ctx context.Context, request *protos.OpenRouterRequest) (*protos.OpenRouterReply, error) {
	l.qr.Initialize()
	return &protos.OpenRouterReply{}, nil
}

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

func (l *LocalQrouterServer) CloseRouter(ctx context.Context, request *protos.CloseRouterRequest) (*protos.CloseRouterReply, error) {
	//TODO implement me
	panic("implement me")
}

func (l *LocalQrouterServer) DropKeyRange(ctx context.Context, request *protos.DropKeyRangeRequest) (*protos.ModifyReply, error) {
	for _, id := range request.Id {
		err := l.mgr.DropKeyRange(ctx, id)
		if err != nil {
			return nil, err
		}
	}
	return &protos.ModifyReply{}, nil
}

func (l *LocalQrouterServer) DropAllKeyRanges(ctx context.Context, _ *protos.DropAllKeyRangesRequest) (*protos.DropAllKeyRangesResponse, error) {
	if err := l.mgr.DropKeyRangeAll(ctx); err != nil {
		return nil, err
	}
	return &protos.DropAllKeyRangesResponse{}, nil
}

func (l *LocalQrouterServer) MoveKeyRange(ctx context.Context, request *protos.MoveKeyRangeRequest) (*protos.ModifyReply, error) {
	err := l.mgr.Move(ctx, &kr.MoveKeyRange{Krid: request.KeyRange.Krid, ShardId: request.ToShardId})
	if err != nil {
		return nil, err
	}

	return &protos.ModifyReply{}, nil
}

func (l *LocalQrouterServer) AddShardingRules(ctx context.Context, request *protos.AddShardingRuleRequest) (*protos.AddShardingRuleReply, error) {
	for _, rule := range request.Rules {
		err := l.mgr.AddShardingRule(ctx, shrule.ShardingRuleFromProto(rule))

		if err != nil {
			return nil, err
		}
	}

	return &protos.AddShardingRuleReply{}, nil
}

func (l *LocalQrouterServer) ListShardingRules(ctx context.Context, request *protos.ListShardingRuleRequest) (*protos.ListShardingRuleReply, error) {
	rules, err := l.mgr.ListShardingRules(ctx)
	if err != nil {
		return nil, err
	}

	var shardingRules []*protos.ShardingRule

	for _, rule := range rules {
		shardingRules = append(shardingRules, shrule.ShardingRuleToProto(rule))
	}

	return &protos.ListShardingRuleReply{
		Rules: shardingRules,
	}, nil
}

func (l *LocalQrouterServer) DropShardingRules(ctx context.Context, request *protos.DropShardingRuleRequest) (*protos.DropShardingRuleReply, error) {
	spqrlog.Zero.Debug().
		Strs("rules", request.Id).
		Msg("dropping sharding rules")
	for _, id := range request.Id {
		if err := l.mgr.DropShardingRule(ctx, id); err != nil {
			return nil, err
		}
	}

	return &protos.DropShardingRuleReply{}, nil
}

func (l *LocalQrouterServer) AddKeyRange(ctx context.Context, request *protos.AddKeyRangeRequest) (*protos.ModifyReply, error) {
	err := l.mgr.AddKeyRange(ctx, kr.KeyRangeFromProto(request.KeyRangeInfo))
	if err != nil {
		return nil, err
	}

	return &protos.ModifyReply{}, nil
}

func (l *LocalQrouterServer) ListKeyRange(ctx context.Context, _ *protos.ListKeyRangeRequest) (*protos.KeyRangeReply, error) {
	var krs []*protos.KeyRangeInfo

	spqrlog.Zero.Debug().Msg("listing key ranges")

	krsqdb, err := l.mgr.ListKeyRanges(ctx)
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

func (l *LocalQrouterServer) LockKeyRange(ctx context.Context, request *protos.LockKeyRangeRequest) (*protos.ModifyReply, error) {
	for _, id := range request.Id {
		if _, err := l.mgr.LockKeyRange(ctx, id); err != nil {
			return nil, err
		}
	}
	return &protos.ModifyReply{}, nil
}

func (l *LocalQrouterServer) UnlockKeyRange(ctx context.Context, request *protos.UnlockKeyRangeRequest) (*protos.ModifyReply, error) {
	for _, id := range request.Id {
		if err := l.mgr.Unlock(ctx, id); err != nil {
			return nil, err
		}
	}
	return &protos.ModifyReply{}, nil
}

func (l *LocalQrouterServer) SplitKeyRange(ctx context.Context, request *protos.SplitKeyRangeRequest) (*protos.ModifyReply, error) {
	if err := l.mgr.Split(ctx, &kr.SplitKeyRange{
		Krid:     request.KeyRangeInfo.Krid,
		SourceID: request.SourceId,
		Bound:    request.Bound,
	}); err != nil {
		return nil, err
	}

	return &protos.ModifyReply{}, nil
}

func (l *LocalQrouterServer) MergeKeyRange(ctx context.Context, request *protos.MergeKeyRangeRequest) (*protos.ModifyReply, error) {
	krs, err := l.mgr.ListKeyRanges(ctx)
	if err != nil {
		return nil, err
	}

	var krright *kr.KeyRange
	var krleft *kr.KeyRange

	for _, keyrange := range krs {
		if kr.CmpRangesEqual(keyrange.LowerBound, request.Bound) {
			krright = keyrange
			if krleft != nil {
				break
			}
			continue
		}

		if kr.CmpRangesEqual(keyrange.UpperBound, request.Bound) {
			krleft = keyrange
			if krright != nil {
				break
			}
			continue
		}
	}

	if krright == nil || krleft == nil {
		return nil, fmt.Errorf("key range on the left or on the right was not found")
	}

	spqrlog.Zero.Debug().
		Str("left krid", krleft.ID).
		Str("right krid", krright.ID).
		Msg("listing key ranges")

	if err := l.mgr.Unite(ctx, &kr.UniteKeyRange{
		KeyRangeIDLeft:  krleft.ID,
		KeyRangeIDRight: krright.ID,
	}); err != nil {
		return nil, err
	}

	return &protos.ModifyReply{}, nil
}

func ClientToProto(cl client.ClientInfo) *protos.ClientInfo {
	clientInfo := &protos.ClientInfo{
		ClientId: cl.ID(),
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

func ShardToProto(sh shard.Shardinfo) *protos.BackendConnectionsInfo {
	shardInfo := &protos.BackendConnectionsInfo{
		BackendConnectionId: sh.ID(),
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

func (l *LocalQrouterServer) ListClients(context.Context, *protos.ListClientsRequest) (*protos.ListClientsReply, error) {
	reply := &protos.ListClientsReply{}

	err := l.rr.ClientPoolForeach(func(client client.ClientInfo) error {
		reply.Clients = append(reply.Clients, ClientToProto(client))
		return nil
	})
	return reply, err
}

func (l *LocalQrouterServer) ListBackendConnections(context.Context, *protos.ListBackendConnectionsRequest) (*protos.ListBackendConntionsReply, error) {
	reply := &protos.ListBackendConntionsReply{}

	err := l.rr.ForEach(func(sh shard.Shardinfo) error {
		reply.Conns = append(reply.Conns, ShardToProto(sh))
		return nil
	})
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
}

var _ protos.KeyRangeServiceServer = &LocalQrouterServer{}
var _ protos.ShardingRulesServiceServer = &LocalQrouterServer{}
var _ protos.RouterServiceServer = &LocalQrouterServer{}
var _ protos.ClientInfoServiceServer = &LocalQrouterServer{}
var _ protos.BackendConnectionsServiceServer = &LocalQrouterServer{}
