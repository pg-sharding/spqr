package grpcqrouter

import (
	"context"

	"github.com/pg-sharding/spqr/pkg/spqrlog"
	"google.golang.org/grpc/reflection"

	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/pkg/models/shrule"
	"github.com/pg-sharding/spqr/router/pkg/qrouter"
	protos "github.com/pg-sharding/spqr/router/protos"
)

type LocalQrouterServer struct {
	protos.UnimplementedKeyRangeServiceServer
	protos.UnimplementedShardingRulesServiceServer
	qr qrouter.QueryRouter
}

func (l *LocalQrouterServer) DropKeyRange(ctx context.Context, request *protos.DropKeyRangeRequest) (*protos.ModifyReply, error) {
	err := l.qr.DropKeyRange(ctx, request.KeyRange.Krid)
	if err != nil {
		return nil, err
	}

	return &protos.ModifyReply{}, nil
}

func (l *LocalQrouterServer) DropAllKeyRanges(ctx context.Context, _ *protos.DropAllKeyRangesRequest) (*protos.DropAllKeyRangesResponse, error) {
	err := l.qr.DropAll(ctx)
	if err != nil {
		return nil, err
	}
	return &protos.DropAllKeyRangesResponse{}, nil
}

func (l *LocalQrouterServer) MoveKeyRange(ctx context.Context, request *protos.MoveKeyRangeRequest) (*protos.ModifyReply, error) {
	err := l.qr.Move(ctx, &kr.MoveKeyRange{Krid: request.KeyRange.Krid, ShardId: request.ToShardId})
	if err != nil {
		return nil, err
	}

	return &protos.ModifyReply{}, nil
}

func (l *LocalQrouterServer) AddShardingRules(ctx context.Context, request *protos.AddShardingRuleRequest) (*protos.AddShardingRuleReply, error) {
	for _, rule := range request.Rules {
		err := l.qr.AddShardingRule(ctx, shrule.NewShardingRule(rule.Id, rule.Columns))

		if err != nil {
			return nil, err
		}
	}

	return &protos.AddShardingRuleReply{}, nil
}

func (l *LocalQrouterServer) ListShardingRules(ctx context.Context, request *protos.ListShardingRuleRequest) (*protos.ListShardingRuleReply, error) {
	rules, err := l.qr.ListShardingRules(ctx)
	if err != nil {
		return nil, err
	}

	var shardingRules []*protos.ShardingRule

	for _, rule := range rules {
		shardingRules = append(shardingRules, &protos.ShardingRule{
			Columns: rule.Columns(),
		})
	}

	return &protos.ListShardingRuleReply{
		Rules: shardingRules,
	}, nil
}

func (l *LocalQrouterServer) AddKeyRange(ctx context.Context, request *protos.AddKeyRangeRequest) (*protos.ModifyReply, error) {
	err := l.qr.AddKeyRange(ctx, kr.KeyRangeFromProto(request.KeyRangeInfo))
	if err != nil {
		return nil, err
	}

	return &protos.ModifyReply{}, nil
}

func (l *LocalQrouterServer) ListKeyRange(ctx context.Context, _ *protos.ListKeyRangeRequest) (*protos.KeyRangeReply, error) {
	var krs []*protos.KeyRangeInfo

	spqrlog.Logger.Printf(spqrlog.DEBUG3, "listing key ranges")

	krsqdb, err := l.qr.ListKeyRange(ctx)
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
	if _, err := l.qr.Lock(ctx, request.KeyRange.Krid); err != nil {
		return nil, err
	}
	return &protos.ModifyReply{}, nil
}

func (l *LocalQrouterServer) UnlockKeyRange(ctx context.Context, request *protos.UnlockKeyRangeRequest) (*protos.ModifyReply, error) {
	if err := l.qr.Unlock(ctx, request.KeyRange.Krid); err != nil {
		return nil, err
	}
	return &protos.ModifyReply{}, nil
}

func (l *LocalQrouterServer) SplitKeyRange(ctx context.Context, request *protos.SplitKeyRangeRequest) (*protos.ModifyReply, error) {
	if err := l.qr.Split(ctx, &kr.SplitKeyRange{
		Krid:     request.KeyRangeInfo.Krid,
		SourceID: request.KeyRangeInfo.ShardId,
		Bound:    request.Bound,
	}); err != nil {
		return nil, err
	}

	return &protos.ModifyReply{}, nil
}

func Register(server reflection.GRPCServer, qrouter qrouter.QueryRouter) {

	reflection.Register(server)

	lqr := &LocalQrouterServer{
		qr: qrouter,
	}

	protos.RegisterKeyRangeServiceServer(server, lqr)
	protos.RegisterShardingRulesServiceServer(server, lqr)
}

var _ protos.KeyRangeServiceServer = &LocalQrouterServer{}
var _ protos.ShardingRulesServiceServer = &LocalQrouterServer{}
