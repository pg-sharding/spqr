package grpcqrouter

import (
	"context"

	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/pkg/models/shrule"
	"github.com/pg-sharding/spqr/router/pkg/qrouter"
	protos "github.com/pg-sharding/spqr/router/protos"
	"github.com/wal-g/tracelog"
	"google.golang.org/grpc/reflection"
)

type LocalQrouterServer struct {
	protos.UnimplementedKeyRangeServiceServer
	protos.UnimplementedShardingRulesServiceServer
	qr qrouter.Qrouter
}

func (l *LocalQrouterServer) AddShardingRules(ctx context.Context, request *protos.AddShardingRuleRequest) (*protos.AddShardingRuleReply, error) {

	for _, rule := range request.Rules {
		err := l.qr.AddShardingRule(shrule.NewShardingRule(rule.Columns))

		if err != nil {
			return nil, err
		}
	}

	return &protos.AddShardingRuleReply{}, nil
}

func (l *LocalQrouterServer) ListShardingRules(ctx context.Context, request *protos.AddShardingRuleRequest) (*protos.AddShardingRuleReply, error) {
	panic("implement me")
}

func (l *LocalQrouterServer) AddKeyRange(ctx context.Context, request *protos.AddKeyRangeRequest) (*protos.AddKeyRangeReply, error) {

	err := l.qr.AddKeyRange(ctx, kr.KeyRangeFromProto(request.KeyRange))
	if err != nil {
		return nil, err
	}

	return &protos.AddKeyRangeReply{}, nil
}

func (l *LocalQrouterServer) ListKeyRange(ctx context.Context, request *protos.ListKeyRangeRequest) (*protos.KeyRangeReply, error) {
	var krs []*protos.KeyRange

	tracelog.InfoLogger.Printf("listing key ranges")

	for _, keyRange := range l.qr.KeyRanges(ctx) {
		krs = append(krs, keyRange.ToProto())
	}

	resp := &protos.KeyRangeReply{
		KeyRanges: krs,
	}

	return resp, nil
}

func (l *LocalQrouterServer) LockKeyRange(ctx context.Context, request *protos.LockKeyRangeRequest) (*protos.LockKeyRangeReply, error) {
	if _, err := l.qr.Lock(ctx, request.Krid); err != nil {
		return nil, err
	}
	return &protos.LockKeyRangeReply{}, nil
}

func (l *LocalQrouterServer) UnlockKeyRange(ctx context.Context, request *protos.UnlockKeyRangeRequest) (*protos.UnlockKeyRangeReply, error) {
	panic("implement me")
}

func (l *LocalQrouterServer) SplitKeyRange(ctx context.Context, request *protos.SplitKeyRangeRequest) (*protos.SplitKeyRangeReply, error) {
	panic("implement me")
}

func Register(server reflection.GRPCServer, qrouter qrouter.Qrouter) {

	reflection.Register(server)

	lqr := &LocalQrouterServer{
		qr: qrouter,
	}

	protos.RegisterKeyRangeServiceServer(server, lqr)
}

var _ protos.KeyRangeServiceServer = &LocalQrouterServer{}
var _ protos.ShardingRulesServiceServer = &LocalQrouterServer{}
