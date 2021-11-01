package provider

import (
	"context"

	"github.com/pg-sharding/spqr/coordinator"
	"github.com/pg-sharding/spqr/pkg/models/shrule"
	protos "github.com/pg-sharding/spqr/router/protos"
)

type CoordinatorService struct {
	protos.UnimplementedKeyRangeServiceServer
	protos.UnimplementedShardingRulesServiceServer

	impl coordinator.Coordinator
}

func (c CoordinatorService) AddShardingRules(ctx context.Context, request *protos.AddShardingRuleRequest) (*protos.AddShardingRuleReply, error) {

	for _, rule := range request.Rules {
		err := c.impl.AddShardingRule(shrule.NewShardingRule(rule.Columns))

		if err != nil {
			return nil, err
		}
	}

	return &protos.AddShardingRuleReply{}, nil
}

func (c CoordinatorService) ListShardingRules(ctx context.Context, request *protos.AddShardingRuleRequest) (*protos.AddShardingRuleReply, error) {
	panic("implement me")
}

func (c CoordinatorService) AddKeyRange(ctx context.Context, request *protos.AddKeyRangeRequest) (*protos.AddKeyRangeReply, error) {
	panic("implement me")
}

func (c CoordinatorService) LockKeyRange(ctx context.Context, request *protos.LockKeyRangeRequest) (*protos.LockKeyRangeReply, error) {
	_, err := c.impl.Lock(request.Krid)
	return nil, err
}

func (c CoordinatorService) UnlockKeyRange(ctx context.Context, request *protos.UnlockKeyRangeRequest) (*protos.UnlockKeyRangeReply, error) {
	err := c.impl.UnLock(request.Krid)
	return nil, err
}
func (c CoordinatorService) SplitKeyRange(ctx context.Context, request *protos.SplitKeyRangeRequest) (*protos.SplitKeyRangeReply, error) {
	panic("implement me")
}

func (c CoordinatorService) ListKeyRange(ctx context.Context, request *protos.ListKeyRangeRequest) (*protos.KeyRangeReply, error) {

	krs := []*protos.KeyRange{
		{
			Krid:    "1",
			ShardId: "2",
		},
	}

	return &protos.KeyRangeReply{
		KeyRanges: krs,
	}, nil
}

var _ protos.KeyRangeServiceServer = CoordinatorService{}
var _ protos.ShardingRulesServiceServer = CoordinatorService{}

func NewKeyRangeService(impl coordinator.Coordinator) protos.KeyRangeServiceServer {
	return &CoordinatorService{
		impl: impl,
	}
}
