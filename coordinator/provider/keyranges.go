package provider

import (
	"context"

	"github.com/pg-sharding/spqr/coordinator"
	"github.com/pg-sharding/spqr/pkg/models/kr"
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
		err := c.impl.AddShardingRule(ctx, shrule.NewShardingRule(rule.Columns))

		if err != nil {
			return nil, err
		}
	}

	return &protos.AddShardingRuleReply{}, nil
}

func (c CoordinatorService) ListShardingRules(ctx context.Context, request *protos.AddShardingRuleRequest) (*protos.ListShardingRuleReply, error) {
	rules, err := c.impl.ListShardingRules(ctx)
	if err != nil {
		return nil, err
	}

	shardingRules := []*protos.ShardingRule{}

	for _, rule := range rules {
		shardingRules = append(shardingRules, &protos.ShardingRule{
			Columns: rule.Columns(),
		})
	}

	return &protos.ListShardingRuleReply{
		Rules: shardingRules,
	}, nil
}

func (c CoordinatorService) AddKeyRange(ctx context.Context, request *protos.AddKeyRangeRequest) (*protos.AddKeyRangeReply, error) {
	err := c.impl.AddKeyRange(ctx, &kr.KeyRange{
		LowerBound: []byte(request.KeyRange.LowerBound),
		UpperBound: []byte(request.KeyRange.UpperBound),
		ID:         request.KeyRange.Krid,
		ShardID:    request.KeyRange.ShardId,
	})
	if err != nil {
		return nil, err
	}

	return &protos.AddKeyRangeReply{}, nil
}

func (c CoordinatorService) LockKeyRange(ctx context.Context, request *protos.LockKeyRangeRequest) (*protos.LockKeyRangeReply, error) {
	_, err := c.impl.Lock(ctx, request.Krid)
	return nil, err
}

func (c CoordinatorService) UnlockKeyRange(ctx context.Context, request *protos.UnlockKeyRangeRequest) (*protos.UnlockKeyRangeReply, error) {
	err := c.impl.UnLock(ctx, request.Krid)
	return nil, err
}
func (c CoordinatorService) SplitKeyRange(ctx context.Context, request *protos.SplitKeyRangeRequest) (*protos.SplitKeyRangeReply, error) {
	panic("implement me")
}

func (c CoordinatorService) ListKeyRange(ctx context.Context, request *protos.ListKeyRangeRequest) (*protos.KeyRangeReply, error) {
	krsqb, err := c.impl.ListKeyRanges(ctx)
	if err != nil {
		return nil, err
	}

	var krs []*protos.KeyRange

	for _, kr := range krsqb {
		krs = append(krs, kr.ToProto())
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
