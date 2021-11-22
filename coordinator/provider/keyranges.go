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

func (c CoordinatorService) AddKeyRange(ctx context.Context, request *protos.AddKeyRangeRequest) (*protos.ModifyReply, error) {
	err := c.impl.AddKeyRange(ctx, &kr.KeyRange{
		LowerBound: []byte(request.KeyRangeInfo.KeyRange.LowerBound),
		UpperBound: []byte(request.KeyRangeInfo.KeyRange.UpperBound),
		ID:         request.KeyRangeInfo.Krid,
		ShardID:    request.KeyRangeInfo.ShardId,
	})
	if err != nil {
		return nil, err
	}

	return &protos.ModifyReply{}, nil
}

func (c CoordinatorService) LockKeyRange(ctx context.Context, request *protos.LockKeyRangeRequest) (*protos.ModifyReply, error) {
	_, err := c.impl.Lock(ctx, "xx")
	return nil, err
}

func (c CoordinatorService) UnlockKeyRange(ctx context.Context, request *protos.UnlockKeyRangeRequest) (*protos.ModifyReply, error) {
	err := c.impl.Unlock(ctx, "xx")
	return nil, err
}
func (c CoordinatorService) SplitKeyRange(ctx context.Context, request *protos.SplitKeyRangeRequest) (*protos.ModifyReply, error) {
	err := c.impl.Split(ctx, &kr.SplitKeyRange{
		Bound:    request.Bound,
	})
	if err != nil {
		return nil, err
	}

	return &protos.ModifyReply{}, nil
}

func (c CoordinatorService) ListKeyRange(ctx context.Context, request *protos.ListKeyRangeRequest) (*protos.KeyRangeReply, error) {
	krsqb, err := c.impl.ListKeyRanges(ctx)
	if err != nil {
		return nil, err
	}

	var krs []*protos.KeyRangeInfo

	for _, keyRange := range krsqb {
		krs = append(krs, keyRange.ToProto())
	}

	return &protos.KeyRangeReply{
		KeyRangesInfo: krs,
	}, nil
}

var _ protos.KeyRangeServiceServer = CoordinatorService{}
var _ protos.ShardingRulesServiceServer = CoordinatorService{}

func NewKeyRangeService(impl coordinator.Coordinator) protos.KeyRangeServiceServer {
	return &CoordinatorService{
		impl: impl,
	}
}
