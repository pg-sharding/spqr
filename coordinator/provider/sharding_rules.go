package provider

import (
	"context"

	"github.com/pg-sharding/spqr/coordinator"
	"github.com/pg-sharding/spqr/pkg/models/shrule"
	protos "github.com/pg-sharding/spqr/router/protos"
)

type ShardingRulesService struct {
	protos.UnimplementedShardingRulesServiceServer
	impl coordinator.Coordinator
}

func (s ShardingRulesService) AddShardingRules(ctx context.Context, request *protos.AddShardingRuleRequest) (*protos.AddShardingRuleReply, error) {
	for _, rule := range request.Rules {
		err := s.impl.AddShardingRule(ctx, shrule.NewShardingRule(rule.Columns))
		if err != nil {
			return nil, err
		}
	}

	return &protos.AddShardingRuleReply{}, nil
}

func (s ShardingRulesService) ListShardingRules(ctx context.Context, request *protos.ListShardingRuleRequest) (*protos.ListShardingRuleReply, error) {
	rules, err := s.impl.ListShardingRules(ctx)
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

func NewShardingRules(impl coordinator.Coordinator) *ShardingRulesService {
	return &ShardingRulesService{
		impl: impl,
	}
}
