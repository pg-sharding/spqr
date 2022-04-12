package provider

import (
	"context"

	"github.com/pg-sharding/spqr/coordinator"
	"github.com/pg-sharding/spqr/pkg/models/shrule"
	shards "github.com/pg-sharding/spqr/router/protos"
)

type ShardingRulesService struct {
	shards.UnimplementedShardingRulesServiceServer

	impl coordinator.Coordinator
}

func (s ShardingRulesService) AddShardingRules(ctx context.Context, request *shards.AddShardingRuleRequest) (*shards.AddShardingRuleReply, error) {
	for _, rule := range request.Rules {
		err := s.impl.AddShardingRule(ctx, shrule.NewShardingRule(rule.Columns))

		if err != nil {
			return nil, err
		}
	}

	return &shards.AddShardingRuleReply{}, nil
}

func (s ShardingRulesService) ListShardingRules(ctx context.Context, request *shards.ListShardingRuleRequest) (*shards.ListShardingRuleReply, error) {
	rules, err := s.impl.ListShardingRules(ctx)
	if err != nil {
		return nil, err
	}

	var shardingRules []*shards.ShardingRule

	for _, rule := range rules {
		shardingRules = append(shardingRules, &shards.ShardingRule{
			Columns: rule.Columns(),
		})
	}

	return &shards.ListShardingRuleReply{
		Rules: shardingRules,
	}, nil
}

func NewShardingRules(impl coordinator.Coordinator) *ShardingRulesService {
	return &ShardingRulesService{
		impl: impl,
	}
}
