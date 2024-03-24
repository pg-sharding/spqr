package provider

import (
	"context"
	"github.com/pg-sharding/spqr/pkg/models/spqrerror"

	"github.com/pg-sharding/spqr/coordinator"
	protos "github.com/pg-sharding/spqr/pkg/protos"
)

type ShardingRulesService struct {
	protos.UnimplementedShardingRulesServiceServer
	impl coordinator.Coordinator
}

func (s *ShardingRulesService) AddShardingRules(ctx context.Context, request *protos.AddShardingRuleRequest) (*protos.AddShardingRuleReply, error) {
	return nil, spqrerror.ShardingKeysRemoved
}

func (s *ShardingRulesService) ListShardingRules(ctx context.Context, request *protos.ListShardingRuleRequest) (*protos.ListShardingRuleReply, error) {
	return nil, spqrerror.ShardingKeysRemoved
}

func NewShardingRulesServer(impl coordinator.Coordinator) *ShardingRulesService {
	return &ShardingRulesService{
		impl: impl,
	}
}
