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

// TODO : unit tests
func (s *ShardingRulesService) AddShardingRules(ctx context.Context, request *protos.AddShardingRuleRequest) (*protos.AddShardingRuleReply, error) {
	return nil, spqrerror.New(spqrerror.SPQR_INVALID_REQUEST, "sharding rules are removed from SPQR")
}

// TODO : unit tests
func (s *ShardingRulesService) ListShardingRules(ctx context.Context, request *protos.ListShardingRuleRequest) (*protos.ListShardingRuleReply, error) {
	return nil, spqrerror.New(spqrerror.SPQR_INVALID_REQUEST, "sharding rules are removed from SPQR")
}

func NewShardingRulesServer(impl coordinator.Coordinator) *ShardingRulesService {
	return &ShardingRulesService{
		impl: impl,
	}
}
