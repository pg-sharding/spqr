package shrule

import "context"

type ShardingRulesMgr interface {
	AddShardingRule(ctx context.Context, rule *ShardingRule) error
	DropShardingRule(ctx context.Context, id string) error
	DropShardingRuleAll(ctx context.Context) ([]*ShardingRule, error)
	ListShardingRules(ctx context.Context, dataspace string) ([]*ShardingRule, error)
}
