package shrule

import "context"

type ShardingRulesMgr interface {
	AddShardingRule(ctx context.Context, rule *ShardingRule) error
	ListShardingRules(ctx context.Context) ([]*ShardingRule, error)
}
