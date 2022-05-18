package shrule

import "context"

type ShardingRulesMgr interface {
	AddShardingRule(ctx context.Context, rule *ShardingRule) error
	CheckShardingRule(ctx context.Context, colnames []string) bool
	ListShardingRules(ctx context.Context) ([]*ShardingRule, error)
}
