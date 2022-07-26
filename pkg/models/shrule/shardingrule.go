package shrule

import "github.com/pg-sharding/spqr/qdb"

type ShardingRule struct {
	id      string
	columns []string
}

// local table sharding rule -> route to world

func NewShardingRule(id string, cols []string) *ShardingRule {
	return &ShardingRule{
		id:      id,
		columns: cols,
	}
}

func (s *ShardingRule) ID() string {
	return s.id
}

func (s *ShardingRule) Columns() []string {
	return s.columns
}

func ShardingRuleFromDB(rule *qdb.ShardingRule) *ShardingRule {
	return &ShardingRule{
		id:      rule.Id,
		columns: rule.Colnames,
	}
}
