package shrule

import "github.com/pg-sharding/spqr/qdb"

type ShardingRule struct {
	Id      string
	columns []string
}

// local table sharding rule -> route to world

func NewShardingRule(id string, cols []string) *ShardingRule {
	return &ShardingRule{
		Id:      id,
		columns: cols,
	}
}

func (s *ShardingRule) ID() string {
	return s.Id
}

func (s *ShardingRule) Columns() []string {
	return s.columns
}

func ShardingRuleFromDB(rule *qdb.ShardingRule) *ShardingRule {
	return &ShardingRule{
		Id:      rule.Id,
		columns: rule.Colnames,
	}
}
