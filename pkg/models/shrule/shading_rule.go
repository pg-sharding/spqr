package shrule

type ShardingRule struct {
	colunms []string
}

// local table sharding rule -> route to world

func NewShardingRule(cols []string) *ShardingRule {
	return &ShardingRule{
		colunms: cols,
	}
}

func (s *ShardingRule) Columns() []string {
	return s.colunms
}

