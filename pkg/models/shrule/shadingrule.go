package shrule

import (
	"github.com/pg-sharding/spqr/qdb"
	proto "github.com/pg-sharding/spqr/router/protos"
)

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

func (s *ShardingRule) ToProto() *proto.ShardingRule {
	return &proto.ShardingRule{
		Columns: s.colunms,
	}
}

func (s *ShardingRule) ToSQL() *qdb.ShardingRule {
	return &qdb.ShardingRule{
		Columns: s.colunms,
	}
}
