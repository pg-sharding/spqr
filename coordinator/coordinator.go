package coordinator

import (
	"github.com/pg-sharding/spqr/pkg/client"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/pkg/models/shrule"
	"github.com/pg-sharding/spqr/qdb/qdb"
	"github.com/pg-sharding/spqr/world"
)

type Coordinator interface {
	client.InteractRunner
	kr.KeyRangeManager

	AddShardingRule(rule *shrule.ShardingRule) error
	ListShardingRules() ([]*shrule.ShardingRule, error)

	RegisterRouter(r *qdb.Router) error
	RegisterWorld(w world.World) error

	// deprecated
	AddLocalTable(tname string) error
}
