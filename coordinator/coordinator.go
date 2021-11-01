package coordinator

import (
	"github.com/pg-sharding/spqr/pkg/models/shrule"
	"net"

	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/qdb/qdb"
	"github.com/pg-sharding/spqr/world"
)

type Coordinator interface {
	kr.KeyRangeManager

	AddShardingRule(rule *shrule.ShardingRule) error

	RegisterRouter(r *qdb.Router) error
	RegisterWorld(w world.World) error

	// cl interaction
	ProcClient(netconn net.Conn) error

	// deprecated
	AddLocalTable(tname string) error
}
