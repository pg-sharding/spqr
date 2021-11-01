package coordinator

import (
	"net"

	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/qdb/qdb"
	"github.com/pg-sharding/spqr/world"
)

type Coordinator interface {
	kr.KeyRangeManager

	AddShardingColumn(col string) error
	AddLocalTable(tname string) error

	RegisterRouter(r *qdb.Router) error

	RegisterWorld(w world.World) error

	// cl interaction
	ProcClient(netconn net.Conn) error
}
