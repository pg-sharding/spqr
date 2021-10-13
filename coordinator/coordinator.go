package coordinator

import (
	"github.com/pg-sharding/spqr/qdb/qdb"
	router2 "github.com/pg-sharding/spqr/router/router"
	"github.com/pg-sharding/spqr/world"
	spqrparser "github.com/pg-sharding/spqr/yacc/console"
)

type Coordinator interface {
	AddShardingColumn(col string) error
	AddLocalTable(tname string) error

	AddKeyRange(kr qdb.KeyRange) error

	Lock(krid string) error
	UnLock(krid string) error
	Split(req *spqrparser.SplitKeyRange) error
	Unite(req *spqrparser.UniteKeyRange) error
	RegisterRouter(r router2.Router) error
	RegisterWorld(w world.World) error
}
