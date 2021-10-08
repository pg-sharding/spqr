package coordinator

import (
	"github.com/pg-sharding/spqr/coordinator/qdb/qdb"
	spqrparser "github.com/pg-sharding/spqr/yacc/console"
)

type router interface {
}

type coordinator interface {
	AddShardingColumn(col string) error
	AddLocalTable(tname string) error

	AddKeyRange(kr qdb.KeyRange) error

	Lock(krid string) error
	UnLock(krid string) error
	Split(req *spqrparser.SplitKeyRange) error
	Unite(req *spqrparser.UniteKeyRange) error
	RegisterRouter(r router) error
}
