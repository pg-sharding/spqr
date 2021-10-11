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


type dcoordinator struct {
	db qdb.QrouterDB
}

func (d *dcoordinator) AddShardingColumn(col string) error {
	panic("implement me")
}

func (d *dcoordinator) AddLocalTable(tname string) error {
	panic("implement me")
}

func (d *dcoordinator) AddKeyRange(kr qdb.KeyRange) error {
	panic("implement me")
}

func (d *dcoordinator) Lock(krid string) error {
	panic("implement me")
}

func (d *dcoordinator) UnLock(krid string) error {
	panic("implement me")
}

func (d *dcoordinator) Split(req *spqrparser.SplitKeyRange) error {
	panic("implement me")
}

func (d *dcoordinator) Unite(req *spqrparser.UniteKeyRange) error {
	panic("implement me")
}

var _ coordinator = &dcoordinator{}

func NewCoordinator(db qdb.QrouterDB) *dcoordinator {
	return &dcoordinator{
		db: db,
	}
}

func (d *dcoordinator) RegisterRouter(r router) error {
	return nil
}