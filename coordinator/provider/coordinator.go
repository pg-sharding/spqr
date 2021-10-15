package provider

import (
	"net"

	"github.com/pg-sharding/spqr/coordinator"
	shhttp "github.com/pg-sharding/spqr/grpc"
	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/qdb/qdb"
	router "github.com/pg-sharding/spqr/router/pkg"
	shards "github.com/pg-sharding/spqr/router/protos"
	"github.com/pg-sharding/spqr/world"
	spqrparser "github.com/pg-sharding/spqr/yacc/console"
	"github.com/wal-g/tracelog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

type dcoordinator struct {
	db qdb.QrouterDB
}

func (d *dcoordinator) RegisterWorld(w world.World) error {
	panic("implement me")
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

var _ coordinator.Coordinator = &dcoordinator{}

func NewCoordinator(db qdb.QrouterDB) *dcoordinator {
	return &dcoordinator{
		db: db,
	}
}

func (d *dcoordinator) RegisterRouter(r router.Router) error {
	return nil
}
func (d *dcoordinator) Run() error {
	serv := grpc.NewServer()
	shhttp.Register(serv)
	reflection.Register(serv)

	krserv := NewKeyRangeService(d)
	rrserv := NewRoutersService(d)

	shards.RegisterKeyRangeServiceServer(serv, krserv)
	shards.RegisterRoutersServiceServer(serv, rrserv)

	httpAddr := config.Get().CoordinatorHttpAddr
	httpAddr = "localhost:7002"

	listener, err := net.Listen("tcp", httpAddr)
	if err != nil {
		return err
	}
	tracelog.InfoLogger.Printf("world listening on %s", httpAddr)

	return serv.Serve(listener)
}
