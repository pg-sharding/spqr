package app

import (
	"context"
	"net"
	"sync"

	"github.com/pg-sharding/spqr/pkg/spqrlog"
	"github.com/pg-sharding/spqr/qdb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	"github.com/pg-sharding/spqr/coordinator"
	"github.com/pg-sharding/spqr/coordinator/provider"
	"github.com/pg-sharding/spqr/pkg/config"
	protos "github.com/pg-sharding/spqr/pkg/protos"
)

type App struct {
	coordinator coordinator.Coordinator
}

func NewApp(qdbType string) (*App, error) {
	db, err := qdb.NewQDB(qdbType)
	if err != nil {
		return nil, err
	}
	return &App{
		coordinator: provider.NewCoordinator(db),
	}, nil
}

func (app *App) Run() error {

	spqrlog.Logger.Printf(spqrlog.LOG, "running coordinator app\n")

	wg := &sync.WaitGroup{}

	wg.Add(2)

	go func(wg *sync.WaitGroup) {
		spqrlog.Logger.PrintError(app.ServeGrpc(wg))
	}(wg)
	go func(wg *sync.WaitGroup) {
		spqrlog.Logger.PrintError(app.ServePsql(wg))
	}(wg)

	wg.Wait()

	spqrlog.Logger.Printf(spqrlog.LOG, "exit")
	return nil
}

func (app *App) ServePsql(wg *sync.WaitGroup) error {
	defer wg.Done()

	var lwg sync.WaitGroup

	listen := map[string]struct{}{
		"localhost":                     {},
		config.CoordinatorConfig().Addr: {},
	}

	lwg.Add(len(listen))

	for addr := range listen {
		go func(addr string) {
			defer lwg.Done()
			spqrlog.Logger.Printf(spqrlog.LOG, "serve psql on %v", addr)

			listener, err := net.Listen("tcp", addr)

			if err != nil {
				spqrlog.Logger.Errorf("error trying to bind psql on %v: %v", addr, err)
				return
			}

			for {
				conn, err := listener.Accept()
				spqrlog.Logger.PrintError(err)
				_ = app.coordinator.ProcClient(context.TODO(), conn)
			}
		}(addr)
	}
	lwg.Wait()
	return nil
}

func (app *App) ServeGrpc(wg *sync.WaitGroup) error {

	defer wg.Done()

	serv := grpc.NewServer()
	reflection.Register(serv)

	spqrlog.Logger.Printf(spqrlog.LOG, "Coordinator Service %v", app.coordinator)

	krserv := provider.NewKeyRangeService(app.coordinator)
	rrserv := provider.NewRouterService(app.coordinator)
	toposerv := provider.NewTopologyService(app.coordinator)
	shardingRulesServ := provider.NewShardingRulesServer(app.coordinator)
	shardServ := provider.NewShardServer(app.coordinator)

	protos.RegisterKeyRangeServiceServer(serv, krserv)
	protos.RegisterRouterServiceServer(serv, rrserv)
	protos.RegisterTopologyServiceServer(serv, toposerv)
	protos.RegisterShardingRulesServiceServer(serv, shardingRulesServ)
	protos.RegisterShardServiceServer(serv, shardServ)

	httpAddr := config.CoordinatorConfig().HttpAddr

	spqrlog.Logger.Printf(spqrlog.LOG, "serve grpc on %v", httpAddr)

	listener, err := net.Listen("tcp", httpAddr)
	if err != nil {
		return err
	}
	return serv.Serve(listener)
}
