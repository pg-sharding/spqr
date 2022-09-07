package app

import (
	"context"
	"net"
	"sync"

	"github.com/pg-sharding/spqr/pkg/spqrlog"

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	"github.com/pg-sharding/spqr/coordinator"
	"github.com/pg-sharding/spqr/coordinator/provider"
	"github.com/pg-sharding/spqr/pkg/config"
	shards "github.com/pg-sharding/spqr/router/protos"
)

type App struct {
	coordinator coordinator.Coordinator
}

func NewApp(c coordinator.Coordinator) *App {
	return &App{
		coordinator: c,
	}
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

	spqrlog.Logger.Printf(spqrlog.LOG, "serve psql on %v", config.CoordinatorConfig().Addr)

	listener, err := net.Listen("tcp", config.CoordinatorConfig().Addr)

	if err != nil {
		return err
	}

	for {
		conn, err := listener.Accept()
		spqrlog.Logger.PrintError(err)
		_ = app.coordinator.ProcClient(context.TODO(), conn)
	}
}

func (app *App) ServeGrpc(wg *sync.WaitGroup) error {

	defer wg.Done()

	serv := grpc.NewServer()
	reflection.Register(serv)

	spqrlog.Logger.Printf(spqrlog.LOG, "Coordinator Service %v", app.coordinator)

	krserv := provider.NewKeyRangeService(app.coordinator)
	rrserv := provider.NewRoutersService(app.coordinator)
	shardingRulesServ := provider.NewShardingRules(app.coordinator)
	shardServ := provider.NewShardServer(app.coordinator)

	shards.RegisterKeyRangeServiceServer(serv, krserv)
	shards.RegisterRoutersServiceServer(serv, rrserv)
	shards.RegisterShardingRulesServiceServer(serv, shardingRulesServ)
	shards.RegisterShardServiceServer(serv, shardServ)

	httpAddr := config.CoordinatorConfig().HttpAddr

	spqrlog.Logger.Printf(spqrlog.LOG, "serve grpc on %v", httpAddr)

	listener, err := net.Listen("tcp", httpAddr)
	if err != nil {
		return err
	}
	return serv.Serve(listener)
}
