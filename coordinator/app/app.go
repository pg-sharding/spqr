package app

import (
	"context"
	"net"
	"sync"

	"github.com/wal-g/tracelog"
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

	tracelog.InfoLogger.Printf("running coordinator app\n")

	wg := &sync.WaitGroup{}

	wg.Add(2)

	go func(wg *sync.WaitGroup) {
		tracelog.InfoLogger.PrintError(app.ServeGrpc(wg))
	}(wg)
	go func(wg *sync.WaitGroup) {
		tracelog.InfoLogger.PrintError(app.ServePsql(wg))
	}(wg)

	wg.Wait()
	tracelog.InfoLogger.Printf("exit")
	return nil
}

func (app *App) ServePsql(wg *sync.WaitGroup) error {

	defer wg.Done()

	tracelog.InfoLogger.Printf("serve psql on %v", config.CoordinatorConfig().Addr)

	listener, err := net.Listen("tcp", config.CoordinatorConfig().Addr)

	if err != nil {
		return err
	}

	for {
		conn, err := listener.Accept()
		tracelog.ErrorLogger.PrintError(err)
		_ = app.coordinator.ProcClient(context.TODO(), conn)
	}
}

func (app *App) ServeGrpc(wg *sync.WaitGroup) error {

	defer wg.Done()

	serv := grpc.NewServer()
	//shhttp.Register(serv)
	reflection.Register(serv)

	tracelog.InfoLogger.Printf("Coordinator Service %v", app.coordinator)
	krserv := provider.NewKeyRangeService(app.coordinator)
	rrserv := provider.NewRoutersService(app.coordinator)
	shardingRulesServ := provider.NewShardingRules(app.coordinator)
	shardServ := provider.NewShardServer(app.coordinator)

	shards.RegisterKeyRangeServiceServer(serv, krserv)
	shards.RegisterRoutersServiceServer(serv, rrserv)
	shards.RegisterShardingRulesServiceServer(serv, shardingRulesServ)
	shards.RegisterShardServiceServer(serv, shardServ)

	httpAddr := config.CoordinatorConfig().HttpAddr

	tracelog.InfoLogger.Printf("serve grpc on %v", httpAddr)

	listener, err := net.Listen("tcp", httpAddr)
	if err != nil {
		return err
	}
	tracelog.InfoLogger.Printf("coordinator listening on %s", httpAddr)

	return serv.Serve(listener)
}
