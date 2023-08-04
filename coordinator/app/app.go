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
	protos "github.com/pg-sharding/spqr/pkg/protos"
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
	spqrlog.Zero.Info().Msg("running coordinator app")

	wg := &sync.WaitGroup{}

	wg.Add(2)

	go func(wg *sync.WaitGroup) {
		if err := app.ServeGrpc(wg); err != nil {
			spqrlog.Zero.Error().Err(err).Msg("")
		}
	}(wg)
	go func(wg *sync.WaitGroup) {
		if err := app.ServePsql(wg); err != nil {
			spqrlog.Zero.Error().Err(err).Msg("")
		}
	}(wg)

	wg.Wait()

	spqrlog.Zero.Debug().Msg("exit coordinator app")
	return nil
}

func (app *App) ServePsql(wg *sync.WaitGroup) error {
	defer wg.Done()

	var lwg sync.WaitGroup

	listen := map[string]struct{}{
		"localhost:7002":                {},
		config.CoordinatorConfig().Addr: {},
	}

	lwg.Add(len(listen))

	for addr := range listen {
		go func(addr string) {
			defer lwg.Done()
			spqrlog.Zero.Info().Str("address", addr).Msg("serve psql")

			listener, err := net.Listen("tcp", addr)

			if err != nil {
				spqrlog.Zero.Error().Err(err).Msg("error trying to bind psql")
				return
			}

			for {
				conn, err := listener.Accept()
				spqrlog.Zero.Error().Err(err).Msg("")
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
	
	spqrlog.Zero.Info().
		Str("address", httpAddr).
		Msg("serve grpc coordinator service")

	listener, err := net.Listen("tcp", httpAddr)
	if err != nil {
		return err
	}
	return serv.Serve(listener)
}
