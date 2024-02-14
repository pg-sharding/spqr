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

func (app *App) Run(withPsql bool) error {
	spqrlog.Zero.Info().Msg("running coordinator app")

	app.coordinator.RunCoordinator(context.TODO(), !withPsql)

	wg := &sync.WaitGroup{}

	wg.Add(1)
	go func(wg *sync.WaitGroup) {
		if err := app.ServeGrpcApi(wg); err != nil {
			spqrlog.Zero.Error().Err(err).Msg("")
		}
	}(wg)
	if withPsql {
		wg.Add(1)
		go func(wg *sync.WaitGroup) {
			if err := app.ServeCoordinator(wg); err != nil {
				spqrlog.Zero.Error().Err(err).Msg("")
			}
		}(wg)
	}

	wg.Wait()

	spqrlog.Zero.Debug().Msg("exit coordinator app")
	return nil
}

func (app *App) ServeCoordinator(wg *sync.WaitGroup) error {
	defer wg.Done()

	var lwg sync.WaitGroup

	listen := []string{
		"localhost:7002",
		net.JoinHostPort(config.CoordinatorConfig().Host, config.CoordinatorConfig().CoordinatorPort),
	}

	lwg.Add(len(listen))

	for _, l := range listen {
		go func(address string) {
			defer lwg.Done()

			listener, err := net.Listen("tcp", address)
			if err != nil {
				spqrlog.Zero.Error().
					Err(err).
					Msg("error serve coordinator console")
				return
			}
			spqrlog.Zero.Info().
				Str("address", address).
				Msg("serve coordinator console")

			for {
				conn, err := listener.Accept()
				spqrlog.Zero.Error().Err(err).Msg("")
				_ = app.coordinator.ProcClient(context.TODO(), conn)
			}
		}(l)
	}
	lwg.Wait()
	return nil
}

func (app *App) ServeGrpcApi(wg *sync.WaitGroup) error {
	defer wg.Done()

	serv := grpc.NewServer()
	reflection.Register(serv)

	krServ := provider.NewKeyRangeService(app.coordinator)
	rrServ := provider.NewRouterService(app.coordinator)
	topServ := provider.NewTopologyService(app.coordinator)
	shardingRulesServ := provider.NewShardingRulesServer(app.coordinator)
	shardServ := provider.NewShardServer(app.coordinator)
	dsServ := provider.NewDistributionServer(app.coordinator)
	protos.RegisterKeyRangeServiceServer(serv, krServ)
	protos.RegisterRouterServiceServer(serv, rrServ)
	protos.RegisterTopologyServiceServer(serv, topServ)
	protos.RegisterShardingRulesServiceServer(serv, shardingRulesServ)
	protos.RegisterShardServiceServer(serv, shardServ)
	protos.RegisterDistributionServiceServer(serv, dsServ)

	address := net.JoinHostPort(config.CoordinatorConfig().Host, config.CoordinatorConfig().GrpcApiPort)
	listener, err := net.Listen("tcp", address)
	if err != nil {
		spqrlog.Zero.Error().
			Err(err).
			Msg("error serve grpc coordinator service")
		return err
	}

	spqrlog.Zero.Info().
		Str("address", address).
		Msg("serve grpc coordinator service")

	return serv.Serve(listener)
}
