package app

import (
	"context"
	"fmt"
	"net"
	"os"
	"path"
	"sync"
	"time"

	"github.com/pg-sharding/spqr/pkg/spqrlog"
	"github.com/pg-sharding/spqr/router/port"

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	"github.com/pg-sharding/spqr/coordinator"
	"github.com/pg-sharding/spqr/coordinator/provider"
	"github.com/pg-sharding/spqr/pkg/config"
	protos "github.com/pg-sharding/spqr/pkg/protos"
	sdnotifier "github.com/pg-sharding/spqr/router/sdnotifier"

	"golang.org/x/sync/semaphore"
)

type App struct {
	coordinator coordinator.Coordinator
	sem         *semaphore.Weighted
}

const (
	maxWorkers = 50
)

func NewApp(c coordinator.Coordinator) *App {
	return &App{
		coordinator: c,
		sem:         semaphore.NewWeighted(int64(maxWorkers)),
	}
}

func (app *App) Run(withPsql bool) error {
	spqrlog.Zero.Info().Msg("running coordinator app")

	var notifier *sdnotifier.Notifier
	if config.CoordinatorConfig().UseSystemdNotifier {
		// systemd notifier
		var err error
		notifier, err = sdnotifier.NewNotifier(os.Getenv("NOTIFY_SOCKET"), config.CoordinatorConfig().SystemdNotifierDebug)
		if err != nil {
			return err
		}

		if err := notifier.Ready(); err != nil {
			return fmt.Errorf("could not send ready msg: %s", err)
		}

		go func() {
			for {
				if err := notifier.Notify(); err != nil {
					spqrlog.Zero.Error().Err(err).Msg("error sending systemd notification")
				}
				time.Sleep(sdnotifier.Timeout)
			}
		}()
	}

	go app.coordinator.RunCoordinator(context.TODO(), !withPsql)

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
		wg.Add(1)
		go func(wg *sync.WaitGroup) {
			if err := app.ServeUnixSocket(wg); err != nil {
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

	address := net.JoinHostPort(config.CoordinatorConfig().Host, config.CoordinatorConfig().CoordinatorPort)
	listener, err := net.Listen("tcp", address)
	if err != nil {
		spqrlog.Zero.Error().
			Err(err).
			Msg("error serve coordinator console")
		return err
	}
	spqrlog.Zero.Info().
		Str("address", address).
		Msg("serve coordinator console")

	for {
		conn, err := listener.Accept()
		if err != nil {
			spqrlog.Zero.Error().Err(err).Msg("")
			continue
		}

		if err := app.sem.Acquire(context.Background(), 1); err != nil {
			spqrlog.Zero.Error().Err(err).Msg("")
			continue
		}

		go func() {
			defer app.sem.Release(1)

			err := app.coordinator.ProcClient(context.TODO(), conn, port.DefaultRouterPortType)
			if err != nil {
				spqrlog.Zero.Error().Err(err).Msg("failed to serve client")
			}
		}()
	}
}

func (app *App) ServeGrpcApi(wg *sync.WaitGroup) error {
	defer wg.Done()

	for app.coordinator.IsReadOnly() {
		time.Sleep(time.Second)
	}

	serv := grpc.NewServer()
	reflection.Register(serv)

	krServ := provider.NewKeyRangeService(app.coordinator)
	rrServ := provider.NewRouterService(app.coordinator)
	topServ := provider.NewTopologyService(app.coordinator)
	shardServ := provider.NewShardServer(app.coordinator)
	dsServ := provider.NewDistributionServer(app.coordinator)
	tasksServ := provider.NewTasksServer(app.coordinator)
	refRelServ := provider.NewReferenceRelationServer(app.coordinator)
	metaTranServ := provider.NewMetaTransactionServer(app.coordinator)
	protos.RegisterKeyRangeServiceServer(serv, krServ)
	protos.RegisterRouterServiceServer(serv, rrServ)
	protos.RegisterTopologyServiceServer(serv, topServ)
	protos.RegisterShardServiceServer(serv, shardServ)
	protos.RegisterDistributionServiceServer(serv, dsServ)
	protos.RegisterMoveTasksServiceServer(serv, tasksServ)
	protos.RegisterBalancerTaskServiceServer(serv, tasksServ)
	protos.RegisterReferenceRelationsServiceServer(serv, refRelServ)
	protos.RegisterMetaTransactionServiceServer(serv, metaTranServ)

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

func (app *App) ServeUnixSocket(wg *sync.WaitGroup) error {
	defer wg.Done()

	if err := os.MkdirAll(config.UnixSocketDirectory, 0777); err != nil {
		return err
	}
	socketPath := path.Join(config.UnixSocketDirectory, fmt.Sprintf(".s.PGSQL.%s", config.CoordinatorConfig().CoordinatorPort))
	lAddr := &net.UnixAddr{Name: socketPath, Net: "unix"}
	listener, err := net.ListenUnix("unix", lAddr)
	if err != nil {
		return err
	}

	for {
		conn, err := listener.Accept()
		if err != nil {
			spqrlog.Zero.Error().Err(err).Msg("")
			continue
		}

		if err := app.sem.Acquire(context.Background(), 1); err != nil {
			spqrlog.Zero.Error().Err(err).Msg("")
			continue
		}

		go func() {
			defer app.sem.Release(1)

			err := app.coordinator.ProcClient(context.TODO(), conn, port.UnixSocketPortType)
			if err != nil {
				spqrlog.Zero.Error().Err(err).Msg("failed to serve client")
			}
		}()
	}
}
