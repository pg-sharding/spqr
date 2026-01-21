package app

import (
	"context"
	"fmt"
	"net"
	"os"
	"path"
	"sync"
	"time"

	reuse "github.com/libp2p/go-reuseport"
	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/spqrlog"
	rgrpc "github.com/pg-sharding/spqr/router/grpc"
	"github.com/pg-sharding/spqr/router/instance"
	"github.com/pg-sharding/spqr/router/port"
	"github.com/pg-sharding/spqr/router/recovery"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
)

type App struct {
	spqr *instance.InstanceImpl
}

func NewApp(sg *instance.InstanceImpl) *App {
	return &App{
		spqr: sg,
	}
}

func (app *App) ServeRouter(ctx context.Context) error {
	var lwg sync.WaitGroup

	listen := map[string]port.RouterPortType{
		net.JoinHostPort(config.RouterConfig().Host, config.RouterConfig().RouterPort): port.DefaultRouterPortType,
	}

	if config.RouterConfig().RouterROPort != "" {
		listen[net.JoinHostPort(config.RouterConfig().Host, config.RouterConfig().RouterROPort)] = port.RORouterPortType
	}

	lwg.Add(len(listen))

	for addr, portType := range listen {
		go func(address string, pt port.RouterPortType) {
			defer lwg.Done()
			var listener net.Listener
			var err error

			if config.RouterConfig().ReusePort {
				listener, err = reuse.Listen("tcp", address)
				if err != nil {
					spqrlog.Zero.Info().Err(err).Msg("failed to listen psql")
					return
				}
			} else {
				listener, err = net.Listen("tcp", address)
				if err != nil {
					spqrlog.Zero.Info().Err(err).Msg("failed to listen psql")
					return
				}
			}
			defer func(listener net.Listener) {
				_ = listener.Close()
			}(listener)

			spqrlog.Zero.Info().
				Str("address", address).
				Msg("SPQR Router is ready by postgresql proto")
			err = app.spqr.Run(ctx, listener, pt)
			if err != nil {
				spqrlog.Zero.Error().
					Err(err).
					Msg("SPQR Router due to error")
			}
		}(addr, portType)
	}
	lwg.Wait()

	return nil
}

func (app *App) ServeAdminConsole(ctx context.Context) error {
	address := net.JoinHostPort(config.RouterConfig().Host, config.RouterConfig().AdminConsolePort)
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return err
	}
	defer func(listener net.Listener) {
		_ = listener.Close()
	}(listener)

	spqrlog.Zero.Info().
		Str("address", address).
		Msg("SPQR Administrative Console is ready on")
	return app.spqr.RunAdm(ctx, listener)
}

func (app *App) ServeGrpcApi(ctx context.Context) error {
	address := net.JoinHostPort(config.RouterConfig().Host, config.RouterConfig().GrpcApiPort)
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return err
	}

	server := grpc.NewServer(grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{PermitWithoutStream: true}))
	rgrpc.Register(server, app.spqr.Qrouter, app.spqr.Mgr, app.spqr.RuleRouter)
	spqrlog.Zero.Info().
		Str("address", address).
		Msg("SPQR GRPC API is ready on")
	go func() {
		_ = server.Serve(listener)
	}()

	<-ctx.Done()
	server.GracefulStop()
	return nil
}

func (app *App) ServiceUnixSocket(ctx context.Context) error {
	if err := os.MkdirAll(config.UnixSocketDirectory, 0777); err != nil {
		return err
	}
	socketPath := path.Join(config.UnixSocketDirectory, fmt.Sprintf(".s.PGSQL.%s", config.RouterConfig().RouterPort))
	lAddr := &net.UnixAddr{Name: socketPath, Net: "unix"}
	listener, err := net.ListenUnix("unix", lAddr)
	if err != nil {
		return err
	}
	defer func(listener net.Listener) {
		_ = listener.Close()
	}(listener)

	spqrlog.Zero.Info().
		Msg("SPQR Router is ready by unix socket")
	go func() {
		_ = app.spqr.Run(ctx, listener, port.UnixSocketPortType)
	}()

	<-ctx.Done()
	return nil
}

func (app *App) ServeWD(ctx context.Context) error {

	wd, err := recovery.NewTwoPCWatchDog(config.RouterConfig().WatchdogBackendRule)

	if err != nil {
		return err
	}

	for {
		select {
		case <-ctx.Done():
			spqrlog.Zero.Info().Msg("recovery watchdog done")
			return nil
		default:
			err := wd.RecoverDistributedTx()
			if err != nil {
				spqrlog.Zero.Error().Err(err)
			}

			/* wait for some period of time */
			time.Sleep(config.ValueOrDefaultDuration(config.RouterConfig().WatchdogSleepInterval, time.Second))
		}
	}
}
