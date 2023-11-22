package app

import (
	"context"
	"net"
	"sync"

	reuse "github.com/libp2p/go-reuseport"
	"github.com/pg-sharding/spqr/pkg/spqrlog"
	router "github.com/pg-sharding/spqr/router"
	rgrpc "github.com/pg-sharding/spqr/router/grpc"
	"github.com/pg-sharding/spqr/router/port"
	"google.golang.org/grpc"
)

type App struct {
	spqr *router.InstanceImpl
}

func NewApp(sg *router.InstanceImpl) *App {
	return &App{
		spqr: sg,
	}
}

func (app *App) ServeRouter(ctx context.Context) error {
	var lwg sync.WaitGroup

	listen := map[string]port.RouterPortType{
		net.JoinHostPort("localhost", app.spqr.RuleRouter.Config().RouterPort):                         port.DefaultRouterPortType,
		net.JoinHostPort(app.spqr.RuleRouter.Config().Host, app.spqr.RuleRouter.Config().RouterPort):   port.DefaultRouterPortType,
		net.JoinHostPort("localhost", app.spqr.RuleRouter.Config().RouterROPort):                       port.RORouterPortType,
		net.JoinHostPort(app.spqr.RuleRouter.Config().Host, app.spqr.RuleRouter.Config().RouterROPort): port.RORouterPortType,
	}

	lwg.Add(len(listen))

	for addr, portType := range listen {
		go func(address string) {
			defer lwg.Done()
			var listener net.Listener
			var err error

			if app.spqr.RuleRouter.Config().ReusePort {
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
			_ = app.spqr.Run(ctx, listener, portType)
		}(addr)
	}
	lwg.Wait()

	return nil
}

func (app *App) ServeAdminConsole(ctx context.Context) error {
	address := net.JoinHostPort(app.spqr.RuleRouter.Config().Host, app.spqr.RuleRouter.Config().AdminConsolePort)
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return err
	}
	defer func(listener net.Listener) {
		_ = listener.Close()
	}(listener)

	spqrlog.Zero.Info().
		Str("address", address).
		Msg("SPQR Administative Console is ready on")
	return app.spqr.RunAdm(ctx, listener)
}

func (app *App) ServeGrpcApi(ctx context.Context) error {
	address := net.JoinHostPort(app.spqr.RuleRouter.Config().Host, app.spqr.RuleRouter.Config().GrpcApiPort)
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return err
	}

	server := grpc.NewServer()
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
