package app

import (
	"context"
	"net"

	"github.com/pg-sharding/spqr/pkg/spqrlog"

	reuse "github.com/libp2p/go-reuseport"
	"google.golang.org/grpc"

	rgrpc "github.com/pg-sharding/spqr/router/grpc"
	router "github.com/pg-sharding/spqr/router/pkg"
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
	address := net.JoinHostPort(app.spqr.RuleRouter.Config().Host, app.spqr.RuleRouter.Config().RouterPort)
	listener, err := reuse.Listen("tcp", address)
	if err != nil {
		return err
	}
	defer func(listener net.Listener) {
		_ = listener.Close()
	}(listener)

	spqrlog.Logger.Printf(spqrlog.INFO, "SPQR Router is ready on %s by postgresql proto", address)
	return app.spqr.Run(ctx, listener)
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

	spqrlog.Logger.Printf(spqrlog.INFO, "SPQR Administative Console is ready on %s", address)
	return app.spqr.RunAdm(ctx, listener)
}

func (app *App) ServeGrpcApi(ctx context.Context) error {
	address := net.JoinHostPort(app.spqr.RuleRouter.Config().Host, app.spqr.RuleRouter.Config().GrpcApiPort)
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return err
	}

	server := grpc.NewServer()
	rgrpc.Register(server, app.spqr.Qrouter)
	spqrlog.Logger.Printf(spqrlog.INFO, "SPQR GRPC API is ready on %s", address)
	go func() {
		_ = server.Serve(listener)
	}()

	<-ctx.Done()
	server.GracefulStop()
	return nil
}
