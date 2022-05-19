package app

import (
	"context"
	"net"

	"github.com/pg-sharding/spqr/pkg/spqrlog"

	reuse "github.com/libp2p/go-reuseport"
	"google.golang.org/grpc"

	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/router/grpcqrouter"
	router "github.com/pg-sharding/spqr/router/pkg"
)

type App struct {
	spqr *router.RouterImpl
}

func NewApp(sg *router.RouterImpl) *App {
	return &App{
		spqr: sg,
	}
}

func (app *App) ProcPG(ctx context.Context) error {
	proto, addr := config.RouterConfig().Proto, config.RouterConfig().Addr

	listener, err := reuse.Listen(proto, addr)
	if err != nil {
		return err
	}
	defer func(listener net.Listener) {
		_ = listener.Close()
	}(listener)

	spqrlog.Logger.Printf(spqrlog.INFO, "ProcPG listening %s by %s", addr, proto)
	return app.spqr.Run(ctx, listener)
}

func (app *App) ProcADM(ctx context.Context) error {
	proto, admaddr := config.RouterConfig().Proto, config.RouterConfig().ADMAddr

	listener, err := net.Listen(proto, admaddr)
	if err != nil {
		return err
	}
	defer func(listener net.Listener) {
		_ = listener.Close()
	}(listener)

	spqrlog.Logger.Printf(spqrlog.INFO, "ProcADM listening %s by %s", admaddr, proto)
	return app.spqr.RunAdm(ctx, listener)
}

func (app *App) ServGrpc(ctx context.Context) error {
	serv := grpc.NewServer()
	grpcqrouter.Register(serv, app.spqr.Qrouter)

	httpAddr := config.RouterConfig().HttpAddr
	listener, err := net.Listen("tcp", httpAddr)
	if err != nil {
		return err
	}

	spqrlog.Logger.Printf(spqrlog.INFO, "ServGrpc listening %s by tcp", httpAddr)
	go func() {
		_ = serv.Serve(listener)
	}()

	<-ctx.Done()
	serv.GracefulStop()
	return nil
}
