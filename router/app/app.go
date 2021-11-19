package app

import (
	"context"
	"net"

	reuse "github.com/libp2p/go-reuseport"
	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/router/grpcqrouter"
	router2 "github.com/pg-sharding/spqr/router/pkg"
	"github.com/wal-g/tracelog"
	"google.golang.org/grpc"
)

type App struct {
	spqr *router2.RouterImpl
}

func NewApp(sg *router2.RouterImpl) *App {
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
	defer listener.Close()

	tracelog.InfoLogger.Printf("ProcPG listening %s by %s", addr, proto)
	return app.spqr.Run(listener)
}

func (app *App) ProcADM(ctx context.Context) error {
	proto, admaddr := config.RouterConfig().Proto, config.RouterConfig().ADMAddr

	listener, err := net.Listen(proto, admaddr)
	if err != nil {
		return err
	}
	defer listener.Close()

	tracelog.InfoLogger.Printf("ProcADM listening %s by %s", admaddr, proto)
	return app.spqr.RunAdm(ctx, listener)
}

func (app *App) ServGrpc(ctx context.Context) error {
	serv := grpc.NewServer()
	//shhttp.Register(serv)
	//reflection.Register(serv)
	grpcqrouter.Register(serv, app.spqr.Qrouter)

	httpAddr := config.RouterConfig().HttpAddr
	listener, err := net.Listen("tcp", httpAddr)
	if err != nil {
		return err
	}

	tracelog.InfoLogger.Printf("ServGrpc listening %s by tcp", httpAddr)
	return serv.Serve(listener)
}
