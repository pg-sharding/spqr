package app

import (
	"net"

	shhttp "github.com/pg-sharding/spqr/http"
	"github.com/pg-sharding/spqr/internal"
	"github.com/wal-g/tracelog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	reuse "github.com/libp2p/go-reuseport"
)

type App struct {
	spqr *spqr.Spqr
}

func NewApp(sg *spqr.Spqr) *App {
	return &App{
		spqr: sg,
	}
}

// TODO split into separate apps?
func (app *App) ProcPG() error {
	////	listener, err := net.Listen("tcp", "man-a6p8ynmq7hanpybg.db.yandex.net:6432")
	listener, err := reuse.Listen(app.spqr.Cfg.PROTO, app.spqr.Cfg.Addr)
	if err != nil {
		return err
	}
	defer listener.Close()
	tracelog.InfoLogger.Printf("ProcPG listening %s by %s", app.spqr.Cfg.Addr, app.spqr.Cfg.PROTO)
	return app.spqr.Run(listener)
}

func (app *App) ProcADM() error {
	//	listener, err := net.Listen("tcp", "man-a6p8ynmq7hanpybg.db.yandex.net:7432")
	listener, err := net.Listen(app.spqr.Cfg.PROTO, app.spqr.Cfg.ADMAddr)
	if err != nil {
		return err
	}
	defer listener.Close()
	tracelog.InfoLogger.Printf("ProcADM listening %s by %s", app.spqr.Cfg.ADMAddr, app.spqr.Cfg.PROTO)
	return app.spqr.RunAdm(listener)
}

func (app *App) ServHttp() error {
	serv := grpc.NewServer()
	shhttp.Register(serv)
	reflection.Register(serv)
	listener, err := net.Listen("tcp", app.spqr.Cfg.HttpConfig.Addr)
	if err != nil {
		return err
	}
	tracelog.InfoLogger.Printf("ServHttp listening %s by tcp", app.spqr.Cfg.HttpConfig.Addr)
	return serv.Serve(listener)
}
