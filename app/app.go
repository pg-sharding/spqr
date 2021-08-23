package app

import (
	"net"

	shhttp "github.com/pg-sharding/spqr/http"
	"github.com/pg-sharding/spqr/internal/spqr"
	"github.com/pg-sharding/spqr/util"
	"github.com/wal-g/tracelog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	reuse "github.com/libp2p/go-reuseport"
)

type App struct {
	sg *spqr.Spqr
}

func NewApp(sg *spqr.Spqr) *App {
	return &App{
		sg: sg,
	}
}

func (app *App) ProcPG() error {
	////	listener, err := net.Listen("tcp", "man-a6p8ynmq7hanpybg.db.yandex.net:6432")
	listener, err := reuse.Listen(app.sg.Cfg.PROTO, app.sg.Cfg.Addr)
	util.Fatal(err)
	defer func() {
		err := listener.Close()
		tracelog.InfoLogger.PrintError(err)
	}()
	return app.sg.Run(listener)
}

func (app *App) ProcADM() error {
	//	listener, err := net.Listen("tcp", "man-a6p8ynmq7hanpybg.db.yandex.net:7432")

	//tracelog.InfoLogger.Print("listening adm   !!!")
	listener, err := net.Listen(app.sg.Cfg.PROTO, app.sg.Cfg.ADMAddr)
	util.Fatal(err)

	defer listener.Close()
	return app.sg.RunAdm(listener)
}

func (app *App) ServHttp() error {

	serv := grpc.NewServer()
	shhttp.Register(serv)

	reflection.Register(serv)

	lis, err := net.Listen("tcp", app.sg.Cfg.HttpConfig.Addr)
	if err != nil {
		return err
	}

	return serv.Serve(lis)
}
