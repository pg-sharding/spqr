package app

import (
	"github.com/wal-g/tracelog"
	"net"

	shhttp "github.com/shgo/src/http"
	"github.com/shgo/src/internal/shgo"
	"github.com/shgo/src/util"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

type App struct {
	sg *shgo.Shgo
}

func NewApp(sg *shgo.Shgo) *App {
	return &App{
		sg: sg,
	}
}

func (app *App) ProcPG() error {
	////	listener, err := net.Listen("tcp", "man-a6p8ynmq7hanpybg.db.yandex.net:6432")
	listener, err := net.Listen(app.sg.Cfg.PROTO, app.sg.Cfg.Addr)
	util.Fatal(err)
	defer func() {
		err := listener.Close()
		tracelog.InfoLogger.PrintError(err)
	} ()
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

	lis, err := net.Listen("tcp", "localhost:7000")
	if err != nil {
		return err
	}

	return serv.Serve(lis)
}
