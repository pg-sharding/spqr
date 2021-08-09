package shgo

import (
	"net"

	shhttp "github.com/shgo/src/http"
	"github.com/shgo/src/internal/shgo"
	"github.com/shgo/src/util"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

type App struct {
	sg shgo.Shgo
}

func (app *App) ProcPG() error {
	////	listener, err := net.Listen("tcp", "man-a6p8ynmq7hanpybg.db.yandex.net:6432")
	listener, err := net.Listen(app.sg.Cfg.PROTO, app.sg.Cfg.Addr)
	util.Fatal(err)
	defer listener.Close()

	return app.sg.Run(listener)
}

//
//func (sg *Shgo) servADM(conn net.Conn) error {
//	backend, err := pgproto3.NewBackend(conn, conn)
//	if err != nil {
//		util.Fatal(err)
//		return err
//	}
//
//	_, err = backend.ReceiveStartupMessage()
//	if err != nil {
//		util.Fatal(err)
//		return err
//	}
//
//	//tracelog.InfoLogger.Println(sm)
//
//	backend.Send(&pgproto3.Authentication{Type: pgproto3.AuthTypeOk})
//	backend.Send(&pgproto3.ParameterStatus{Name: "integer_datetimes", Value: "on"})
//	backend.Send(&pgproto3.ParameterStatus{Name: "server_version", Value: "lolkekcheburek"})
//	backend.Send(&pgproto3.ReadyForQuery{})
//	//msgBuf := make([]pgproto3.FrontendMessage, 0)
//
//
//	for {
//		//tracelog.InfoLogger.Println("round")
//		msg, err := backend.Receive()
//		if err != nil {
//			util.Fatal(err)
//			return err
//		}
//		tracelog.InfoLogger.Println(reflect.TypeOf(msg))
//		tracelog.InfoLogger.Println(msg)
//
//		switch v := msg.(type) {
//		case *pgproto3.Query:
//			tracelog.InfoLogger.Println("loh %v", v)
//		}
//
//		//tracelog.InfoLogger.Printf("crnt msgs buff %+v\n", msgs)
//	}
//
//	return nil
//}

func (app *App) ProcADM() error {
	//	listener, err := net.Listen("tcp", "man-a6p8ynmq7hanpybg.db.yandex.net:6432")
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
