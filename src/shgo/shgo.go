package shgo

import (
	"fmt"
	"net"
	"reflect"

	"github.com/jackc/pgproto3"
	shhttp "github.com/shgo/src/http"
	"github.com/shgo/src/internal/core"
	"github.com/shgo/src/internal/r"
	"github.com/shgo/src/util"
	"github.com/wal-g/tracelog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

type GlobConfig struct {
	Addr    string `json:"addr" toml:"addr" yaml:"addr"`
	ADMAddr string `json:"adm_addr" toml:"adm_addr" yaml:"adm_addr"`
	PROTO   string `json:"proto" toml:"proto" yaml:"proto"`

	Tlscfg core.TLSConfig `json:"tls_cfg" toml:"tls_cfg" yaml:"tls_cfg"`

	RouterCfg core.RouterConfig `json:"router" toml:"router" yaml:"router"`
}

type Shgo struct {
	Cfg GlobConfig `json:"global" toml:"global" yaml:"global"`

	Router *core.Router `json:"router" toml:"router" yaml:"router"`
	R      r.R
}

const TXREL = 73

func (sg *Shgo) frontend(cl *core.ShClient) error {

	for k, v := range cl.StartupMessage().Parameters {
		tracelog.InfoLogger.Println("log loh %v %v", k, v)
	}

	msgs := make([]pgproto3.Query, 0)
	activeSh := r.NOSHARD

	for {
		tracelog.InfoLogger.Println("round")
		msg, err := cl.Receive()
		if err != nil {
			util.Fatal(err)
			return err
		}
		tracelog.InfoLogger.Println(reflect.TypeOf(msg))
		tracelog.InfoLogger.Println(msg)

		switch v := msg.(type) {
		case *pgproto3.Query:

			shindx := sg.R.Route(v.String)

			msgs = append(msgs, pgproto3.Query{
				String: v.String,
			})

			if shindx == r.NOSHARD && activeSh == r.NOSHARD {

				_ = cl.Send(&pgproto3.Authentication{Type: pgproto3.AuthTypeOk})
				_ = cl.Send(&pgproto3.RowDescription{Fields: []pgproto3.FieldDescription{
					{
						Name:                 "fortune",
						TableOID:             0,
						TableAttributeNumber: 0,
						DataTypeOID:          25,
						DataTypeSize:         -1,
						TypeModifier:         -1,
						Format:               0,
					},
				}})

				_ = cl.Send(&pgproto3.DataRow{Values: [][]byte{[]byte("loh")}})
				_ = cl.Send(&pgproto3.CommandComplete{CommandTag: "SELECT 1"})
				_ = cl.Send(&pgproto3.ReadyForQuery{})

			} else {

				fmt.Printf("get conn to %d\n", shindx)
				if cl.ShardConn() == nil {

					activeSh = shindx

					shConn, err := cl.Route().GetConn(sg.Router.CFG.PROTO, activeSh)

					if err != nil {
						panic(err)
					}

					cl.AssignShrdConn(shConn)
				}
				var txst byte
				for _, msg := range msgs {
					if txst, err = cl.ProcQuery(&msg); err != nil {
						return err
					}
				}

				msgs = make([]pgproto3.Query, 0, 0)

				if err := cl.Send(&pgproto3.ReadyForQuery{}); err != nil {
					return err
				}

				if txst == TXREL {
					fmt.Println("releasing tx\n")

					cl.Route().Unroute(activeSh, cl)

					activeSh = r.NOSHARD
				}
			}

		default:
			//msgs := append(msgs, msg)
		}

		//tracelog.InfoLogger.Printf("crnt msgs buff %+v\n", msgs)
	}

	return nil
}
func (sg *Shgo) serv(conn net.Conn) error {

	client, err := sg.Router.PreRoute(conn)
	if err != nil {
		tracelog.ErrorLogger.PrintError(err)
		return err
	}

	return sg.frontend(client)
}

func (sg *Shgo) ProcPG() error {
	////	listener, err := net.Listen("tcp", "man-a6p8ynmq7hanpybg.db.yandex.net:6432")
	listener, err := net.Listen(sg.Cfg.PROTO, sg.Cfg.Addr)
	util.Fatal(err)
	defer listener.Close()

	for {
		conn, err := listener.Accept()

		util.Fatal(err)
		go sg.serv(conn)
	}
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

func (sg *Shgo) ProcADM() error {
	//	listener, err := net.Listen("tcp", "man-a6p8ynmq7hanpybg.db.yandex.net:6432")
	listener, err := net.Listen(sg.Cfg.PROTO, sg.Cfg.Addr)
	util.Fatal(err)

	defer listener.Close()

	for {
		conn, err := listener.Accept()

		util.Fatal(err)
		go sg.serv(conn)
	}
}

func (sg *Shgo) ServHttp() error {

	serv := grpc.NewServer()
	shhttp.Register(serv)

	reflection.Register(serv)

	lis, err := net.Listen("tcp", "localhost:7000")
	if err != nil {
		return err
	}

	return serv.Serve(lis)
}
