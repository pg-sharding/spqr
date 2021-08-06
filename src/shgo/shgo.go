package shgo

import (
	"fmt"
	shhttp "github.com/shgo/src/http"
	"github.com/shgo/src/internal/core"
	"google.golang.org/grpc"
	"net"
	"reflect"
	//"strconv"

	//	"github.com/jackc/pgx"
	//	"encoding/json"
	"github.com/jackc/pgproto3"
	"github.com/shgo/src/internal/conn"
	"github.com/shgo/src/internal/r"
	"github.com/shgo/src/util"
	//spqr "github.com/shgo/parser"
	//sqlp "github.com/blastrain/vitess-sqlparser/sqlparser"
	//	"github.com/pganalyze/pg_query_go"
	//"github.com/wal-g///tracelog"
	//"os"
	//"crypto/x509"
	"github.com/wal-g/tracelog"
	"google.golang.org/grpc/reflection"
)

type Config struct {
	Addr    string      `json:"addr" toml:"addr" yaml:"addr"`
	ADMAddr string      `json:"adm_addr" toml:"adm_addr" yaml:"adm_addr"`
	PROTO   string      `json:"proto" toml:"proto" yaml:"proto"`
	ConnCfg conn.Config `json:"conn_cfg" toml:"conn_cfg" yaml:"conn_cfg"`

	Tlscfg core.TLSConfig `json:"tls_cfg" toml:"tls_cfg" yaml:"tls_cfg"`
}

type Shgo struct {
	Cfg Config

	Od conn.Connector
	router core.Router
	R  r.R
}

const TXREL = 73

func (sg *Shgo) frontend(route *core.Route) error {

	for k, v := range route.Client().StartupMessage().Parameters {
		tracelog.InfoLogger.Println("log loh %v %v", k, v)
	}
	msgs := make([]pgproto3.Query, 0)


	activeSh := r.NOSHARD

	for {
		tracelog.InfoLogger.Println("round")
		msg, err := route.Client().Receive()
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

				_ = route.Client().Send(&pgproto3.Authentication{Type: pgproto3.AuthTypeOk})
				_ = route.Client().Send(&pgproto3.RowDescription{Fields: []pgproto3.FieldDescription{
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

				_ = route.Client().Send(&pgproto3.DataRow{Values: [][]byte{[]byte("loh")}})
				_ = route.Client().Send(&pgproto3.CommandComplete{CommandTag: "SELECT 1"})
				_ = route.Client().Send(&pgproto3.ReadyForQuery{})

			} else {

				fmt.Printf("get conn to %d\n", shindx)
				if route.ShardConn() == nil {
					activeSh = shindx
					netconn, err := sg.Od.Connect(shindx)
					route.PushConn(netconn)
					if err != nil {
						return err
					}
				}
				var txst byte
				for _, msg := range msgs {
					if txst, err = route.ProcQuery(&msg); err != nil {
						return err
					}
				}

				msgs = make([]pgproto3.Query, 0, 0)

				if err := route.Client().Send(&pgproto3.ReadyForQuery{}); err != nil {
					return err
				}

				if txst == TXREL {
					fmt.Println("releasing tx\n")
					route.Unroute()
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

	route, err := sg.router.PreRoute(conn)
	if err != nil {
		return err
	}

	//msgBuf := make([]pgproto3.FrontendMessage, 0)
	return sg.frontend(route)
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