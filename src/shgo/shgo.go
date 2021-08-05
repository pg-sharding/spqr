package shgo

import (
	bufio "bufio"
	"crypto/tls"
	"encoding/binary"
	"fmt"
	shhttp "github.com/shgo/src/http"
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
	Addr        string      `json:"addr" toml:"addr" yaml:"addr"`
	ADMAddr     string      `json:"adm_addr" toml:"adm_addr" yaml:"adm_addr"`
	PROTO       string      `json:"proto" toml:"proto" yaml:"proto"`
	ConnCfg     conn.Config `json:"conn_cfg" toml:"conn_cfg" yaml:"conn_cfg"`
	CAPath      string      `json:"ca_path" toml:"ca_path" yaml:"ca_path"`
	ServPath    string      `json:"serv_key_path" toml:"serv_key_path" yaml:"serv_key_path"`
	TLSSertPath string      `json:"tls_cert_path" toml:"tls_cert_path" yaml:"tls_cert_path"`
}

const DEFAULT_MAX_CONN_PER_ROUTE = 3

type Shgo struct {
	Cfg Config

	Od conn.Connector
	R  r.R
}

const TXREL = 73

func (sg *Shgo) frontend(backend *pgproto3.Backend, sm *pgproto3.StartupMessage) error {

	for k, v := range sm.Parameters {
		tracelog.InfoLogger.Println("log loh %v %v", k, v)
	}
	msgs := make([]pgproto3.Query, 0)

	var shardConn *pgproto3.Frontend
	shardConn = nil

	activeSh := r.NOSHARD

	for {
		tracelog.InfoLogger.Println("round")
		msg, err := backend.Receive()
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

				_ = backend.Send(&pgproto3.Authentication{Type: pgproto3.AuthTypeOk})
				_ = backend.Send(&pgproto3.RowDescription{Fields: []pgproto3.FieldDescription{
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

				_ = backend.Send(&pgproto3.DataRow{Values: [][]byte{[]byte("loh")}})
				_ = backend.Send(&pgproto3.CommandComplete{CommandTag: "SELECT 1"})
				_ = backend.Send(&pgproto3.ReadyForQuery{})

			} else {

				fmt.Printf("get conn to %d\n", shindx)
				if shardConn == nil {
					activeSh = shindx
					shardConn, err = sg.Od.Connect(shindx)
					if err != nil {
						return err
					}
				}
				var txst byte
				for _, msg := range msgs {
					if txst, err = sg.Od.ProcQuery(backend, &msg, shardConn); err != nil {
						return err
					}
				}

				msgs = make([]pgproto3.Query, 0, 0)

				if err := backend.Send(&pgproto3.ReadyForQuery{}); err != nil {
					return err
				}

				if txst == TXREL {
					fmt.Println("releasing tx\n")
					sg.Od.ReleaseConn(activeSh, shardConn)
					shardConn = nil
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

const sslproto = 80877103

func (sg *Shgo) serv(conn net.Conn) error {

	var backend *pgproto3.Backend

	cr := pgproto3.NewChunkReader(bufio.NewReader(conn))

	var sm *pgproto3.StartupMessage

	headerRaw, err := cr.Next(4)
	if err != nil {
		return err
	}
	msgSize := int(binary.BigEndian.Uint32(headerRaw) - 4)

	buf, err := cr.Next(msgSize)
	if err != nil {
		return err
	}

	protVer := binary.BigEndian.Uint32(buf)

	tracelog.InfoLogger.Println("prot version %v", protVer)

	if protVer == sslproto {
		_, err := conn.Write([]byte{'S'})
		if err != nil {
			panic(err)
		}

		fmt.Printf("%v %v\n", sg.Cfg.TLSSertPath, sg.Cfg.ServPath)
		cert, err := tls.LoadX509KeyPair(sg.Cfg.TLSSertPath, sg.Cfg.ServPath)
		if err != nil {
			panic(err)
		}

		cfg := &tls.Config{Certificates: []tls.Certificate{cert}, InsecureSkipVerify: true}
		conn = tls.Server(conn, cfg)

		fmt.Printf("%v\n", conn)

		backend, err = pgproto3.NewBackend(pgproto3.NewChunkReader(bufio.NewReader(conn)), conn)

		if err != nil {
			panic(err)
		}

		sm, err = backend.ReceiveStartupMessage()

		if err != nil {
			panic(err)
		}

	} else if protVer == pgproto3.ProtocolVersionNumber {
		// reuse
		sm = &pgproto3.StartupMessage{}
		err = sm.Decode(buf)
		if err != nil {
			util.Fatal(err)
			return err
		}

		backend, err = pgproto3.NewBackend(cr, conn)
		if err != nil {
			util.Fatal(err)
			return err
		}
	}
	//!! frontend auth

	tracelog.InfoLogger.Println("sm prot ver %v", sm.ProtocolVersion)
	for k, v := range sm.Parameters {
		tracelog.InfoLogger.Printf("%v %v\n", k, v)
	}

	backend.Send(&pgproto3.Authentication{Type: pgproto3.AuthTypeOk})
	backend.Send(&pgproto3.ParameterStatus{Name: "integer_datetimes", Value: "on"})
	backend.Send(&pgproto3.ParameterStatus{Name: "server_version", Value: "lolkekcheburek"})
	backend.Send(&pgproto3.ReadyForQuery{})
	//msgBuf := make([]pgproto3.FrontendMessage, 0)
	return sg.frontend(backend, sm)
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

	lis, err  := net.Listen("tcp", "localhost:7000")
	if err != nil {
		return err
	}

	return serv.Serve(lis)
	//
	//http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
	//	fmt.Println("loh1")
	//})
	//http.HandleFunc("/listshards", func(w http.ResponseWriter, r *http.Request) {
	//	fmt.Println("loh2")
	//
	//	w.Header().Set("Content-Type", "text/plain; charset=utf-8") // normal header
	//	w.WriteHeader(http.StatusOK)
	//
	//	_, _ = io.WriteString(w, strings.Join(sg.Od.ListShards(), ","))
	//})
	//return http.ListenAndServe(":7000", nil)
}

func (sg *Shgo) procHttp() error {
	//listener, err := net.Listen(sg.Cfg.PROTO, sg.Cfg.Addr)
	//fatal(err)
	//
	//defer listener.Close()
	//
	//for {
	//	conn, err := listener.Accept()
	//	fatal(err)
	//	go sg.serv(conn)
	//}

	return nil
}
