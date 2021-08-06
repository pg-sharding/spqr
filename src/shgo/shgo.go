package shgo

import (
	"fmt"
	"net"
	"reflect"

	"github.com/jackc/pgproto3"
	shhttp "github.com/shgo/src/http"
	"github.com/shgo/src/internal/conn"
	"github.com/shgo/src/internal/core"
	"github.com/shgo/src/internal/r"
	"github.com/shgo/src/util"
	"github.com/wal-g/tracelog"
	"golang.org/x/xerrors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

type GlobConfig struct {
	Addr    string `json:"addr" toml:"addr" yaml:"addr"`
	ADMAddr string `json:"adm_addr" toml:"adm_addr" yaml:"adm_addr"`
	PROTO   string `json:"proto" toml:"proto" yaml:"proto"`

	Tlscfg core.TLSConfig `json:"tls_
type relayState struct {
	txActive bool

	activeShard int
}

type ConnManager interface {
	TXBeginCB(cl *core.ShClient, rst *relayState) error
	TXEndCB(cl *core.ShClient, rst *relayState) error

	RouteCB(cl *core.ShClient, rst *relayState) error
	ValidateReRoute(rst *relayState) bool
}

type TxConnManager struct {
}

func NewTxConnManager() *TxConnManager {
	return &TxConnManager{

	}
}

func (t *TxConnManager) RouteCB(cl *core.ShClient, rst *relayState) error {

	shConn, err := cl.Route().GetConn("tcp6", rst.activeShard)

	if err != nil {
		return err
	}

	cl.AssignShrdConn(shConn)

	return nil
}

func (t *TxConnManager) ValidateReRoute(rst *relayState) bool {
	return rst.activeShard == r.NOSHARD || rst.txActive
}

func (t *TxConnManager) TXBeginCB(cl *core.ShClient, rst *relayState) error {
	return nil
}

func (t *TxConnManager) TXEndCB(cl *core.ShClient, rst *relayState) error {

	fmt.Println("releasing tx\n")

	cl.Route().Unroute(rst.activeShard, cl)

	return nil
}

var _ ConnManager = &TxConnManager{}

type SessConnManager struct {

}
cfg" toml:"tls_cfg" yaml:"tls_cfg"`

	RouterCfg core.RouterConfig `json:"router" toml:"router" yaml:"router"`
}

type Shgo struct {
	Cfg GlobConfig `json:"global" toml:"global" yaml:"global"`

	Router *core.Router `json:"router" toml:"router" yaml:"router"`
	R      r.R
}

const TXREL = 73

func frontend(rt r.R, cl *core.ShClient, cmngr conn.ConnManager) error {

	for k, v := range cl.StartupMessage().Parameters {
		tracelog.InfoLogger.Println("log loh %v %v", k, v)
	}

	msgs := make([]pgproto3.Query, 0)

	rst := &conn.RelayState{
		ActiveShard: r.NOSHARD,
		TxActive:    false,
	}

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

			msgs = append(msgs, pgproto3.Query{
				String: v.String,
			})

			// txactive == 0 || activeSh == r.NOSHARD
			if cmngr.ValidateReRoute(rst) {
				shindx := rt.Route(v.String)

				if shindx == r.NOSHARD && rst.ActiveShard == r.NOSHARD {

					if err := cl.DefaultReply(); err != nil {
						return err
					}

					break
				}

				fmt.Printf("get conn to %d\n", shindx)

				if rst.ActiveShard != r.NOSHARD {
					cl.Route().Unroute(rst.ActiveShard, cl)
				}

				rst.ActiveShard = shindx

				if err := cmngr.RouteCB(cl, rst); err != nil {
					return err
				}
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
				if rst.TxActive {
					if err := cmngr.TXEndCB(cl, rst); err != nil {
						return err
					}
					rst.TxActive = false
				}
			} else {
				if rst.TxActive {
					if err := cmngr.TXBeginCB(cl, rst); err != nil {
						return err
					}
					rst.TxActive = true
				}
			}

		default:
			//msgs := append(msgs, msg)
		}

		//tracelog.InfoLogger.Printf("crnt msgs buff %+v\n", msgs)
	}

	return nil
}

func (sg *Shgo) serv(netconn net.Conn) error {

	client, err := sg.Router.PreRoute(netconn)
	if err != nil {
		tracelog.ErrorLogger.PrintError(err)
		return err
	}

	var cmngr conn.ConnManager

	switch client.Rule().PoolingMode {
	case conn.PoolingModeSession:
		cmngr = nil
	case conn.PoolingModeTransaction:
		cmngr = conn.NewTxConnManager()
	default:
		return xerrors.Errorf("unknown pooling mode %v", client.Rule().PoolingMode)
	}

	return frontend(sg.R, client, cmngr)
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
