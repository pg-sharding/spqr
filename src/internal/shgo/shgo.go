package shgo

import (
	"fmt"
	"github.com/shgo/yacc/shgoparser"
	"net"
	"reflect"

	"github.com/jackc/pgproto3"
	"github.com/shgo/src/internal/core"
	"github.com/shgo/src/internal/r"
	"github.com/shgo/src/util"
	shgop "github.com/shgo/yacc/shgoparser"
	"github.com/wal-g/tracelog"
)

type GlobConfig struct {
	Addr    string `json:"addr" toml:"addr" yaml:"addr"`
	ADMAddr string `json:"adm_addr" toml:"adm_addr" yaml:"adm_addr"`
	PROTO   string `json:"proto" toml:"proto" yaml:"proto"`

	Tlscfg core.TLSConfig `json:"tls_cfg" toml:"tls_cfg" yaml:"tls_cfg"`

	RouterCfg core.RouterConfig `json:"router" toml:"router" yaml:"router"`
}

type Shgo struct {
	Cfg    GlobConfig
	Router *core.Router
	R      r.R
}

const TXREL = 73

func frontend(rt r.R, cl *core.ShClient, cmngr core.ConnManager) error {

	for k, v := range cl.StartupMessage().Parameters {
		tracelog.InfoLogger.Println("log loh %v %v", k, v)
	}

	msgs := make([]pgproto3.Query, 0)

	rst := &core.RelayState{
		ActiveShardIndx: r.NOSHARD,
		ActiveShardConn: nil,
		TxActive:        false,
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

				fmt.Printf("rerouting\n")
				shindx := rt.Route(v.String)

				if shindx == r.NOSHARD {
					if err := cl.DefaultReply(); err != nil {
						return err
					}

					break
				}

				//if shindx == r.NOSHARD {
				//	break
				//}

				fmt.Printf("get conn to %d\n", shindx)

				if rst.ActiveShardIndx != r.NOSHARD {
					fmt.Printf("unrouted prev shard conn\n")

					if err := cl.Route().Unroute(rst.ActiveShardIndx, cl); err != nil {
						return err
					}
				}

				rst.ActiveShardIndx = shindx

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
				if !rst.TxActive {
					if err := cmngr.TXBeginCB(cl, rst); err != nil {
						return err
					}
					rst.TxActive = true
				}
			}

			tracelog.InfoLogger.Printf(" relay state is %v\n", rst)

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

	cmngr, err := core.InitClConnection(client)
	if err != nil {
		return err
	}

	return frontend(sg.R, client, cmngr)
}

func (sg *Shgo) Run(listener net.Listener) error {
	return nil
	for {
		conn, err := listener.Accept()

		util.Fatal(err)
		go func() {
			if err := sg.serv(conn); err != nil {
				util.Fatal(err)
			}
		}()
	}

	return nil
}
func (sg *Shgo) servAdm(netconn net.Conn) error {

	cl := core.NewClient(netconn)

	if err := cl.Init(sg.Cfg.RouterCfg.TLSCfg, sg.Cfg.RouterCfg.ReqSSL); err != nil {
		return err
	}

	//_, err := core.InitClConnection(cl)
	//if err != nil {
	//	return err
	//}

	for _, msg := range []pgproto3.BackendMessage{
		&pgproto3.Authentication{Type: pgproto3.AuthTypeOk},
		&pgproto3.ParameterStatus{Name: "integer_datetimes", Value: "on"},
		&pgproto3.ParameterStatus{Name: "server_version", Value: "lolkekcheburek"},
		&pgproto3.ReadyForQuery{},
	} {
		if err :=
			cl.Send(msg); err != nil {

			tracelog.InfoLogger.Printf("server starsup resp failed %v", msg)

			return err
		}
	}
	for {

		msg, err := cl.Receive()
		if err != nil {
			util.Fatal(err)
			return err
		}
		tracelog.InfoLogger.Println(reflect.TypeOf(msg))
		tracelog.InfoLogger.Println(msg)

		switch v := msg.(type) {
		case *pgproto3.Query:

			tstmt, err := shgop.Parse(v.String)

			if err != nil {
				tracelog.ErrorLogger.PrintError(err)
			}

			switch stmt := tstmt.(type) {
			case *shgop.Show:
				tracelog.InfoLogger.Print("jifjweoifjwioef %v", stmt.Cmd)

				switch stmt.Cmd {
				case shgoparser.ShowPoolsStr:
					for _, msg := range []pgproto3.BackendMessage{
						&pgproto3.Authentication{Type: pgproto3.AuthTypeOk},
						&pgproto3.RowDescription{Fields: []pgproto3.FieldDescription{
							{
								Name:                 "fortune",
								TableOID:             0,
								TableAttributeNumber: 0,
								DataTypeOID:          25,
								DataTypeSize:         -1,
								TypeModifier:         -1,
								Format:               0,
							},
						},
						},
						&pgproto3.DataRow{Values: [][]byte{[]byte("show pools")}},
						&pgproto3.CommandComplete{CommandTag: "SELECT 1"},
						&pgproto3.ReadyForQuery{},
					} {
						if err := cl.Send(msg); err != nil {
							tracelog.InfoLogger.Print(err)
						}
					}

				case shgoparser.ShowDatabasesStr:
					for _, msg := range []pgproto3.BackendMessage{
						&pgproto3.Authentication{Type: pgproto3.AuthTypeOk},
						&pgproto3.RowDescription{Fields: []pgproto3.FieldDescription{
							{
								Name:                 "fortune",
								TableOID:             0,
								TableAttributeNumber: 0,
								DataTypeOID:          25,
								DataTypeSize:         -1,
								TypeModifier:         -1,
								Format:               0,
							},
						},
						},
						&pgproto3.DataRow{Values: [][]byte{[]byte("show dbs")}},
						&pgproto3.CommandComplete{CommandTag: "SELECT 1"},
						&pgproto3.ReadyForQuery{},
					} {
						if err := cl.Send(msg); err != nil {
							tracelog.InfoLogger.Print(err)
						}
					}
				default:
					tracelog.InfoLogger.Printf("loh %s", stmt.Cmd)

					cl.DefaultReply()
				}

			default:
				tracelog.InfoLogger.Printf("jifjweoifjwioef %v %T", tstmt, tstmt)
			}

			if err := cl.DefaultReply(); err != nil {
				return err
			}
		}
	}

	return nil
}

func (sg *Shgo) RunAdm(listener net.Listener) error {
	for {
		conn, err := listener.Accept()

		util.Fatal(err)
		go sg.servAdm(conn)
	}

	return nil

}
