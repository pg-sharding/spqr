package shgo

import (
	"crypto/tls"
	shhttp "github.com/shgo/src/http"

	//"fmt"
	"net"
	//"reflect"

	"github.com/jackc/pgproto3"
	"github.com/shgo/src/internal/core"
	"github.com/shgo/src/internal/r"
	"github.com/shgo/src/util"
	"github.com/shgo/yacc/shgoparser"
	shgop "github.com/shgo/yacc/shgoparser"
	//"github.com/wal-g/tracelog"
	"github.com/wal-g/tracelog"
)

type GlobConfig struct {
	Addr    string `json:"addr" toml:"addr" yaml:"addr"`
	ADMAddr string `json:"adm_addr" toml:"adm_addr" yaml:"adm_addr"`
	PROTO   string `json:"proto" toml:"proto" yaml:"proto"`

	Tlscfg core.TLSConfig `json:"tls_cfg" toml:"tls_cfg" yaml:"tls_cfg"`

	RouterCfg core.RouterConfig `json:"router" toml:"router" yaml:"router"`

	HttpConfig shhttp.HttpConf `json:"http_conf" toml:"http_conf" yaml:"http_conf"`
}

type Shgo struct {
	Cfg    GlobConfig
	Router *core.Router
	R      r.R
}

func NewShgo(Cfg GlobConfig, Router *core.Router, R r.R) (*Shgo, error) {

	for _, be := range Cfg.RouterCfg.BackendRules {
		if !be.SHStorage.ReqSSL {
			continue
		}

		cert, err := tls.LoadX509KeyPair(be.TLSCfg.TLSSertPath, be.TLSCfg.ServPath)
		if err != nil {
			tracelog.InfoLogger.Printf("failed to init backend rule tls conf %w", err)
			return nil, err
		}

		tlscfg := &tls.Config{Certificates: []tls.Certificate{cert}, InsecureSkipVerify: true}

		if err := be.SHStorage.Init(tlscfg); err != nil {
			return nil, err
		}
	}

	return &Shgo{
		Cfg:    Cfg,
		Router: Router,
		R:      R,
	}, nil
}

const TXREL = 73

func frontend(rt r.R, cl *core.ShClient, cmngr core.ConnManager) error {

	//for k, v := range cl.StartupMessage().Parameters {
	//tracelog.InfoLogger.Println("log loh %v %v", k, v)
	//}

	msgs := make([]pgproto3.Query, 0)

	rst := &core.RelayState{
		ActiveShardIndx: r.NOSHARD,
		ActiveShardConn: nil,
		TxActive:        false,
	}

	for {
		//tracelog.InfoLogger.Println("round")
		msg, err := cl.Receive()
		if err != nil {
			util.Fatal(err)
			return err
		}
		//tracelog.InfoLogger.Println(reflect.TypeOf(msg))
		//tracelog.InfoLogger.Println(msg)

		switch v := msg.(type) {
		case *pgproto3.Query:

			msgs = append(msgs, pgproto3.Query{
				String: v.String,
			})

			// txactive == 0 || activeSh == r.NOSHARD
			if cmngr.ValidateReRoute(rst) {

				//fmt.Printf("rerouting\n")
				shindx := rt.Route(v.String)

				//tracelog.InfoLogger.Printf("parsed shindx %d", shindx)

				if shindx == r.NOSHARD {
					if err := cl.DefaultReply(); err != nil {
						return err
					}

					break
				}

				//if shindx == r.NOSHARD {
				//	break
				//}

				//fmt.Printf("get conn to %d\n", shindx)

				if rst.ActiveShardIndx != r.NOSHARD {
					//fmt.Printf("unrouted prev shard conn %d\n", rst.ActiveShardIndx)

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

			//tracelog.InfoLogger.Printf(" relay state is %v\n", rst)

		default:
			//msgs := append(msgs, msg)
		}

		////tracelog.InfoLogger.Printf("crnt msgs buff %+v\n", msgs)
	}

	return nil
}

func (sg *Shgo) serv(netconn net.Conn) error {

	client, err := sg.Router.PreRoute(netconn)
	if err != nil {
		//tracelog.ErrorLogger.PrintError(err)
		return err
	}

	cmngr, err := core.InitClConnection(client)
	if err != nil {
		return err
	}

	return frontend(sg.R, client, cmngr)
}

func (sg *Shgo) Run(listener net.Listener) error {

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

	var cfg *tls.Config = nil

	cert, err := tls.LoadX509KeyPair(sg.Cfg.RouterCfg.TLSCfg.TLSSertPath, sg.Cfg.RouterCfg.TLSCfg.ServPath)
	if err != nil {
		return err
	}

	cfg = &tls.Config{Certificates: []tls.Certificate{cert}, InsecureSkipVerify: true}

	if err := cl.Init(cfg, sg.Cfg.RouterCfg.ReqSSL); err != nil {
		return err
	}

	//_, err := core.InitClConnection(cl)
	//if err != nil {
	//	return err
	//}

	for _, msg := range []pgproto3.BackendMessage{
		&pgproto3.Authentication{Type: pgproto3.AuthTypeOk},
		&pgproto3.ParameterStatus{Name: "integer_datetimes", Value: "on"},
		&pgproto3.ParameterStatus{Name: "server_version", Value: "console"},
		&pgproto3.ReadyForQuery{},
	} {
		if err :=
			cl.Send(msg); err != nil {

			//tracelog.InfoLogger.Printf("server starsup resp failed %v", msg)

			return err
		}
	}

	console := core.NewConsole()

	for {

		msg, err := cl.Receive()
		if err != nil {
			util.Fatal(err)
			return err
		}
		//tracelog.InfoLogger.Println(reflect.TypeOf(msg))
		//tracelog.InfoLogger.Println(msg)

		switch v := msg.(type) {
		case *pgproto3.Query:

			tstmt, err := shgop.Parse(v.String)

			if err != nil {
				tracelog.ErrorLogger.PrintError(err)
			}

			switch stmt := tstmt.(type) {
			case *shgop.Show:
				//tracelog.InfoLogger.Print("jifjweoifjwioef %v", stmt.Cmd)

				switch stmt.Cmd {
				case shgoparser.ShowPoolsStr:
					console.Pools(cl)
				case shgoparser.ShowDatabasesStr:
					console.Databases(cl)
				default:
					//tracelog.InfoLogger.Printf("loh %s", stmt.Cmd)

					_ = cl.DefaultReply()
				}
			case *shgoparser.ShardingColumn:

				console.AddShardingColumn(cl, stmt, &sg.R)
			case *shgoparser.KeyRange:
				console.AddKeyRange(cl, &sg.R, r.KeyRange{From: stmt.From, To: stmt.To, ShardId: stmt.ShardID})
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
