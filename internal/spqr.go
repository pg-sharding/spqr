package spqr

import (
	"crypto/tls"
	"net"

	"github.com/jackc/pgproto3"
	shhttp "github.com/pg-sharding/spqr/http"
	"github.com/pg-sharding/spqr/internal/core"
	"github.com/pg-sharding/spqr/internal/r"
	"github.com/pg-sharding/spqr/yacc/spqrparser"
	"github.com/pkg/errors"
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

type Spqr struct {
	Cfg    GlobConfig
	Router *core.Router
	R      r.R
}

func NewSpqr(Cfg GlobConfig, Router *core.Router, R r.R) (*Spqr, error) {

	for _, be := range Cfg.RouterCfg.BackendRules {
		if !be.SHStorage.ReqSSL {
			continue
		}

		cert, err := tls.LoadX509KeyPair(be.TLSCfg.CertFile, be.TLSCfg.KeyFile)
		if err != nil {
			return nil, errors.Wrap(err, "failed to make route failure resp")
		}

		tlscfg := &tls.Config{Certificates: []tls.Certificate{cert}, InsecureSkipVerify: true}

		if err := be.SHStorage.Init(tlscfg); err != nil {
			return nil, err
		}
	}

	return &Spqr{
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
			tracelog.ErrorLogger.PrintError(err)
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

				//tracelog.InfoLogger.Println("rerouting\n")
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

				//tracelog.InfoLogger.Println("get conn to %d\n", shindx)

				if rst.ActiveShardIndx != r.NOSHARD {
					//tracelog.InfoLogger.Println("unrouted prev shard conn %d\n", rst.ActiveShardIndx)

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

			msgs = make([]pgproto3.Query, 0)

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
}

func (sg *Spqr) serv(netconn net.Conn) error {

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

func (sg *Spqr) Run(listener net.Listener) error {

	for {
		conn, err := listener.Accept()

		tracelog.ErrorLogger.PrintError(err)
		go func() {
			if err := sg.serv(conn); err != nil {
				tracelog.ErrorLogger.PrintError(err)
			}
		}()
	}
}

func (sg *Spqr) servAdm(netconn net.Conn) {

	cl := core.NewClient(netconn)

	var cfg *tls.Config = nil

	cert, err := tls.LoadX509KeyPair(sg.Cfg.RouterCfg.TLSCfg.CertFile, sg.Cfg.RouterCfg.TLSCfg.KeyFile)
	if err != nil {
		tracelog.ErrorLogger.Fatal(err)
	}

	cfg = &tls.Config{Certificates: []tls.Certificate{cert}, InsecureSkipVerify: true}

	if err := cl.Init(cfg, sg.Cfg.RouterCfg.ReqSSL); err != nil {
		tracelog.ErrorLogger.Fatal(err)
	}

	for _, msg := range []pgproto3.BackendMessage{
		&pgproto3.Authentication{Type: pgproto3.AuthTypeOk},
		&pgproto3.ParameterStatus{Name: "integer_datetimes", Value: "on"},
		&pgproto3.ParameterStatus{Name: "server_version", Value: "console"},
		&pgproto3.ReadyForQuery{},
	} {
		if err := cl.Send(msg); err != nil {
		    tracelog.ErrorLogger.Fatal(err)
		}
	}

	console := core.NewConsole()

	for {
		msg, err := cl.Receive()
		tracelog.ErrorLogger.FatalOnError(err)

		switch v := msg.(type) {
		case *pgproto3.Query:

			tstmt, err := spqrparser.Parse(v.String)
			tracelog.ErrorLogger.FatalOnError(err)

			switch stmt := tstmt.(type) {
			case *spqrparser.Show:

				switch stmt.Cmd {
				case spqrparser.ShowPoolsStr: // TODO serv errors
					console.Pools(cl)
				case spqrparser.ShowDatabasesStr:
					console.Databases(cl)
				default:
					tracelog.InfoLogger.Printf("Unknown default %s", stmt.Cmd)

					_ = cl.DefaultReply()
				}
			case *spqrparser.ShardingColumn:

				console.AddShardingColumn(cl, stmt, &sg.R)
			case *spqrparser.KeyRange:
				console.AddKeyRange(cl, &sg.R, r.KeyRange{From: stmt.From, To: stmt.To, ShardId: stmt.ShardID})
			default:
				tracelog.InfoLogger.Printf("jifjweoifjwioef %v %T", tstmt, tstmt)
			}

			if err := cl.DefaultReply(); err != nil {
		        tracelog.ErrorLogger.Fatal(err)
			}
		}
	}
}

func (sg *Spqr) RunAdm(listener net.Listener) error {
	for {
		conn, err := listener.Accept()
		if err != nil {
			return errors.Wrap(err, "RunAdm failed")
		}
		go sg.servAdm(conn)
	}
}
