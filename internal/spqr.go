package internal

import (
	"crypto/tls"
	"net"

	"github.com/jackc/pgproto3"
	"github.com/pg-sharding/spqr/internal/config"
	"github.com/pg-sharding/spqr/internal/r"
	"github.com/pg-sharding/spqr/yacc/spqrparser"
	"github.com/pkg/errors"
	"github.com/wal-g/tracelog"
)

type Spqr struct {
	//TODO add some fiels from spqrconfig
	Cfg    *config.SpqrConfig
	Router *Router
	R      *r.R
}

func NewSpqr(config *config.SpqrConfig) (*Spqr, error) {
	router, err := NewRouter(config.RouterCfg)
	if err != nil {
		return nil, errors.Wrap(err, "NewRouter")
	}

	for _, shard := range config.RouterCfg.SQPRShards {
		if !shard.TLSCfg.ReqSSL {
			continue
		}

		cert, err := tls.LoadX509KeyPair(shard.TLSCfg.CertFile, shard.TLSCfg.KeyFile)
		if err != nil {
			return nil, errors.Wrap(err, "failed to make route failure resp")
		}

		tlscfg := &tls.Config{Certificates: []tls.Certificate{cert}, InsecureSkipVerify: true}
		if err := shard.Init(tlscfg); err != nil {
			return nil, err
		}
	}

	return &Spqr{
		Cfg:    config,
		Router: router,
		R:      r.NewR(),
	}, nil
}

const TXREL = 73

func frontend(rt *r.R, cl *SpqrClient, cmngr ConnManager) error {

	msgs := make([]pgproto3.Query, 0)

	rst := &RelayState{
		ActiveShardName:   "",
		ActiveBackendConn: nil,
		TxActive:          false,
	}

	for {
		msg, err := cl.Receive()
		if err != nil {
			tracelog.ErrorLogger.PrintError(err)
			return err
		}

		switch v := msg.(type) {
		case *pgproto3.Query:

			msgs = append(msgs, pgproto3.Query{
				String: v.String,
			})

			// txactive == 0 || activeSh == nil
			if cmngr.ValidateReRoute(rst) {

				shardName := rt.Route(v.String)

				if shardName == "" {
					if err := cl.DefaultReply(); err != nil {
						return err
					}

					break
				}

				if rst.ActiveShardName != "" {

					if err := cl.Route().Unroute(rst.ActiveShardName, cl); err != nil {
						return err
					}
				}

				rst.ActiveShardName = shardName

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

		default:
			//msgs := append(msgs, msg)
		}
	}
}

func (sg *Spqr) serv(netconn net.Conn) error {

	client, err := sg.Router.PreRoute(netconn)
	if err != nil {
		//tracelog.ErrorLogger.PrintError(err)
		return err
	}

	cmngr, err := InitClConnection(client)
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

	cl := NewClient(netconn)

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

	console := NewConsole()

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

				console.AddShardingColumn(cl, stmt, sg.R)
			case *spqrparser.KeyRange:
				console.AddKeyRange(cl, sg.R, r.KeyRange{From: stmt.From, To: stmt.To, ShardId: stmt.ShardID})
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
