package internal

import (
	"crypto/tls"
	"net"

	"github.com/jackc/pgproto3"
	"github.com/pg-sharding/spqr/internal/config"
	"github.com/pg-sharding/spqr/internal/qrouter"
	"github.com/pkg/errors"
	"github.com/wal-g/tracelog"
)

type Spqr struct {
	//TODO add some fiels from spqrconfig
	Cfg *config.SpqrConfig

	Router  *Router
	Qrouter *qrouter.QrouterImpl

	SPIexecuter *Executer
}

func NewSpqr(config *config.SpqrConfig) (*Spqr, error) {

	qrouter := qrouter.NewR()

	router, err := NewRouter(config.RouterCfg, qrouter)
	if err != nil {
		return nil, errors.Wrap(err, "NewRouter")
	}
	tracelog.InfoLogger.Printf("%v", config.RouterCfg.ShardMapping)

	for name, shard := range config.RouterCfg.ShardMapping {
		if shard.TLSCfg.ReqSSL {
			cert, err := tls.LoadX509KeyPair(shard.TLSCfg.CertFile, shard.TLSCfg.KeyFile)
			if err != nil {
				return nil, errors.Wrap(err, "failed to make route failure resp")
			}
			tlscfg := &tls.Config{Certificates: []tls.Certificate{cert}, InsecureSkipVerify: true}
			tracelog.InfoLogger.Printf("initialising shard tls config for %s", name)

			if err := shard.Init(tlscfg); err != nil {
				return nil, err
			}
		}
		tracelog.InfoLogger.FatalOnError(qrouter.AddShard(name, &shard))
	}

	executer := NewExecuter(config.ExecuterCfg)

	executer.SPIexec(router.ConsoleDB, NewFakeClient())

	return &Spqr{
		Cfg:         config,
		Router:      router,
		Qrouter:     qrouter,
		SPIexecuter: executer,
	}, nil
}

const TXREL = 73

func frontend(rt *qrouter.QrouterImpl, cl Client, cmngr ConnManager) error {

	tracelog.InfoLogger.Printf("process frontend for user %s %s", cl.Usr(), cl.DB())

	msgs := make([]pgproto3.Query, 0)

	rst := &RelayState{
		ActiveShard:       nil,
		ActiveBackendConn: nil,
		TxActive:          false,
	}

	for {
		msg, err := cl.Receive()
		if err != nil {
			tracelog.ErrorLogger.Printf("failed to recieve msg %w", err)
			return err
		}

		tracelog.InfoLogger.Printf("recieved msg %v", msg)

		switch v := msg.(type) {
		case *pgproto3.Query:

			msgs = append(msgs, pgproto3.Query{
				String: v.String,
			})

			// txactive == 0 || activeSh == nil
			if cmngr.ValidateReRoute(rst) {

				shardName := rt.Route(v.String)
				shard := NewShard(shardName, rt.ShardCfgs[shardName])

				if shardName == "" {
					if err := cl.ReplyErr(errors.New("failed to match shard")); err != nil {
						return err
					}

					return nil
				} else {
					tracelog.InfoLogger.Printf("parsed shard name %s", shardName)
				}

				if rst.ActiveShard != nil {

					if err := cmngr.UnRouteCB(cl, rst); err != nil {
						return err
					}
				}

				rst.ActiveShard = shard

				if err := cmngr.RouteCB(cl, rst); err != nil {
					return cmngr.UnRouteWithError(cl, rst, err)
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
		return err
	}

	cmngr, err := InitClConnection(client)
	if err != nil {
		return err
	}

	return frontend(sg.Qrouter, client, cmngr)
}

func (sg *Spqr) Run(listener net.Listener) error {
	for {
		conn, _ := listener.Accept()

		go func() {
			if err := sg.serv(conn); err != nil {
				tracelog.ErrorLogger.PrintError(err)
			}
		}()
	}
}

func (sg *Spqr) servAdm(netconn net.Conn) error {
	return sg.Router.ServeConsole(netconn)
}

func (sg *Spqr) RunAdm(listener net.Listener) error {
	for {
		conn, err := listener.Accept()
		if err != nil {
			return errors.Wrap(err, "RunAdm failed")
		}
		go func() {
			if err := sg.servAdm(conn); err != nil {
				tracelog.ErrorLogger.PrintError(err)
			}
		}()
	}
}
