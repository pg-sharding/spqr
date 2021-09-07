package internal

import (
	"crypto/tls"
	"net"

	"github.com/jackc/pgproto3"
	"github.com/pg-sharding/spqr/internal/config"
	"github.com/pg-sharding/spqr/internal/console"
	"github.com/pg-sharding/spqr/internal/qdb"
	"github.com/pg-sharding/spqr/internal/qrouter"
	"github.com/pg-sharding/spqr/internal/rrouter"
	"github.com/pkg/errors"
	"github.com/wal-g/tracelog"
	"golang.org/x/xerrors"
)

type Spqr struct {
	Router  rrouter.Router
	Qrouter qrouter.Qrouter

	ConsoleDB console.Console

	stchan chan struct{}

	SPIexecuter *Executer

	frTLS *tls.Config
}

func NewSpqr(dataFolder string) (*Spqr, error) { // TODO

	var qr qrouter.Qrouter
	var err error

	switch config.Get().QRouterCfg.Qtype {
	case config.ShardQrouter:
		qr, err = qrouter.NewQrouter()
		if err != nil {
			return nil, err
		}

	case config.LocalQrouter:
		qr = qrouter.NewLocalQrouter(config.Get().QRouterCfg.LocalShard)
	default:
		return nil, xerrors.Errorf("unknown qrouter type %v", config.Get().QRouterCfg.Qtype)
	}

	var tlscfg *tls.Config

	if config.Get().RouterConfig.TLSCfg.SslMode != config.SSLMODEDISABLE {
		cert, err := tls.LoadX509KeyPair(config.Get().RouterConfig.TLSCfg.CertFile, config.Get().RouterConfig.TLSCfg.KeyFile)
		tracelog.InfoLogger.Printf("loading tls cert file %s, key file %s", config.Get().RouterConfig.TLSCfg.CertFile, config.Get().RouterConfig.TLSCfg.KeyFile)

		if err != nil {
			return nil, errors.Wrap(err, "failed to load frontend tls conf")
		}
		tlscfg = &tls.Config{Certificates: []tls.Certificate{cert}, InsecureSkipVerify: true}
	}

	router, err := rrouter.NewRouter(tlscfg)

	if err != nil {
		return nil, errors.Wrap(err, "NewRouter")
	}
	tracelog.InfoLogger.Printf("%v", config.Get().RouterConfig.ShardMapping)

	for name, shard := range config.Get().RouterConfig.ShardMapping {
		if shard.TLSCfg.SslMode != config.SSLMODEDISABLE {
			cert, err := tls.LoadX509KeyPair(shard.TLSCfg.CertFile, shard.TLSCfg.KeyFile)
			if err != nil {
				return nil, errors.Wrap(err, "failed to make route failure resp")
			}

			shardTLS := &tls.Config{Certificates: []tls.Certificate{cert}, InsecureSkipVerify: true}

			if err := shard.Init(shardTLS); err != nil {
				return nil, err
			}
		}

		_ = router.AddShard(qdb.ShardKey{
			Name: name,
		})

		tracelog.InfoLogger.FatalOnError(qr.AddShard(name, shard))
	}

	spqr := &Spqr{
		Router:  router,
		Qrouter: qr,
		stchan:  make(chan struct{}),
		frTLS:   tlscfg,
	}

	spqr.ConsoleDB = console.NewConsole(tlscfg, spqr.Qrouter, spqr.stchan)

	executer := NewExecuter(config.Get().ExecuterCfg)

	_ = executer.SPIexec(spqr.ConsoleDB, rrouter.NewFakeClient())

	spqr.SPIexecuter = executer

	return spqr, nil
}

func (sg *Spqr) serv(netconn net.Conn) error {

	client, err := sg.Router.PreRoute(netconn)
	if err != nil {
		return err
	}

	tracelog.InfoLogger.Printf("preroute ok")

	cmngr, err := rrouter.InitClConnection(client)
	if err != nil {
		return err
	}

	return frontend(sg.Qrouter, client, cmngr)
}

func (sg *Spqr) Run(listener net.Listener) error {
	cChan := make(chan net.Conn)

	accept := func(l net.Listener) {
		for {
			c, err := l.Accept()
			if err != nil {
				// handle error (and then for example indicate acceptor is down)
				cChan <- nil
				return
			}
			cChan <- c
		}
	}

	go accept(listener)

	for {

		select {
		case conn := <-cChan:

			go func() {
				if err := sg.serv(conn); err != nil {
					tracelog.ErrorLogger.PrintError(err)
				}
			}()

		case <-sg.stchan:
			_ = sg.Router.Shutdown()
			_ = listener.Close()
		}
	}
}

func (sg *Spqr) servAdm(netconn net.Conn) error {

	cl := rrouter.NewClient(netconn)

	if err := cl.Init(sg.frTLS, config.SSLMODEDISABLE); err != nil {
		return err
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

	return sg.ConsoleDB.Serve(cl)
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
