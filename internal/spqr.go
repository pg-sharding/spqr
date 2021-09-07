package internal

import (
	"crypto/tls"
	"net"

	"github.com/pg-sharding/spqr/internal/config"
	"github.com/pg-sharding/spqr/internal/qrouter"
	"github.com/pkg/errors"
	"github.com/wal-g/tracelog"
	"golang.org/x/xerrors"
)

type Spqr struct {
	//TODO add some fiels from spqrconfig
	Cfg *config.SpqrConfig

	Router  Router
	Qrouter qrouter.Qrouter

	ConsoleDB Console

	stchan chan struct{}

	SPIexecuter *Executer
}

const defaultProto = "tcp"

func NewSpqr(cfg *config.SpqrConfig) (*Spqr, error) {

	var qr qrouter.Qrouter
	var err error

	switch cfg.QRouterCfg.Qtype {
	case config.ShardQrouter:
		qr, err = qrouter.NewQrouter()
		if err != nil {
			return nil, err
		}

	case config.LocalQrouter:
		qr = qrouter.NewLocalQrouter(cfg.QRouterCfg.LocalShard)
	default:
		return nil, xerrors.Errorf("unknown qrouter type %v", cfg.QRouterCfg.Qtype)
	}
	var tlscfg *tls.Config
	if cfg.RRouterCfg.TLSCfg.SslMode != config.SSLMODEDISABLE {
		cert, err := tls.LoadX509KeyPair(cfg.RRouterCfg.TLSCfg.CertFile, cfg.RRouterCfg.TLSCfg.KeyFile)
		tracelog.InfoLogger.Printf("loading tls cert file %s, key file %s", cfg.RRouterCfg.TLSCfg.CertFile, cfg.RRouterCfg.TLSCfg.KeyFile)
		if err != nil {
			return nil, errors.Wrap(err, "failed to load frontend tls conf")
		}
		tlscfg = &tls.Config{Certificates: []tls.Certificate{cert}, InsecureSkipVerify: true}
	}

	router, err := NewRouter(cfg.RRouterCfg, tlscfg)

	if err != nil {
		return nil, errors.Wrap(err, "NewRouter")
	}
	tracelog.InfoLogger.Printf("%v", cfg.RRouterCfg.ShardMapping)

	for name, shard := range cfg.RRouterCfg.ShardMapping {
		if shard.TLSCfg.SslMode != config.SSLMODEDISABLE {
			cert, err := tls.LoadX509KeyPair(shard.TLSCfg.CertFile, shard.TLSCfg.KeyFile)
			if err != nil {
				return nil, errors.Wrap(err, "failed to make route failure resp")
			}

			tlscfg := &tls.Config{Certificates: []tls.Certificate{cert}, InsecureSkipVerify: true}

			if err := shard.Init(tlscfg); err != nil {
				return nil, err
			}
		}

		if shard.Hosts[0].Proto == "" {
			shard.Hosts[0].Proto = defaultProto
		}

		tracelog.InfoLogger.FatalOnError(qr.AddShard(name, shard))
	}

	spqr := &Spqr{
		Cfg:     cfg,
		Router:  router,
		Qrouter: qr,
		stchan:  make(chan struct{}),
	}

	spqr.ConsoleDB = NewConsole(tlscfg, spqr.Qrouter, spqr.stchan)

	executer := NewExecuter(cfg.ExecuterCfg)

	_ = executer.SPIexec(spqr.ConsoleDB, NewFakeClient())

	spqr.SPIexecuter = executer

	return spqr, nil
}

func (sg *Spqr) serv(netconn net.Conn) error {

	client, err := sg.Router.PreRoute(netconn)
	if err != nil {
		return err
	}

	tracelog.InfoLogger.Printf("preroute ok")

	cmngr, err := InitClConnection(client)
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
		case conn, _ := <-cChan:

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
	return sg.ConsoleDB.Serve(netconn)
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
