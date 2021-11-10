package pkg

import (
	"context"
	"crypto/tls"
	"net"

	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/models/datashards"
	"github.com/pg-sharding/spqr/qdb"
	"github.com/pg-sharding/spqr/router/pkg/client"
	"github.com/pg-sharding/spqr/router/pkg/console"
	"github.com/pg-sharding/spqr/router/pkg/qrouter"
	"github.com/pg-sharding/spqr/router/pkg/rrouter"
	"github.com/pkg/errors"
	"github.com/wal-g/tracelog"
	"golang.org/x/xerrors"
)

type Router interface {
	Addr() string
	ID() string
}

type RouterImpl struct {
	Rrouter rrouter.RequestRouter
	Qrouter qrouter.QueryRouter

	AdmConsole console.Console

	SPIexecuter *Executer
	stchan      chan struct{}
	addr        string
	frTLS       *tls.Config
}

func (r *RouterImpl) ID() string {
	return "noid"
}

func (r *RouterImpl) Addr() string {
	return r.addr
}

var _ Router = &RouterImpl{}

func NewRouter(ctx context.Context) (*RouterImpl, error) {

	// qrouter init
	qtype := config.QrouterType(config.RouterConfig().QRouterCfg.Qtype)
	tracelog.InfoLogger.Printf("create QueryRouter with type %s", qtype)

	qr, err := qrouter.NewQrouter(qtype)
	if err != nil {
		return nil, err
	}

	// frontend
	frTlsCfg := config.RouterConfig().RouterConfig.TLSCfg
	frTLS, err := config.InitTLS(frTlsCfg.SslMode, frTlsCfg.CertFile, frTlsCfg.KeyFile)
	if err != nil {
		return nil, errors.Wrap(err, "init frontend TLS")
	}

	// request router
	rr, err := rrouter.NewRouter(frTLS)
	if err != nil {
		return nil, errors.Wrap(err, "NewRouter")
	}

	if err := initShards(ctx, rr, qr); err != nil {
		tracelog.InfoLogger.PrintError(err)
	}

	stchan := make(chan struct{})
	localConsole, err := console.NewConsole(frTLS, qr, rr, stchan)
	if err != nil {
		tracelog.ErrorLogger.PrintError(xerrors.Errorf("failed to initialize router: %w", err))
		return nil, err
	}

	executer := NewExecuter(config.RouterConfig().ExecuterCfg)

	for _, fname := range []string{
		config.RouterConfig().InitSQL,
		config.RouterConfig().AutoConf,
	} {
		queries, err := localConsole.Qlog.Recover(ctx, fname)
		if err != nil {
			tracelog.ErrorLogger.PrintError(xerrors.Errorf("failed to initialize router: %w", err))
			return nil, err
		}

		if err := executer.SPIexec(context.TODO(), localConsole, client.NewFakeClient(), queries); err != nil {
			tracelog.ErrorLogger.PrintError(err)
		}

		tracelog.InfoLogger.Printf("Successfully init %d queries from %s", len(queries), fname)
	}

	return &RouterImpl{
		Rrouter:     rr,
		Qrouter:     qr,
		AdmConsole:  localConsole,
		SPIexecuter: executer,
		stchan:      stchan,
		frTLS:       frTLS,
	}, nil
}

func initShards(ctx context.Context, rr rrouter.RequestRouter, qr qrouter.QueryRouter) error {

	// data shards, world datashard and sharding rules
	for name, shard := range config.RouterConfig().RouterConfig.ShardMapping {

		switch shard.ShType {
		case config.WorldShard:

			if err := rr.AddWorldShard(qdb.ShardKey{Name: name}); err != nil {
				return err
			}
			if err := qr.AddWorldShard(name, shard); err != nil {
				return err
			}

		case config.DataShard:
			// data datashard assumed by default
			fallthrough
		default:

			if err := shard.InitShardTLS(); err != nil {
				return err
			}

			if err := rr.AddDataShard(qdb.ShardKey{Name: name}); err != nil {
				return err
			}
			if err := qr.AddDataShard(ctx, datashards.NewDataShard(name, shard)); err != nil {
				return err
			}
		}
	}

	return nil
}

func (r *RouterImpl) serv(netconn net.Conn) error {

	psqlclient, err := r.Rrouter.PreRoute(netconn)
	if err != nil {
		return err
	}

	tracelog.InfoLogger.Printf("preroute ok")

	cmngr, err := rrouter.MatchConnectionPooler(psqlclient)
	if err != nil {
		return err
	}

	return Frontend(r.Qrouter, psqlclient, cmngr)
}

func (r *RouterImpl) Run(listener net.Listener) error {
	closer, err := r.initJaegerTracer()
	if err != nil {
		return xerrors.Errorf("could not initialize jaeger tracer: %s", err.Error())
	}
	defer func() { _ = closer.Close() }()

	cChan := make(chan net.Conn)

	accept := func(l net.Listener, cChan chan net.Conn) {
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

	go accept(listener, cChan)

	for {
		select {
		case conn := <-cChan:

			go func() {
				if err := r.serv(conn); err != nil {
					tracelog.ErrorLogger.PrintError(err)
				}
			}()

		case <-r.stchan:
			_ = r.Rrouter.Shutdown()
			_ = listener.Close()
		}
	}
}

func (r *RouterImpl) servAdm(ctx context.Context, conn net.Conn) error {
	cl := client.NewPsqlClient(conn)

	if err := cl.Init(r.frTLS, config.SSLMODEDISABLE); err != nil {
		return err
	}

	return r.AdmConsole.Serve(ctx, cl)
}

func (r *RouterImpl) RunAdm(ctx context.Context, listener net.Listener) error {
	for {
		conn, err := listener.Accept()
		if err != nil {
			return errors.Wrap(err, "RunAdm failed")
		}
		go func() {
			if err := r.servAdm(ctx, conn); err != nil {
				tracelog.ErrorLogger.PrintError(err)
			}
		}()
	}
}
