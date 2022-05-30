package pkg

import (
	"context"
	"crypto/tls"
	"net"

	"github.com/pg-sharding/spqr/pkg/spqrlog"

	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/router/pkg/client"
	"github.com/pg-sharding/spqr/router/pkg/console"
	"github.com/pg-sharding/spqr/router/pkg/qrouter"
	"github.com/pg-sharding/spqr/router/pkg/rrouter"
	"github.com/pkg/errors"
	"golang.org/x/xerrors"
)

type Router interface {
	Addr() string
	ID() string
}

type RouterImpl struct {
	Rrouter    rrouter.RequestRouter
	Qrouter    qrouter.QueryRouter
	AdmConsole console.Console

	stchan chan struct{}
	addr   string
	frTLS  *tls.Config
}

func (r *RouterImpl) ID() string {
	return "noid"
}

func (r *RouterImpl) Addr() string {
	return r.addr
}

var _ Router = &RouterImpl{}

func NewRouter(ctx context.Context) (*RouterImpl, error) {

	// init tls
	if err := initShards(config.RouterConfig().RulesConfig); err != nil {
		spqrlog.Logger.PrintError(err)
	}
	// qrouter init
	qtype := config.QrouterType(config.RouterConfig().QRouterCfg.Qtype)
	spqrlog.Logger.Printf(spqrlog.DEBUG1, "creating QueryRouter with type %s", qtype)
	rules := config.RouterConfig().RulesConfig

	qr, err := qrouter.NewQrouter(qtype, rules)
	if err != nil {
		return nil, err
	}

	// frontend
	frTLS, err := rules.TLSCfg.Init()
	if err != nil {
		return nil, errors.Wrap(err, "init frontend TLS")
	}

	// request router
	rr, err := rrouter.NewRouter(frTLS)
	if err != nil {
		return nil, errors.Wrap(err, "NewRouter")
	}

	stchan := make(chan struct{})
	localConsole, err := console.NewConsole(frTLS, qr, rr, stchan)
	if err != nil {
		spqrlog.Logger.Printf(spqrlog.ERROR, "failed to initialize router: %v", err)
		return nil, err
	}

	for _, fname := range []string{
		config.RouterConfig().InitSQLPath,
		config.RouterConfig().AutoConf,
	} {
		if len(fname) == 0 {
			continue
		}
		queries, err := localConsole.Qlog().Recover(ctx, fname)
		if err != nil {
			spqrlog.Logger.Printf(spqrlog.ERROR, "failed to initialize router: %v", err)
			return nil, err
		}

		spqrlog.Logger.Printf(spqrlog.INFO, "executing init sql cmd %s", queries)
		if err := localConsole.ProcessQuery(context.TODO(), queries, client.NewFakeClient()); err != nil {
			spqrlog.Logger.PrintError(err)
		}

		spqrlog.Logger.Printf(spqrlog.INFO, "Successfully init %d queries from %s", len(queries), fname)
	}

	return &RouterImpl{
		Rrouter:    rr,
		Qrouter:    qr,
		AdmConsole: localConsole,
		stchan:     stchan,
		frTLS:      frTLS,
	}, nil
}

func initShards(rules config.RulesCfg) error {
	for _, shard := range rules.ShardMapping {
		switch shard.ShType {
		case config.WorldShard:
			// nothing
		case config.DataShard:
			// datashard assumed by default
			fallthrough
		default:
			if err := shard.InitShardTLS(); err != nil {
				return err
			}
		}
	}

	return nil
}

func (r *RouterImpl) serv(netconn net.Conn) error {
	psqlclient, err := r.Rrouter.PreRoute(netconn)
	if err != nil {
		_ = netconn.Close()
		return err
	}

	spqrlog.Logger.Printf(spqrlog.LOG, "pre route ok")

	cmngr, err := rrouter.MatchConnectionPooler(psqlclient)
	if err != nil {
		return err
	}

	return Frontend(r.Qrouter, psqlclient, cmngr)
}

func (r *RouterImpl) Run(ctx context.Context, listener net.Listener) error {
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
					spqrlog.Logger.PrintError(err)
				}
			}()

		case <-r.stchan:
			_ = r.Rrouter.Shutdown()
			_ = listener.Close()
		case <-ctx.Done():
			_ = r.Rrouter.Shutdown()
			_ = listener.Close()
			spqrlog.Logger.Printf(spqrlog.LOG, "psql server done")
			return nil
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
		case <-ctx.Done():
			_ = listener.Close()
			spqrlog.Logger.Printf(spqrlog.LOG, "admin sever done")
			return nil
		case conn := <-cChan:
			go func() {
				if err := r.servAdm(ctx, conn); err != nil {
					spqrlog.Logger.PrintError(err)
				}
			}()
		}
	}
}
