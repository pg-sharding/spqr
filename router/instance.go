package app

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"

	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/coord/local"
	"github.com/pg-sharding/spqr/pkg/meta"
	"github.com/pg-sharding/spqr/pkg/spqrlog"
	"github.com/pg-sharding/spqr/qdb"
	"github.com/pg-sharding/spqr/router/client"
	"github.com/pg-sharding/spqr/router/console"
	"github.com/pg-sharding/spqr/router/qrouter"
	"github.com/pg-sharding/spqr/router/rulerouter"
)

type Router interface {
	Addr() string
	ID() string
}

type InstanceImpl struct {
	RuleRouter rulerouter.RuleRouter
	Qrouter    qrouter.QueryRouter
	AdmConsole console.Console
	Mgr        meta.EntityMgr

	stchan chan struct{}
	addr   string
	frTLS  *tls.Config
}

func (r *InstanceImpl) ID() string {
	return "noid"
}

func (r *InstanceImpl) Addr() string {
	return r.addr
}

func (r *InstanceImpl) Initialized() bool {
	return r.Qrouter.Initialized()
}

var _ Router = &InstanceImpl{}

func NewRouter(ctx context.Context, rcfg *config.Router) (*InstanceImpl, error) {
	// Logger
	if err := spqrlog.UpdateDefaultLogLevel(rcfg.LogLevel); err != nil {
		return nil, err
	}

	/* TODO: fix by adding configurable setting */
	qdb, _ := qdb.NewMemQDB()

	lc := local.NewLocalCoordinator(qdb)

	// qrouter init
	qtype := config.RouterMode(rcfg.RouterMode)
	spqrlog.Logger.Printf(spqrlog.DEBUG1, "creating QueryRouter with type %s", qtype)

	qr, err := qrouter.NewQrouter(qtype, rcfg.ShardMapping, lc, &rcfg.Qr)
	if err != nil {
		return nil, err
	}

	// frontend
	frTLS, err := rcfg.FrontendTLS.Init(rcfg.Host)
	if err != nil {
		return nil, fmt.Errorf("init frontend TLS: %w", err)
	}

	// request router
	rr := rulerouter.NewRouter(frTLS, rcfg)

	stchan := make(chan struct{})
	localConsole, err := console.NewConsole(frTLS, lc, rr, stchan)
	if err != nil {
		spqrlog.Logger.Printf(spqrlog.ERROR, "failed to initialize router: %v", err)
		return nil, err
	}

	if !rcfg.UnderCoordinator {
		for _, fname := range []string{
			rcfg.InitSQL,
			rcfg.AutoConf,
		} {
			if len(fname) == 0 {
				continue
			}
			queries, err := localConsole.Qlog().Recover(ctx, fname)
			if err != nil {
				spqrlog.Logger.Printf(spqrlog.ERROR, "failed to initialize router: %v", err)
				return nil, err
			}

			spqrlog.Logger.Printf(spqrlog.INFO, "executing init sql")
			for _, query := range queries {
				spqrlog.Logger.Printf(spqrlog.INFO, "query: %s", query)
				if err := localConsole.ProcessQuery(ctx, query, client.NewFakeClient()); err != nil {
					spqrlog.Logger.PrintError(err)
					return nil, err
				}
			}

			spqrlog.Logger.Printf(spqrlog.INFO, "Successfully init %d queries from %s", len(queries), fname)
		}

		qr.Initialize()
	}

	return &InstanceImpl{
		RuleRouter: rr,
		Qrouter:    qr,
		AdmConsole: localConsole,
		Mgr:        lc,
		stchan:     stchan,
		frTLS:      frTLS,
	}, nil
}

func (r *InstanceImpl) serv(netconn net.Conn) error {
	routerClient, err := r.RuleRouter.PreRoute(netconn)
	if err != nil {
		_ = netconn.Close()
		return err
	}

	defer netconn.Close()

	if routerClient.DB() == "spqr-console" {
		return r.AdmConsole.Serve(context.Background(), routerClient)
	}

	if routerClient.CancelMsg() != nil {
		return r.RuleRouter.CancelClient(routerClient.CancelMsg())
	}

	spqrlog.Logger.Printf(spqrlog.DEBUG2, "client %p: prerouting phase succeeded", routerClient)

	cmngr, err := rulerouter.MatchConnectionPooler(routerClient, r.RuleRouter.Config())
	if err != nil {
		return err
	}

	r.RuleRouter.AddClient(routerClient)
	defer r.RuleRouter.ReleaseClient(routerClient)

	return Frontend(r.Qrouter, routerClient, cmngr, r.RuleRouter.Config())
}

func (r *InstanceImpl) Run(ctx context.Context, listener net.Listener) error {
	closer, err := r.initJaegerTracer(r.RuleRouter.Config())
	if err != nil {
		return fmt.Errorf("could not initialize jaeger tracer: %s", err.Error())
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
			if !r.Initialized() {
				_ = conn.Close()
			} else {
				go func() {
					if err := r.serv(conn); err != nil {
						spqrlog.Logger.PrintError(err)
					}
				}()
			}
		case <-r.stchan:
			_ = r.RuleRouter.Shutdown()
			_ = listener.Close()
		case <-ctx.Done():
			_ = r.RuleRouter.Shutdown()
			_ = listener.Close()
			spqrlog.Logger.Printf(spqrlog.LOG, "psql server done")
			return nil
		}
	}
}

func (r *InstanceImpl) servAdm(ctx context.Context, conn net.Conn) error {
	cl, err := r.RuleRouter.PreRouteAdm(conn)
	if err != nil {
		return err
	}

	return r.AdmConsole.Serve(ctx, cl)
}

func (r *InstanceImpl) RunAdm(ctx context.Context, listener net.Listener) error {
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
