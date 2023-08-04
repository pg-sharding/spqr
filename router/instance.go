package app

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"os"

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

	stchan     chan struct{}
	addr       string
	frTLS      *tls.Config
	WithJaeger bool
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
	/* TODO: fix by adding configurable setting */
	skipInitSQL := false
	if _, err := os.Stat(rcfg.MemqdbBackupPath); err == nil {
		skipInitSQL = true
	}

	qdb, err := qdb.RestoreQDB(rcfg.MemqdbBackupPath)
	if err != nil {
		return nil, err
	}

	lc := local.NewLocalCoordinator(qdb)

	// qrouter init
	qtype := config.RouterMode(rcfg.RouterMode)
	spqrlog.Zero.Debug().
		Type("qtype", qtype).
		Msg("creating QueryRouter with type")

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
		spqrlog.Zero.Error().Err(err).Msg("failed to initialize router")
		return nil, err
	}

	if skipInitSQL {
		for _, fname := range []string{
			rcfg.InitSQL,
		} {
			if len(fname) == 0 {
				continue
			}
			queries, err := localConsole.Qlog().Recover(ctx, fname)
			if err != nil {
				spqrlog.Zero.Error().Err(err).Msg("failed to initialize router")
				return nil, err
			}

			spqrlog.Zero.Info().Msg("executing init sql")
			for _, query := range queries {
				spqrlog.Zero.Info().Str("query", query).Msg("")
				if err := localConsole.ProcessQuery(ctx, query, client.NewFakeClient()); err != nil {
					spqrlog.Zero.Error().Err(err).Msg("")
				}
			}

			spqrlog.Zero.Info().
				Int("count", len(queries)).
				Str("filename", fname).
				Msg("successfully init queries from file")
		}
	}

	qr.Initialize()

	return &InstanceImpl{
		RuleRouter: rr,
		Qrouter:    qr,
		AdmConsole: localConsole,
		Mgr:        lc,
		stchan:     stchan,
		frTLS:      frTLS,
		WithJaeger: rcfg.WithJaeger,
	}, nil
}

func (r *InstanceImpl) serv(netconn net.Conn, admin_console bool) error {
	routerClient, err := r.RuleRouter.PreRoute(netconn, admin_console)
	if err != nil {
		_ = netconn.Close()
		return err
	}

	defer netconn.Close()

	/* If cancel, procced and return, close connection */
	if routerClient.CancelMsg() != nil {
		return r.RuleRouter.CancelClient(routerClient.CancelMsg())
	}

	if admin_console || routerClient.DB() == "spqr-console" {
		return r.AdmConsole.Serve(context.Background(), routerClient)
	}

	spqrlog.Zero.Debug().
		Uint("client", spqrlog.GetPointer(routerClient)).
		Msg("prerouting phase succeeded")

	cmngr, err := rulerouter.MatchConnectionPooler(routerClient, r.RuleRouter.Config())
	if err != nil {
		return err
	}

	r.RuleRouter.AddClient(routerClient)
	defer r.RuleRouter.ReleaseClient(routerClient)
	defer func() {
		_, _ = routerClient.Route().ReleaseClient(routerClient.ID())
	}()

	return Frontend(r.Qrouter, routerClient, cmngr, r.RuleRouter.Config())
}

func (r *InstanceImpl) Run(ctx context.Context, listener net.Listener) error {
	if r.WithJaeger {
		closer, err := r.initJaegerTracer(r.RuleRouter.Config())
		if err != nil {
			return fmt.Errorf("could not initialize jaeger tracer: %s", err)
		}
		defer func() { _ = closer.Close() }()
	}

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
					if err := r.serv(conn, false); err != nil {
						spqrlog.Zero.Error().Err(err).Msg("error serving client")
					}
				}()
			}
		case <-r.stchan:
			_ = r.RuleRouter.Shutdown()
			_ = listener.Close()
		case <-ctx.Done():
			_ = r.RuleRouter.Shutdown()
			_ = listener.Close()
			spqrlog.Zero.Info().Msg("psql server done")
			return nil
		}
	}
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
			spqrlog.Zero.Info().Msg("admin server done")
			return nil
		case conn := <-cChan:
			go func() {
				if err := r.serv(conn, true); err != nil {
					spqrlog.Zero.Error().Err(err).Msg("")
				}
			}()
		}
	}
}
