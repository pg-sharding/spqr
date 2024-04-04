package instance

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"time"

	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/coord/local"
	"github.com/pg-sharding/spqr/pkg/meta"
	"github.com/pg-sharding/spqr/pkg/spqrlog"
	"github.com/pg-sharding/spqr/pkg/workloadlog"
	"github.com/pg-sharding/spqr/qdb"
	"github.com/pg-sharding/spqr/router/console"
	"github.com/pg-sharding/spqr/router/frontend"
	"github.com/pg-sharding/spqr/router/poolmgr"
	"github.com/pg-sharding/spqr/router/port"
	"github.com/pg-sharding/spqr/router/qrouter"
	"github.com/pg-sharding/spqr/router/rulerouter"
	sdnotifier "github.com/pg-sharding/spqr/router/sdnotifier"
)

type RouterInstance interface {
	Addr() string
	ID() string
	Initialized() bool
	Initialize() bool

	Console() console.Console
	Config() *config.Router
}

type InstanceImpl struct {
	RuleRouter rulerouter.RuleRouter
	Qrouter    qrouter.QueryRouter
	AdmConsole console.Console
	Mgr        meta.EntityMgr
	Writer     workloadlog.WorkloadLog

	stchan chan struct{}
	addr   string
	frTLS  *tls.Config
	cfg    *config.Router

	notifier *sdnotifier.Notifier
}

// Console implements RouterInstance.
func (r *InstanceImpl) Console() console.Console {
	return r.AdmConsole
}

func (r *InstanceImpl) Config() *config.Router {
	return r.cfg
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

func (r *InstanceImpl) Initialize() bool {
	return r.Qrouter.Initialize()
}

var _ RouterInstance = &InstanceImpl{}

func NewRouter(ctx context.Context, rcfg *config.Router, ns string, persist bool) (*InstanceImpl, error) {
	var db *qdb.MemQDB
	var err error

	if persist {
		db, err = qdb.RestoreQDB(rcfg.MemqdbBackupPath)
		if err != nil {
			return nil, err
		}
	} else {
		db, err = qdb.NewMemQDB("")
		if err != nil {
			return nil, err
		}
	}

	lc := local.NewLocalCoordinator(db)

	var notifier *sdnotifier.Notifier
	if rcfg.UseSystemdNotifier {
		// systemd notifier
		notifier, err = sdnotifier.NewNotifier(ns, rcfg.SystemdNotifierDebug)
		if err != nil {
			return nil, err
		}
	} else {
		notifier = nil
	}

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

	//workload writer
	batchSize := rcfg.WorkloadBatchSize
	if batchSize == 0 {
		batchSize = 1000000
	}
	logFile := rcfg.LogFileName
	if logFile == "" {
		logFile = "mylogs.txt"
	}
	writ := workloadlog.NewLogger(batchSize, logFile)

	// request router
	rr := rulerouter.NewRouter(frTLS, rcfg, notifier)

	stchan := make(chan struct{})
	localConsole, err := console.NewLocalInstanceConsole(lc, rr, stchan, writ)
	if err != nil {
		spqrlog.Zero.Error().Err(err).Msg("failed to initialize router")
		return nil, err
	}

	r := &InstanceImpl{
		RuleRouter: rr,
		Qrouter:    qr,
		AdmConsole: localConsole,
		Mgr:        lc,
		stchan:     stchan,
		frTLS:      frTLS,
		cfg:        rcfg,
		Writer:     writ,
		notifier:   notifier,
	}

	return r, nil
}

func (r *InstanceImpl) serv(netconn net.Conn, pt port.RouterPortType) error {
	routerClient, err := r.RuleRouter.PreRoute(netconn, pt)
	if err != nil {
		_ = netconn.Close()
		return err
	}

	defer netconn.Close()

	/* If cancel, procced and return, close connection */
	if routerClient.CancelMsg() != nil {
		return r.RuleRouter.CancelClient(routerClient.CancelMsg())
	}

	if pt == port.ADMRouterPortType || routerClient.DB() == "spqr-console" {
		return r.AdmConsole.Serve(context.Background(), routerClient)
	}

	spqrlog.Zero.Debug().
		Uint("client", routerClient.ID()).
		Msg("prerouting phase succeeded")

	cmngr, err := poolmgr.MatchConnectionPooler(routerClient, r.RuleRouter.Config())
	if err != nil {
		return err
	}

	r.RuleRouter.AddClient(routerClient)
	defer r.RuleRouter.ReleaseClient(routerClient)
	defer func() {
		_, _ = routerClient.Route().ReleaseClient(routerClient.ID())
	}()

	return frontend.Frontend(r.Qrouter, routerClient, cmngr, r.RuleRouter.Config(), r.Writer)
}

func (r *InstanceImpl) Run(ctx context.Context, listener net.Listener, pt port.RouterPortType) error {
	if r.cfg.WithJaeger {
		closer, err := r.initJaegerTracer(r.RuleRouter.Config())
		if err != nil {
			return fmt.Errorf("could not initialize jaeger tracer: %s", err)
		}
		defer func() { _ = closer.Close() }()
	}

	if r.notifier != nil {
		if err := r.notifier.Ready(); err != nil {
			return fmt.Errorf("could not send ready msg: %s", err)
		}
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

	if r.notifier != nil {
		go func() {
			for {
				if err := r.notifier.Notify(); err != nil {
					spqrlog.Zero.Error().Err(err).Msg("error sending systemd notification")
				}
				time.Sleep(sdnotifier.Timeout)
			}
		}()
	}

	for {
		select {
		case conn := <-cChan:
			if !r.Initialized() {
				/* do not accept client connections on un-initialized router */
				_ = conn.Close()
			} else {
				go func() {
					if err := r.serv(conn, pt); err != nil {
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
				if err := r.serv(conn, port.ADMRouterPortType); err != nil {
					spqrlog.Zero.Error().Err(err).Msg("")
				}
			}()
		}
	}
}
