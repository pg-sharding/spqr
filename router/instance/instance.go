package instance

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"time"

	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/coord"
	"github.com/pg-sharding/spqr/pkg/grpccreds"
	"github.com/pg-sharding/spqr/pkg/meta"
	"github.com/pg-sharding/spqr/pkg/metrics"
	"github.com/pg-sharding/spqr/pkg/models/sequences"
	"github.com/pg-sharding/spqr/pkg/models/topology"
	"github.com/pg-sharding/spqr/pkg/spqrlog"
	"github.com/pg-sharding/spqr/pkg/workloadlog"
	"github.com/pg-sharding/spqr/qdb"
	"github.com/pg-sharding/spqr/router/cache"
	"github.com/pg-sharding/spqr/router/console"
	"github.com/pg-sharding/spqr/router/frontend"
	"github.com/pg-sharding/spqr/router/planner"
	"github.com/pg-sharding/spqr/router/poolmgr"
	"github.com/pg-sharding/spqr/router/port"
	"github.com/pg-sharding/spqr/router/qrouter"
	"github.com/pg-sharding/spqr/router/rulerouter"
	sdnotifier "github.com/pg-sharding/spqr/router/sdnotifier"
	"golang.org/x/time/rate"
)

const (
	defaultAcceptorMaxRetries = 10

	defaultCancelRateLimit       = 100000 /* 100k per second is basically no limit */
	defaultAcceptorRetrySleep    = 50 * time.Millisecond
	defaultAcceptorRetrySleepMax = 1 * time.Second
)

var (
	cancelRateLim = rate.NewLimiter(rate.Limit(config.ValueOrDefaultInt(config.RouterConfig().CancelRateLimit, defaultCancelRateLimit)), defaultCancelRateLimit)
)

// Accept connection and send it to channel.
func acceptLoop(l net.Listener, cChan chan net.Conn,
	kind string, /* To get different log messages for console/non-console clients */
	maxRetries int, retrySleep, retrySleepMax time.Duration) {

	/* XXX: move this logic somewhere */
	if maxRetries <= 0 {
		maxRetries = defaultAcceptorMaxRetries
	}
	if retrySleep <= 0 {
		retrySleep = defaultAcceptorRetrySleep
	}
	if retrySleepMax <= 0 {
		retrySleepMax = defaultAcceptorRetrySleepMax
	}
	if retrySleepMax < retrySleep {
		retrySleepMax = retrySleep
	}

	errCount := 0
	var backoff time.Duration
	for {
		c, err := l.Accept()

		/* Most of net error are already filtered out by
		* go runtime reties. However, not all of possible error codes are
		* handled https://github.com/golang/go/blob/25de5ebd/src/internal/poll/fd_unix.go#L613-L628
		* accept(2) doc explicitly tells to retry  ENETDOWN,
		* EPROTO, ENOPROTOOPT, EHOSTDOWN, ENONET, EHOSTUNREACH, EOPNOTSUPP,
		* and ENETUNREACH, so we will simply retry everything, but fail anyway in case of
		* long enough series of errors. */
		if err != nil {
			errCount++

			if backoff == 0 {
				backoff = retrySleep
			} else {
				backoff *= 2
			}
			if backoff > retrySleepMax {
				backoff = retrySleepMax
			}

			spqrlog.Zero.Error().
				Err(err).
				Int("attempt", errCount).
				Int("max retries", maxRetries).
				Dur("retry in", backoff).
				Msgf("failed to accept a new connection")
			if errCount >= maxRetries {
				spqrlog.Zero.Error().
					Str("kind", kind).
					Msgf("acceptor reached max consecutive errors on listener, shutting down")
				close(cChan)
				return
			}
			time.Sleep(backoff)
			continue
		}
		errCount = 0
		backoff = 0
		spqrlog.Zero.Info().
			Str("kind", kind).
			Str("remote addr", c.RemoteAddr().String()).
			Msg("new network client connection")

		/*  XXX: what if channel is closed? */
		cChan <- c
	}
}

type RouterInstance interface {
	Addr() string
	ID() string
	Initialized() bool
	Initialize() bool

	Console() console.Console
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

	notifier *sdnotifier.Notifier
}

// Console implements RouterInstance.
func (r *InstanceImpl) Console() console.Console {
	return r.AdmConsole
}

func (r *InstanceImpl) ID() string {
	return "no_id"
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

func NewRouter(_ context.Context, ns string, maxTxnBatchSize uint16, metricRegistry *metrics.RouterMetricRegistry) (*InstanceImpl, error) {
	if err := grpccreds.ValidateClient(config.RouterConfig().ClientTLS); err != nil {
		return nil, fmt.Errorf("init coordinator gRPC client TLS: %w", err)
	}

	db, err := qdb.GetStateKeeperQDB()
	if err != nil {
		return nil, err
	}

	cache := cache.NewSchemaCache(topology.TopMgr, config.RouterConfig().SchemaCacheBackendRule)

	var notifier *sdnotifier.Notifier
	if config.RouterConfig().UseSystemdNotifier {
		// systemd notifier
		notifier, err = sdnotifier.NewNotifier(ns, config.RouterConfig().SystemdNotifierDebug)
		if err != nil {
			return nil, err
		}
	} else {
		notifier = nil
	}

	// qrouter init
	qtype := config.RouterMode(config.RouterConfig().RouterMode)
	spqrlog.Zero.Debug().
		Type("qtype", qtype).
		Msg("creating QueryRouter with type")

	// frontend
	frTLS, err := config.RouterConfig().FrontendTLS.Init(config.RouterConfig().Host)
	if err != nil {
		return nil, fmt.Errorf("init frontend TLS: %w", err)
	}

	//workload writer
	batchSize := config.RouterConfig().WorkloadBatchSize
	if batchSize == 0 {
		batchSize = 1000000
	}
	logFile := config.RouterConfig().LogFileName
	if logFile == "" {
		logFile = "mylogs.txt"
	}
	writ := workloadlog.NewLogger(batchSize, logFile)

	// request router
	rr := rulerouter.NewRouter(topology.TopMgr, frTLS, config.RouterConfig(), notifier)
	lc := coord.NewLocalInstanceMetadataMgr(
		db,
		db,
		cache,
		topology.TopMgr,
		config.RouterConfig().ManageShardsByCoordinator,
		rr,
		maxTxnBatchSize,
	)

	var seqMngr sequences.SequenceMgr = lc
	idRangeSize := config.RouterConfig().IdentityRangeSize
	var identityMgr planner.IdentityRouterCache = planner.NewIdentityRouterCache(idRangeSize, &seqMngr)

	stchan := make(chan struct{})
	localConsole, err := console.NewLocalInstanceConsole(lc, rr, stchan, writ)
	if err != nil {
		spqrlog.Zero.Error().Err(err).Msg("failed to initialize router")
		return nil, err
	}

	qr, err := qrouter.NewQrouter(qtype,
		topology.TopMgr, /* TODO: fix */
		lc,
		rr,
		&config.RouterConfig().Qr,
		cache,
		identityMgr,
		metricRegistry,
	)
	if err != nil {
		return nil, err
	}

	r := &InstanceImpl{
		RuleRouter: rr,
		Qrouter:    qr,
		AdmConsole: localConsole,
		Mgr:        lc,
		stchan:     stchan,
		frTLS:      frTLS,
		Writer:     writ,
		notifier:   notifier,
	}

	return r, nil
}

func (r *InstanceImpl) watchRouterReadiness(ctx context.Context) {
	spqrlog.Zero.Info().Msg("waiting for router to be ready")
	for {
		select {
		case <-ctx.Done():
			spqrlog.Zero.Info().Msg("context done, exiting readiness watch")
			return
		default:
			healthChecks := r.RuleRouter.InstanceHealthChecks()
			ready := len(healthChecks) == 0
			for _, check := range r.RuleRouter.InstanceHealthChecks() {
				if check.CR.Alive {
					ready = true
				}
			}
			r.Qrouter.SetReady(ready)
			time.Sleep(100 * time.Millisecond) // TODO tune this interval
		}
	}
}

func (r *InstanceImpl) serv(netconn net.Conn, pt port.RouterPortType) (uint, error) {
	defer func() {
		if err := netconn.Close(); err != nil {
			spqrlog.Zero.Debug().Err(err).Msg("failed to close netconn")
		}
	}()
	defer r.RuleRouter.ReleaseConnection()

	routerClient, err := r.RuleRouter.PreRoute(netconn, pt)
	if err != nil {
		if routerClient == nil {
			return 0, err
		}
		return routerClient.ID(), err
	}

	/* If cancel, proceed and return, close connection */
	if routerClient.CancelMsg() != nil {

		if config.RouterConfig().IgnoreCancel {
			return routerClient.ID(), nil
		}

		if err := cancelRateLim.Wait(context.Background()); err != nil {
			return 0, err
		}

		return routerClient.ID(), r.RuleRouter.CancelClient(routerClient.CancelMsg())
	}

	if pt == port.ADMRouterPortType || routerClient.DB() == "spqr-console" {
		return routerClient.ID(), r.AdmConsole.Serve(context.Background(), routerClient)
	}

	spqrlog.Zero.Debug().
		Uint("client", routerClient.ID()).
		Msg("prerouting phase succeeded")

	cmngr, err := poolmgr.MatchConnectionPooler(routerClient)
	if err != nil {
		return routerClient.ID(), err
	}

	r.RuleRouter.AddClient(routerClient)
	defer r.RuleRouter.ReleaseClient(routerClient)
	defer func() {
		_, _ = routerClient.Route().ReleaseClient(routerClient.ID())
	}()

	return routerClient.ID(), frontend.Frontend(r.Qrouter, routerClient, cmngr, r.Writer)
}

func (r *InstanceImpl) Run(ctx context.Context, listener net.Listener, pt port.RouterPortType) error {
	if r.notifier != nil {
		if err := r.notifier.Ready(); err != nil {
			return fmt.Errorf("could not send ready msg: %s", err)
		}
	}

	cChan := make(chan net.Conn, max(config.RouterConfig().AcceptorBufferSize, 1))

	go acceptLoop(listener, cChan, "router",
		config.RouterConfig().AcceptorMaxRetries,
		config.RouterConfig().AcceptorRetrySleep,
		config.RouterConfig().AcceptorRetrySleepMax)
	go r.watchRouterReadiness(ctx)

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
		case conn, ok := <-cChan:
			if !ok {
				_ = r.RuleRouter.Shutdown()
				_ = listener.Close()
				return fmt.Errorf("acceptor is down, stopping router")
			}

			initTime := time.Now()
			if !r.Initialized() {
				/* do not accept client connections on un-initialized router */
				_ = conn.Close()
			} else {
				go func() {

					if id, err := r.serv(conn, pt); err != nil {

						n := time.Now()
						if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
							spqrlog.Zero.Info().
								Uint("client id", id).
								Int64("ms", n.UnixMilli()).
								TimeDiff("client live time", n, initTime).
								Err(err).Msg("error serving client")
						} else {
							spqrlog.Zero.Error().
								Uint("client id", id).
								Int64("ms", n.UnixMilli()).
								TimeDiff("client live time", n, initTime).
								Err(err).Msg("error serving client")
						}
					} else {
						n := time.Now()
						spqrlog.Zero.Info().
							Uint("id", id).
							Int64("ms", n.UnixMilli()).
							TimeDiff("client live time", n, initTime).
							Str("remote addr", conn.RemoteAddr().String()).
							Msg("client disconnected")
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

	go acceptLoop(listener, cChan, "admin console",
		config.RouterConfig().AcceptorMaxRetries,
		config.RouterConfig().AcceptorRetrySleep,
		config.RouterConfig().AcceptorRetrySleepMax)

	for {
		select {
		case <-ctx.Done():
			_ = listener.Close()
			spqrlog.Zero.Info().Msg("admin server done")
			return nil
		case conn, ok := <-cChan:
			if !ok {
				_ = r.RuleRouter.Shutdown()
				_ = listener.Close()
				return fmt.Errorf("acceptor is down, stopping router")
			}

			go func() {
				if id, err := r.serv(conn, port.ADMRouterPortType); err != nil {
					spqrlog.Zero.Error().Uint("id", id).Int64("ms", time.Now().UnixMilli()).Err(err).Msg("")
				} else {
					spqrlog.Zero.Info().Uint("id", id).Int64("ms", time.Now().UnixMilli()).Msg("client disconnected")
				}
			}()
		}
	}
}
