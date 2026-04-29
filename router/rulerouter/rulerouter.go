package rulerouter

import (
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	"maps"
	"net"
	"sync"
	"sync/atomic"

	"github.com/pg-sharding/spqr/pkg/catalog"
	"github.com/pg-sharding/spqr/pkg/connmgr"
	"github.com/pg-sharding/spqr/pkg/models/spqrerror"
	"github.com/pg-sharding/spqr/pkg/models/topology"
	"github.com/pg-sharding/spqr/pkg/pool"
	"github.com/pg-sharding/spqr/pkg/tsa"
	"golang.org/x/sync/semaphore"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/pg-sharding/spqr/pkg/auth"
	"github.com/pg-sharding/spqr/pkg/client"
	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/rulemgr"
	"github.com/pg-sharding/spqr/pkg/spqrlog"
	rclient "github.com/pg-sharding/spqr/router/client"
	"github.com/pg-sharding/spqr/router/port"
	"github.com/pg-sharding/spqr/router/route"
	notifier "github.com/pg-sharding/spqr/router/sdnotifier"
)

const (
	defaultInstanceClientInitMax int64 = 200
)

type RuleRouter interface {
	connmgr.ConnectionMgr

	Shutdown() error
	Reload(configPath string) error
	PreRoute(conn net.Conn, pt port.RouterPortType) (rclient.RouterClient, error)

	AddClient(cl rclient.RouterClient)
	CancelClient(csm *pgproto3.CancelRequest) error
	ReleaseClient(cl rclient.RouterClient)

	ReleaseConnection()
}

type RuleRouterImpl struct {
	RoutePool
	rmgr rulemgr.RulesMgr

	tlsconfig *tls.Config

	mu   sync.Mutex
	rcfg *config.Router

	clmp sync.Map

	notifier *notifier.Notifier

	initSem semaphore.Weighted

	totalTCPCount   atomic.Int64
	activeTCPCount  atomic.Int64
	cancelConnCount atomic.Int64
}

// ErrorCounts implements [RuleRouter].
func (r *RuleRouterImpl) ErrorCounts() map[string]uint64 {
	counters := map[string]uint64{}
	_ = r.NotifyRoutes(func(route *route.Route) (bool, error) {
		curr := route.ErrorCounts()
		for k, v := range curr {
			counters[k] += v
		}

		return true, nil
	})

	return counters
}

// ReportError implements [RuleRouter].
func (r *RuleRouterImpl) ReportError(_ string) {
	/* nothing */
}

// InstanceHealthChecks implements RuleRouter.
func (r *RuleRouterImpl) InstanceHealthChecks() map[string]tsa.CachedCheckResult {
	rt := map[string]tsa.CachedCheckResult{}
	_ = r.NotifyRoutes(func(r *route.Route) (bool, error) {
		m := r.MultiShardPool().InstanceHealthChecks()
		for k, v := range m {
			// we are interested in most recent check
			if v2, ok := rt[k]; !ok || v2.LastCheckTime.UnixNano() > v.LastCheckTime.UnixNano() {
				rt[k] = v
			}
		}
		return true, nil
	})
	return rt
}

// TsaCacheEntries implements ConnectionStatsMgr.
func (r *RuleRouterImpl) TsaCacheEntries() map[pool.TsaKey]pool.CachedEntry {
	rt := map[pool.TsaKey]pool.CachedEntry{}
	_ = r.NotifyRoutes(func(r *route.Route) (bool, error) {
		m := r.MultiShardPool().TsaCacheEntries()
		maps.Copy(rt, m)
		return true, nil
	})
	return rt
}

// ReleaseConnection implements RuleRouter.
func (r *RuleRouterImpl) ReleaseConnection() {
	r.activeTCPCount.Add(-1)
}

// ActiveTCPCount implements RuleRouter.
func (r *RuleRouterImpl) ActiveTCPCount() int64 {
	return r.activeTCPCount.Load()
}

// TotalCancelCount implements RuleRouter.
func (r *RuleRouterImpl) TotalCancelCount() int64 {
	return r.cancelConnCount.Load()
}

// TotalTCPCount implements RuleRouter.
func (r *RuleRouterImpl) TotalTCPCount() int64 {
	return r.totalTCPCount.Load()
}

// TODO : unit tests
func (r *RuleRouterImpl) Reload(configPath string) error {
	/*
			* Reload config changes:
			* While reloading router config we need
			* to do the following:
			* 1) Re-read conf file.
			* 2) Add all new routes to router
		 	* 3) Mark all active routes as expired
	*/
	if r.notifier != nil {
		if err := r.notifier.Reloading(); err != nil {
			return err
		}
	}

	_, err := config.LoadRouterCfg(configPath)
	if err != nil {
		return err
	}
	rcfg := config.RouterConfig()
	topology.InitShardMapping(topology.DataShardMapFromConfig(rcfg.ShardMapping))

	r.mu.Lock()
	defer r.mu.Unlock()

	spqrlog.ReloadLogger(rcfg.LogFileName, rcfg.LogLevel, rcfg.PrettyLogging)

	r.rmgr.Reload(rcfg.FrontendRules, rcfg.BackendRules)

	if r.notifier != nil {
		if err = r.notifier.Ready(); err != nil {
			return err
		}
	}

	if rcfg.EnableRoleSystem && rcfg.RolesFile != "" {
		_, err := config.LoadRolesCfg(rcfg.RolesFile)
		if err != nil {
			return err
		}
		err = catalog.Reload(rcfg.EnableRoleSystem, config.RolesConfig().TableGroups)
		if err != nil {
			return err
		}
	}

	return nil
}

func NewRouter(tlsconfig *tls.Config, rcfg *config.Router, notifier *notifier.Notifier) *RuleRouterImpl {
	return &RuleRouterImpl{
		RoutePool: NewRouterPoolImpl(topology.ShardMapping),
		rcfg:      rcfg,
		rmgr:      rulemgr.NewMgr(rcfg.FrontendRules, rcfg.BackendRules),
		tlsconfig: tlsconfig,
		clmp:      sync.Map{},
		notifier:  notifier,
		initSem:   *semaphore.NewWeighted(config.ValueOrDefaultInt64(rcfg.ClientInitMax, defaultInstanceClientInitMax)),
	}
}

// TODO : unit tests
func (r *RuleRouterImpl) PreRoute(conn net.Conn, pt port.RouterPortType) (rclient.RouterClient, error) {
	r.totalTCPCount.Add(1)
	r.activeTCPCount.Add(1)

	cl := rclient.NewPsqlClient(conn,
		pt,
		string(config.RouterConfig().Qr.DefaultRouteBehaviour),
		config.RouterConfig().ShowNoticeMessages,
		config.RouterConfig().Qr.DefaultTSA)

	tlsConfig := r.tlsconfig
	if pt == port.UnixSocketPortType {
		tlsConfig = nil
	}

	_ = r.initSem.Acquire(context.TODO(), 1)

	if err := cl.Init(tlsConfig); err != nil {
		r.initSem.Release(1)
		return cl, err
	}

	r.initSem.Release(1)

	if cl.CancelMsg() != nil {
		r.cancelConnCount.Add(1)
		return cl, nil
	}

	if cl.Usr() == "spqr-ping" && cl.DB() == "spqr-ping" {
		// TODO : unit tests
		rule := &config.FrontendRule{
			Usr:      "spqr-ping",
			DB:       "spqr-ping",
			AuthRule: &config.AuthCfg{Method: config.AuthOK},
			PoolMode: config.PoolModeVirtual,
		}
		if err := cl.AssignRule(rule); err != nil {
			_ = cl.ReplyErr(
				spqrerror.Newf(spqrerror.SPQR_ROUTING_ERROR, "failed to assign rule"))
			return nil, err
		}
	}

	if pt == port.ADMRouterPortType || cl.DB() == "spqr-console" {
		return r.preRouteInitializedClientAdm(cl)
	}

	// match client to frontend rule
	key := *route.NewRouteKey(cl.Usr(), cl.DB())
	frRule, err := r.rmgr.MatchKeyFrontend(key)
	if err != nil {
		for _, msg := range []pgproto3.BackendMessage{
			&pgproto3.ErrorResponse{
				Severity: "ERROR",
				Message:  err.Error(),
			},
		} {
			if err := cl.Send(msg); err != nil {
				return nil, fmt.Errorf("failed to make route failure response: %w", err)
			}
		}
		return nil, err
	}

	beRule, err := r.rmgr.MatchKeyBackend(key)
	if err != nil {
		for _, msg := range []pgproto3.BackendMessage{
			&pgproto3.ErrorResponse{
				Severity: "ERROR",
				Message:  err.Error(),
			},
		} {
			if err := cl.Send(msg); err != nil {
				return nil, fmt.Errorf("failed to make route failure response: %w", err)
			}
		}
		return nil, err
	}

	_ = cl.AssignRule(frRule)

	rt, err := r.MatchRoute(key, beRule, frRule)
	if err != nil {
		return nil, err
	}

	if err := cl.Auth(rt); err != nil {
		_ = cl.ReplyErr(err)
		if !config.RouterConfig().DisableObsoleteClient {
			route := r.Obsolete(key)
			/* Stop watchdogs, if any */
			if route != nil {
				route.MultiShardPool().StopCacheWatchdog()
			}
		}
		return cl, err
	}

	spqrlog.Zero.
		Debug().
		Uint("client", spqrlog.GetPointer(cl)).
		Msg("client auth succeeded")

	if err := cl.AssignRoute(rt); err != nil {
		return nil, err
	}
	if err := rt.AddClient(cl); err != nil {
		return nil, err
	}
	return cl, nil
}

// TODO : unit tests
func (r *RuleRouterImpl) preRouteInitializedClientAdm(cl rclient.RouterClient) (rclient.RouterClient, error) {
	key := *route.NewRouteKey(cl.Usr(), cl.DB())
	frRule, err := r.rmgr.MatchKeyFrontend(key)
	if err != nil {
		_ = cl.ReplyErr(err)
		return nil, err
	}

	spqrlog.Zero.Debug().
		Str("db", frRule.DB).
		Str("user", frRule.Usr).
		Msg("console client routed")

	if err := cl.AssignRule(frRule); err != nil {
		_ = cl.ReplyErr(
			spqrerror.Newf(spqrerror.SPQR_ROUTING_ERROR, "failed to assign rule"))
		return nil, err
	}

	if err := auth.AuthFrontend(cl, frRule); err != nil {
		_ = cl.ReplyErr(err)
		return cl, err
	}

	return cl, nil
}

// TODO : unit tests
func (r *RuleRouterImpl) AddClient(cl rclient.RouterClient) {
	r.clmp.Store(cl.GetCancelPid(), cl)
}

// TODO : unit tests
func (r *RuleRouterImpl) ReleaseClient(cl rclient.RouterClient) {
	r.clmp.Delete(cl.GetCancelPid())
}

// TODO : unit tests
func (r *RuleRouterImpl) CancelClient(csm *pgproto3.CancelRequest) error {
	if v, ok := r.clmp.Load(csm.ProcessID); ok {
		cl, ok := v.(rclient.RouterClient)
		if !ok {
			return fmt.Errorf("internal: unexpected client type %T for pid %d", v, csm.ProcessID)
		}

		if !bytes.Equal(cl.GetCancelKey(), csm.SecretKey) {
			return fmt.Errorf("cancel secret does not match")
		}

		spqrlog.Zero.Debug().Uint32("pid", csm.ProcessID).Msg("cancelling client")
		return cl.Cancel()
	}
	return fmt.Errorf("no client with pid %d", csm.ProcessID)
}

// TODO : unit tests
func (r *RuleRouterImpl) ClientPoolForeach(cb func(client client.ClientInfo) error) error {
	return r.NotifyRoutes(func(route *route.Route) (bool, error) {
		return true, route.NotifyClients(cb)
	})
}

// TODO : unit tests
func (r *RuleRouterImpl) Pop(clientID uint) (bool, error) {
	var popped = false
	err := r.NotifyRoutes(func(route *route.Route) (bool, error) {
		ok, nestedErr := route.ReleaseClient(clientID)
		popped = popped || ok
		return !popped, nestedErr
	})

	return popped, err
}

func (r *RuleRouterImpl) Put(_ client.Client) error {
	return nil
}

var _ RuleRouter = &RuleRouterImpl{}
