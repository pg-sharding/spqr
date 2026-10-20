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
	"time"

	"github.com/pg-sharding/spqr/pkg/catalog"
	"github.com/pg-sharding/spqr/pkg/connmgr"
	"github.com/pg-sharding/spqr/pkg/models/spqrerror"
	"github.com/pg-sharding/spqr/pkg/models/topology"
	"github.com/pg-sharding/spqr/pkg/pool"
	"github.com/pg-sharding/spqr/pkg/router_util"
	"github.com/pg-sharding/spqr/pkg/tsa"
	"github.com/prometheus/client_golang/prometheus"
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

	rcfg *config.Router

	// Keep rule resolution and route creation in the same reload generation.
	rulesMu   sync.RWMutex
	routePool *RoutePoolImpl

	clmp sync.Map

	notifier *notifier.Notifier

	initSem semaphore.Weighted

	totalTCPCount       atomic.Int64
	activeTCPCount      atomic.Int64
	cancelConnCount     atomic.Int64
	clientInitFailCount atomic.Int64
	clientAuthFailCount atomic.Int64

	InstanceStartTime time.Time
	LastReload        time.Time
}

func (r *RuleRouterImpl) PrimaryMonitorCollector() prometheus.Collector {
	return r.routePool
}

// LastReloadTime implements [RuleRouter].
func (r *RuleRouterImpl) LastReloadTime() time.Time {
	return r.LastReload
}

// StartTime implements [RuleRouter].
func (r *RuleRouterImpl) StartTime() time.Time {
	return r.InstanceStartTime
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
			if v2, ok := rt[k]; !ok || v.LastCheckTime.After(v2.LastCheckTime) {
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

// RunRecheckHosts implements ConnectionStatsMgr.
func (r *RuleRouterImpl) RunRecheckHosts() []pool.HostRecheckResult {
	var res []pool.HostRecheckResult
	_ = r.NotifyRoutes(func(r *route.Route) (bool, error) {
		res = append(res, r.MultiShardPool().RunRecheckHosts()...)
		return true, nil
	})
	return res
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

// FailedAuthCount implements RuleRouter.
func (r *RuleRouterImpl) FailedAuthCount() int64 {
	return r.clientAuthFailCount.Load()
}

// FailedInitCount implements RuleRouter.
func (r *RuleRouterImpl) FailedInitCount() int64 {
	return r.clientInitFailCount.Load()
}

// TODO : unit tests
func (r *RuleRouterImpl) Reload(configPath string) error {
	r.rulesMu.Lock()
	defer r.rulesMu.Unlock()
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

	router_util.ReloadRotateLog()

	r.rmgr.Reload(rcfg.FrontendRules, rcfg.BackendRules)
	r.rcfg = rcfg
	previousKeys := r.routePool.reconfigure(topology.TopMgr, rcfg)
	r.initializeConfiguredRoutes(previousKeys)

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

	r.LastReload = time.Now()

	return nil
}

func NewRouter(tmgr topology.TopologyMgr, tlsconfig *tls.Config, rcfg *config.Router, notifier *notifier.Notifier) *RuleRouterImpl {
	routePool := NewRouterPoolImpl(tmgr, rcfg)
	r := &RuleRouterImpl{
		RoutePool: routePool,
		routePool: routePool,
		rcfg:      rcfg,
		rmgr:      rulemgr.NewMgr(rcfg.FrontendRules, rcfg.BackendRules),
		tlsconfig: tlsconfig,
		clmp:      sync.Map{},
		notifier:  notifier,

		InstanceStartTime: time.Now(),
		LastReload:        time.Now(),
		initSem:           *semaphore.NewWeighted(config.ValueOrDefaultInt64(rcfg.ClientInitMax, defaultInstanceClientInitMax)),
	}
	r.initializeConfiguredRoutes(nil)
	return r
}

// initializeConfiguredRoutes discovers concrete identities from either side of
// the rule configuration, including matches supplied by a default rule. Default
// rules alone have no concrete identity and are registered on first resolution.
func (r *RuleRouterImpl) initializeConfiguredRoutes(previousKeys []route.Key) {
	if r.rcfg.DbpoolPrimaryCheckInterval <= 0 {
		return
	}
	keys := make(map[route.Key]struct{}, len(previousKeys))
	for _, key := range previousKeys {
		keys[key] = struct{}{}
	}
	for _, rule := range r.rcfg.FrontendRules {
		if !rule.PoolDefault && rule.Usr != "" && rule.DB != "" && rule.DB != "spqr-console" && rule.DB != "spqr-ping" {
			keys[*route.NewRouteKey(rule.Usr, rule.DB)] = struct{}{}
		}
	}
	for _, rule := range r.rcfg.BackendRules {
		if !rule.PoolDefault && rule.Usr != "" && rule.DB != "" && rule.DB != "spqr-console" && rule.DB != "spqr-ping" {
			keys[*route.NewRouteKey(rule.Usr, rule.DB)] = struct{}{}
		}
	}
	for key := range keys {
		frRule, err := r.rmgr.MatchKeyFrontend(key)
		if err != nil {
			continue
		}
		beRule, err := r.rmgr.MatchKeyBackend(key)
		if err != nil {
			continue
		}
		if _, err := r.MatchRoute(key, beRule, frRule); err != nil {
			spqrlog.Zero.Error().Err(err).Str("user", key.Usr()).Str("db", key.DB()).Msg("initialize primary monitoring route")
		}
	}
}

// resolveRoute holds the rule snapshot until the pool has installed the matched
// backend configuration and registered the resulting route with its monitor.
func (r *RuleRouterImpl) resolveRoute(key route.Key) (*route.Route, *config.FrontendRule, error) {
	r.rulesMu.RLock()
	defer r.rulesMu.RUnlock()
	frRule, err := r.rmgr.MatchKeyFrontend(key)
	if err != nil {
		return nil, nil, err
	}
	beRule, err := r.rmgr.MatchKeyBackend(key)
	if err != nil {
		return nil, nil, err
	}
	rt, err := r.MatchRoute(key, beRule, frRule)
	return rt, frRule, err
}

// TODO : unit tests
func (r *RuleRouterImpl) PreRoute(conn net.Conn, pt port.RouterPortType) (rclient.RouterClient, error) {
	r.totalTCPCount.Add(1)
	r.activeTCPCount.Add(1)

	cl := rclient.NewPsqlClient(conn,
		pt,
		config.RouterConfig().ShowNoticeMessages,
		config.RouterConfig().Qr.DefaultTSA)

	tlsConfig := r.tlsconfig
	if pt == port.UnixSocketPortType {
		tlsConfig = nil
	}

	_ = r.initSem.Acquire(context.TODO(), 1)

	if err := cl.Init(tlsConfig); err != nil {
		r.initSem.Release(1)
		r.clientInitFailCount.Add(1)
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
	rt, frRule, err := r.resolveRoute(key)
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

	if err := cl.Auth(rt); err != nil {
		r.clientAuthFailCount.Add(1)
		_ = cl.ReplyErr(err)
		if !config.RouterConfig().DisableObsoleteClient {
			r.Obsolete(key)
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
	r.rulesMu.RLock()
	frRule, err := r.rmgr.MatchKeyFrontend(key)
	r.rulesMu.RUnlock()
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
