package rulerouter

import (
	"crypto/tls"
	"fmt"
	"net"
	"sync"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/pg-sharding/spqr/pkg/auth"
	"github.com/pg-sharding/spqr/pkg/client"
	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/connectiterator"
	"github.com/pg-sharding/spqr/pkg/pool"
	"github.com/pg-sharding/spqr/pkg/shard"
	"github.com/pg-sharding/spqr/pkg/spqrlog"
	"github.com/pg-sharding/spqr/qdb"
	rclient "github.com/pg-sharding/spqr/router/client"
	"github.com/pg-sharding/spqr/router/port"
	"github.com/pg-sharding/spqr/router/route"
	"github.com/pg-sharding/spqr/router/rule"
	notifier "github.com/pg-sharding/spqr/router/sdnotifier"
	"github.com/pkg/errors"
)

type RuleRouter interface {
	connectiterator.ConnectIterator

	Shutdown() error
	Reload(configPath string) error
	PreRoute(conn net.Conn, pt port.RouterPortType) (rclient.RouterClient, error)
	PreRouteInitializedClientAdm(cl rclient.RouterClient) (rclient.RouterClient, error)
	ObsoleteRoute(key route.Key) error

	AddDataShard(key qdb.ShardKey) error
	AddWorldShard(key qdb.ShardKey) error
	AddShardInstance(key qdb.ShardKey, host string)

	CancelClient(csm *pgproto3.CancelRequest) error
	AddClient(cl rclient.RouterClient)

	ReleaseClient(cl rclient.RouterClient)

	Config() *config.Router
}

type RuleRouterImpl struct {
	routePool RoutePool
	rmgr      rule.RulesMgr

	tlsconfig *tls.Config

	mu   sync.Mutex
	rcfg *config.Router

	clmu sync.Mutex
	clmp map[uint32]rclient.RouterClient

	notifier *notifier.Notifier
}

func (r *RuleRouterImpl) AddWorldShard(key qdb.ShardKey) error {
	spqrlog.Zero.Info().
		Str("shard name", key.Name).
		Msg("added world datashard to rrouter")
	return nil
}

func (r *RuleRouterImpl) AddShardInstance(key qdb.ShardKey, host string) {
	panic("implement me")
}

func (r *RuleRouterImpl) AddDataShard(key qdb.ShardKey) error {
	return nil
}

func (r *RuleRouterImpl) Shutdown() error {
	return r.routePool.Shutdown()
}

func (r *RuleRouterImpl) ForEach(cb func(sh shard.Shardinfo) error) error {
	return r.routePool.ForEach(cb)
}

// TODO : unit tests
func ParseRules(rcfg *config.Router) (map[route.Key]*config.FrontendRule, map[route.Key]*config.BackendRule, *config.FrontendRule, *config.BackendRule) {
	frontendRules := map[route.Key]*config.FrontendRule{}
	var defaultFrontendRule *config.FrontendRule
	for _, frontendRule := range rcfg.FrontendRules {
		if frontendRule.PoolDefault {
			defaultFrontendRule = frontendRule
			continue
		}
		spqrlog.Zero.Debug().
			Str("db", frontendRule.DB).
			Str("user", frontendRule.Usr).
			Msg("adding frontend rule")
		key := *route.NewRouteKey(frontendRule.Usr, frontendRule.DB)
		frontendRules[key] = frontendRule
	}

	backendRules := map[route.Key]*config.BackendRule{}
	var defaultBackendRule *config.BackendRule
	for _, backendRule := range rcfg.BackendRules {
		if backendRule.PoolDefault {
			defaultBackendRule = backendRule
			continue
		}
		key := *route.NewRouteKey(backendRule.Usr, backendRule.DB)
		backendRules[key] = backendRule
	}

	return frontendRules, backendRules, defaultFrontendRule, defaultBackendRule
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

	err := config.LoadRouterCfg(configPath)
	if err != nil {
		return err
	}
	rcfg := config.RouterConfig()

	r.mu.Lock()
	defer r.mu.Unlock()

	if err := spqrlog.UpdateZeroLogLevel(rcfg.LogLevel); err != nil {
		return err
	}

	frontendRules, backendRules, defaultFrontendRule, defaultBackendRule := ParseRules(rcfg)
	r.rmgr.Reload(frontendRules, backendRules, defaultFrontendRule, defaultBackendRule)

	if r.notifier != nil {
		if err = r.notifier.Ready(); err != nil {
			return err
		}
	}

	return nil
}

func NewRouter(tlsconfig *tls.Config, rcfg *config.Router, notifier *notifier.Notifier) *RuleRouterImpl {
	frontendRules, backendRules, defaultFrontendRule, defaultBackendRule := ParseRules(rcfg)
	return &RuleRouterImpl{
		routePool: NewRouterPoolImpl(rcfg.ShardMapping),
		rcfg:      rcfg,
		rmgr:      rule.NewMgr(frontendRules, backendRules, defaultFrontendRule, defaultBackendRule),
		tlsconfig: tlsconfig,
		clmp:      map[uint32]rclient.RouterClient{},
		notifier:  notifier,
	}
}

// TODO : unit tests
func (r *RuleRouterImpl) PreRoute(conn net.Conn, pt port.RouterPortType) (rclient.RouterClient, error) {
	cl := rclient.NewPsqlClient(conn, pt, r.Config().Qr.DefaultRouteBehaviour)

	if err := cl.Init(r.tlsconfig); err != nil {
		return cl, err
	}

	if cl.CancelMsg() != nil {
		return cl, nil
	}

	if pt == port.ADMRouterPortType || cl.DB() == "spqr-console" {
		return r.PreRouteInitializedClientAdm(cl)
	}

	// match client to frontend rule
	key := *route.NewRouteKey(cl.Usr(), cl.DB())
	frRule, err := r.rmgr.MatchKeyFrontend(key)
	if err != nil {
		for _, msg := range []pgproto3.BackendMessage{
			&pgproto3.ErrorResponse{
				Message: err.Error(),
			},
		} {
			if err := cl.Send(msg); err != nil {
				return nil, errors.Wrap(err, "failed to make route failure response")
			}
		}
		return nil, err
	}

	beRule, err := r.rmgr.MatchKeyBackend(key)
	if err != nil {
		for _, msg := range []pgproto3.BackendMessage{
			&pgproto3.ErrorResponse{
				Message: err.Error(),
			},
		} {
			if err := cl.Send(msg); err != nil {
				return nil, errors.Wrap(err, "failed to make route failure response")
			}
		}
		return nil, err
	}

	_ = cl.AssignRule(frRule)

	rt, err := r.routePool.MatchRoute(key, beRule, frRule)
	if err != nil {
		return nil, err
	}

	if err := cl.Auth(rt); err != nil {
		_ = cl.ReplyErrMsg(err.Error())
		return cl, err
	}

	spqrlog.Zero.
		Debug().
		Uint("client", spqrlog.GetPointer(cl)).
		Msg("client auth succeeded")

	if err != nil {
		return nil, err
	}
	if err := cl.AssignRoute(rt); err != nil {
		return nil, err
	}
	if err := rt.AddClient(cl); err != nil {
		return nil, err
	}
	return cl, nil
}

// TODO : unit tests
func (r *RuleRouterImpl) PreRouteInitializedClientAdm(cl rclient.RouterClient) (rclient.RouterClient, error) {
	key := *route.NewRouteKey(cl.Usr(), cl.DB())
	frRule, err := r.rmgr.MatchKeyFrontend(key)
	if err != nil {
		_ = cl.ReplyErrMsg(err.Error())
		return nil, err
	}

	spqrlog.Zero.Debug().
		Str("db", frRule.DB).
		Str("user", frRule.Usr).
		Msg("console client routed")

	if err := cl.AssignRule(frRule); err != nil {
		_ = cl.ReplyErrMsg("failed to assign rule")
		return nil, err
	}

	if err := auth.AuthFrontend(cl, frRule); err != nil {
		_ = cl.ReplyErrMsg(err.Error())
		return cl, err
	}

	return cl, nil
}

// TODO : unit tests
func (r *RuleRouterImpl) ListShards() []string {
	var ret []string

	for _, sh := range r.rcfg.ShardMapping {
		ret = append(ret, sh.Hosts[0])
	}

	return ret
}

// TODO : unit tests
func (r *RuleRouterImpl) ObsoleteRoute(key route.Key) error {
	rt := r.routePool.Obsolete(key)

	if err := rt.NofityClients(func(cl client.ClientInfo) error {
		return nil
	}); err != nil {
		return err
	}

	return nil
}

// TODO : unit tests
func (r *RuleRouterImpl) Config() *config.Router {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.rcfg
}

// TODO : unit tests
func (r *RuleRouterImpl) AddClient(cl rclient.RouterClient) {
	r.clmu.Lock()
	defer r.clmu.Unlock()
	r.clmp[cl.GetCancelPid()] = cl
}

// TODO : unit tests
func (r *RuleRouterImpl) ReleaseClient(cl rclient.RouterClient) {
	r.clmu.Lock()
	defer r.clmu.Unlock()
	delete(r.clmp, cl.GetCancelPid())
}

// TODO : unit tests
func (r *RuleRouterImpl) CancelClient(csm *pgproto3.CancelRequest) error {
	r.clmu.Lock()
	defer r.clmu.Unlock()

	if cl, ok := r.clmp[csm.ProcessID]; ok {
		if cl.GetCancelKey() != csm.SecretKey {
			return fmt.Errorf("cancel secret does not match")
		}

		spqrlog.Zero.Debug().Uint32("pid", csm.ProcessID).Msg("cancelling client")
		return cl.Cancel()
	}
	return fmt.Errorf("no client with pid %d", csm.ProcessID)
}

// TODO : unit tests
func (rr *RuleRouterImpl) ClientPoolForeach(cb func(client client.ClientInfo) error) error {
	return rr.routePool.NotifyRoutes(func(route *route.Route) error {
		return route.NofityClients(cb)
	})
}

// TODO : unit tests
func (rr *RuleRouterImpl) Pop(clientID uint) (bool, error) {
	var popped = false
	err := rr.routePool.NotifyRoutes(func(route *route.Route) error {
		ok, nestedErr := route.ReleaseClient(clientID)
		popped = popped || ok
		return nestedErr
	})

	return popped, err
}

func (rr *RuleRouterImpl) Put(id client.Client) error {
	return nil
}

func (rr *RuleRouterImpl) ForEachPool(cb func(pool.Pool) error) error {
	return rr.routePool.ForEachPool(cb)
}

var _ RuleRouter = &RuleRouterImpl{}
