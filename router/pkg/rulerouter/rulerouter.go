package rulerouter

import (
	"crypto/tls"
	"log"
	"net"
	"os"
	"sync"

	"github.com/pg-sharding/spqr/router/pkg/rule"

	"github.com/pg-sharding/spqr/pkg/spqrlog"

	"github.com/jackc/pgproto3/v2"
	"github.com/pg-sharding/spqr/pkg/client"
	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/qdb"
	rclient "github.com/pg-sharding/spqr/router/pkg/client"
	"github.com/pg-sharding/spqr/router/pkg/route"
	"github.com/pkg/errors"
	"github.com/wal-g/tracelog"
)

type RuleRouter interface {
	Shutdown() error
	Reload(configPath string) error
	PreRoute(conn net.Conn) (rclient.RouterClient, error)
	ObsoleteRoute(key route.Key) error

	AddDataShard(key qdb.ShardKey) error
	AddWorldShard(key qdb.ShardKey) error
	AddShardInstance(key qdb.ShardKey, host string)

	Config() *config.Router
}

type RuleRouterImpl struct {
	routePool RoutePool
	rmgr      rule.RulesMgr

	tlsconfig *tls.Config
	lg        *log.Logger

	mu   sync.Mutex
	rcfg *config.Router
}

func (r *RuleRouterImpl) AddWorldShard(key qdb.ShardKey) error {
	tracelog.InfoLogger.Printf("added world datashard to rrouter %v", key.Name)
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

func ParseRules(rcfg *config.Router) (map[route.Key]*config.FrontendRule, map[route.Key]*config.BackendRule, *config.FrontendRule, *config.BackendRule) {
	frontendRules := map[route.Key]*config.FrontendRule{}
	var defaultFrontendRule *config.FrontendRule
	for _, frontendRule := range rcfg.FrontendRules {
		if frontendRule.PoolDefault {
			defaultFrontendRule = frontendRule
			continue
		}
		spqrlog.Logger.Printf(spqrlog.DEBUG3, "adding frontend rule %+v", frontendRule)
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

func (r *RuleRouterImpl) Reload(configPath string) error {

	/*
			* Reload config changes:
			* While reloading router config we need
			* to do the following:
			* 1) Re-read conf file.
			* 2) Add all new routes to router
		 	* 3) Mark all active routes as expired
	*/

	rcfg, err := config.LoadRouterCfg(configPath)
	if err != nil {
		return err
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if err := spqrlog.UpdateDefaultLogLevel(rcfg.LogLevel); err != nil {
		return err
	}

	frontendRules, backendRules, defaultFrontendRule, defaultBackendRule := ParseRules(&rcfg)
	r.rmgr.Reload(frontendRules, backendRules, defaultFrontendRule, defaultBackendRule)

	return nil
}

func NewRouter(tlsconfig *tls.Config, rcfg *config.Router) *RuleRouterImpl {
	frontendRules, backendRules, defaultFrontendRule, defaultBackendRule := ParseRules(rcfg)
	return &RuleRouterImpl{
		routePool: NewRouterPoolImpl(rcfg.ShardMapping),
		rcfg:      rcfg,
		rmgr:      rule.NewMgr(frontendRules, backendRules, defaultFrontendRule, defaultBackendRule),
		lg:        log.New(os.Stdout, "router", 0),
		tlsconfig: tlsconfig,
	}
}

func (r *RuleRouterImpl) PreRoute(conn net.Conn) (rclient.RouterClient, error) {
	cl := rclient.NewPsqlClient(conn)

	if err := cl.Init(r.tlsconfig); err != nil {
		return cl, err
	}

	// match client frontend rule
	key := *route.NewRouteKey(cl.Usr(), cl.DB())
	frRule, err := r.rmgr.MatchKeyFrontend(key)
	if err != nil {
		for _, msg := range []pgproto3.BackendMessage{
			&pgproto3.ErrorResponse{
				Message: err.Error(),
			},
		} {
			if err := cl.Send(msg); err != nil {
				return nil, errors.Wrap(err, "failed to make route failure responce")
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
				return nil, errors.Wrap(err, "failed to make route failure responce")
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

	spqrlog.Logger.Printf(spqrlog.LOG, "client auth OK")

	if err != nil {
		spqrlog.Logger.PrintError(err)
		return nil, err
	}
	_ = rt.AddClient(cl)
	if err := cl.AssignRoute(rt); err != nil {
		return nil, err
	}

	return cl, nil
}

func (r *RuleRouterImpl) ListShards() []string {
	var ret []string

	for _, sh := range r.rcfg.ShardMapping {
		ret = append(ret, sh.Hosts[0])
	}

	return ret
}

func (r *RuleRouterImpl) ObsoleteRoute(key route.Key) error {
	rt := r.routePool.Obsolete(key)

	if err := rt.NofityClients(func(cl client.Client) error {
		return nil
	}); err != nil {
		return err
	}

	return nil
}

func (r *RuleRouterImpl) Config() *config.Router {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.rcfg
}

var _ RuleRouter = &RuleRouterImpl{}
