package rrouter

import (
	"crypto/tls"
	"fmt"
	"log"
	"net"
	"os"

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

type RequestRouter interface {
	Shutdown() error
	PreRoute(conn net.Conn) (rclient.RouterClient, error)
	ObsoleteRoute(key route.RouteKey) error

	AddDataShard(key qdb.ShardKey) error
	AddWorldShard(key qdb.ShardKey) error
	AddShardInstance(key qdb.ShardKey, host string)
}

type RRouter struct {
	routePool RoutePool

	frontendRules map[route.RouteKey]*config.FrontendRule
	backendRules  map[route.RouteKey]*config.BackendRule

	defaultFrontendRule *config.FrontendRule
	defaultBackendRule  *config.BackendRule

	tlsconfig *tls.Config
	lg        *log.Logger

	wgs map[qdb.ShardKey]Watchdog
}

func (r *RRouter) AddWorldShard(key qdb.ShardKey) error {
	tracelog.InfoLogger.Printf("added world datashard to rrouter %v", key.Name)
	return nil
}

func (r *RRouter) AddShardInstance(key qdb.ShardKey, host string) {
	panic("implement me")
}

func (r *RRouter) AddDataShard(key qdb.ShardKey) error {
	return nil
}

var _ RequestRouter = &RRouter{}

func (r *RRouter) Shutdown() error {
	return r.routePool.Shutdown()
}

func NewRouter(tlsconfig *tls.Config) *RRouter {
	frontendRules := map[route.RouteKey]*config.FrontendRule{}
	var defaultFrontendRule *config.FrontendRule
	for _, rule := range config.RouterConfig().FrontendRules {
		if rule.PoolDefault {
			defaultFrontendRule = rule
			continue
		}
		key := *route.NewRouteKey(rule.Usr, rule.DB)
		frontendRules[key] = rule
	}

	backendRules := map[route.RouteKey]*config.BackendRule{}
	var defaultBackendRule *config.BackendRule
	for _, rule := range config.RouterConfig().BackendRules {
		if rule.PoolDefault {
			defaultBackendRule = rule
			continue
		}
		key := *route.NewRouteKey(rule.Usr, rule.DB)
		backendRules[key] = rule
	}

	return &RRouter{
		routePool:           NewRouterPoolImpl(config.RouterConfig().ShardMapping),
		frontendRules:       frontendRules,
		backendRules:        backendRules,
		defaultFrontendRule: defaultFrontendRule,
		defaultBackendRule:  defaultBackendRule,
		lg:                  log.New(os.Stdout, "router", 0),
		wgs:                 map[qdb.ShardKey]Watchdog{},
		tlsconfig:           tlsconfig,
	}
}

func (r *RRouter) PreRoute(conn net.Conn) (rclient.RouterClient, error) {
	cl := rclient.NewPsqlClient(conn)

	if err := cl.Init(r.tlsconfig); err != nil {
		return cl, err
	}

	// match client frontend rule
	key := *route.NewRouteKey(cl.Usr(), cl.DB())
	frRule, ok := r.frontendRules[key]
	if !ok {
		if r.defaultFrontendRule != nil {
			// ok
			frRule = &config.FrontendRule{
				Usr:                   cl.Usr(),
				DB:                    cl.DB(),
				AuthRule:              r.defaultFrontendRule.AuthRule,
				PoolMode:              r.defaultFrontendRule.PoolMode,
				PoolPreparedStatement: r.defaultFrontendRule.PoolPreparedStatement,
			}
		} else {
			errmsg := fmt.Sprintf("Failed to route frontend for client: route for user:%s and db:%s is unconfigured", cl.Usr(), cl.DB())
			for _, msg := range []pgproto3.BackendMessage{
				&pgproto3.ErrorResponse{
					Message: errmsg,
				},
			} {
				if err := cl.Send(msg); err != nil {
					return nil, errors.Wrap(err, "failed to make route failure responce")
				}
			}

			return nil, errors.New(errmsg)
		}
	}

	beRule, ok := r.backendRules[key]
	if !ok {
		if r.defaultBackendRule != nil {
			beRule = &config.BackendRule{
				Usr: cl.Usr(),
				DB:  cl.DB(),
			}
		} else {
			errmsg := fmt.Sprintf("Failed to route backend for client: route for user:%s and db:%s is unconfigured", cl.Usr(), cl.DB())
			for _, msg := range []pgproto3.BackendMessage{
				&pgproto3.ErrorResponse{
					Message: errmsg,
				},
			} {
				if err := cl.Send(msg); err != nil {
					return nil, errors.Wrap(err, "failed to make route failure responce")
				}
			}

			return nil, errors.New(errmsg)
		}
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

func (r *RRouter) ListShards() []string {
	var ret []string

	for _, sh := range config.RouterConfig().ShardMapping {
		ret = append(ret, sh.Hosts[0])
	}

	return ret
}

func (r *RRouter) ObsoleteRoute(key route.RouteKey) error {
	rt := r.routePool.Obsolete(key)

	if err := rt.NofityClients(func(cl client.Client) error {
		return nil
	}); err != nil {
		return err
	}

	return nil
}
