package rrouter

import (
	"crypto/tls"
	"fmt"
	"log"
	"net"
	"os"
	"sync"

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
	ObsoleteRoute(key route.Key) error
	AddRouteRule(key route.Key, befule *config.BERule, frRule *config.FRRule) error

	AddDataShard(key qdb.ShardKey) error
	AddWorldShard(key qdb.ShardKey) error
	AddShardInstance(key qdb.ShardKey, cfg *config.InstanceCFG)
}

type RRouter struct {
	routePool RoutePool

	frontendRules map[route.Key]*config.FRRule
	backendRules  map[route.Key]*config.BERule

	mu sync.Mutex

	cfg *tls.Config
	lg  *log.Logger

	wgs map[qdb.ShardKey]Watchdog
}

func (r *RRouter) AddWorldShard(key qdb.ShardKey) error {
	tracelog.InfoLogger.Printf("added world datashard to rrouter %v", key.Name)
	return nil
}

func (r *RRouter) AddShardInstance(key qdb.ShardKey, cfg *config.InstanceCFG) {
	panic("implement me")
}

func (r *RRouter) AddDataShard(key qdb.ShardKey) error {
	return nil
	// wait to datashard to become available
	//wg, err := NewShardWatchDog(r.cfg, key.Name, r.routePool)
	//
	//if err != nil {
	//	return errors.Wrap(err, "NewShardWatchDog")
	//}
	//
	//wg.Run()
	//
	//r.mu.Lock()
	//defer r.mu.Unlock()
	//
	//r.wgs[key] = wg
	//
	//return nil
}

var _ RequestRouter = &RRouter{}

func (r *RRouter) Shutdown() error {
	return r.routePool.Shutdown()
}

func (router *RRouter) initRules() error {
	frs := make(map[route.Key]*config.FRRule)

	for _, frRule := range config.RouterConfig().RulesConfig.FrontendRules {
		frs[*route.NewRouteKey(frRule.RK.Usr, frRule.RK.DB)] = frRule
	}

	for _, berule := range config.RouterConfig().RulesConfig.BackendRules {
		key := *route.NewRouteKey(
			berule.RK.Usr, berule.RK.DB,
		)
		if err := router.AddRouteRule(key, berule, frs[key]); err != nil {
			return err
		}
	}

	return nil
}

func NewRouter(tlscfg *tls.Config) (*RRouter, error) {
	router := &RRouter{
		routePool:     NewRouterPoolImpl(config.RouterConfig().RulesConfig.ShardMapping),
		frontendRules: map[route.Key]*config.FRRule{},
		backendRules:  map[route.Key]*config.BERule{},
		lg:            log.New(os.Stdout, "router", 0),
		wgs:           map[qdb.ShardKey]Watchdog{},
	}

	if err := router.initRules(); err != nil {
		return nil, err
	}

	if config.RouterConfig().RulesConfig.TLSCfg.SslMode != config.SSLMODEDISABLE {
		router.cfg = tlscfg
	}

	return router, nil
}

func (r *RRouter) PreRoute(conn net.Conn) (rclient.RouterClient, error) {

	cl := rclient.NewPsqlClient(conn)

	if err := cl.Init(r.cfg, config.RouterConfig().RulesConfig.TLSCfg.SslMode); err != nil {
		return cl, err
	}

	// match client frontend rule
	key := *route.NewRouteKey(cl.Usr(), cl.DB())

	frRule, ok := r.frontendRules[key]
	if !ok {
		errmsg := fmt.Sprintf("Failed to preroute client: route %s-%s is unconfigured", cl.Usr(), cl.DB())
		for _, msg := range []pgproto3.BackendMessage{
			&pgproto3.ErrorResponse{
				Message: errmsg,
			},
		} {
			if err := cl.Send(msg); err != nil {
				return nil, errors.Wrap(err, "failed to make route failure responce")
			}
		}

		return cl, errors.New(errmsg)
	}

	beRule, ok := r.backendRules[key]
	if !ok {
		return cl, errors.New("Failed to route client")
	}

	_ = cl.AssignRule(frRule)

	rt, err := r.routePool.MatchRoute(key, beRule, frRule)

	if err := cl.Auth(rt); err != nil {
		return cl, err
	}

	tracelog.InfoLogger.Printf("client auth OK")

	if err != nil {
		tracelog.ErrorLogger.Fatal(err)
	}
	_ = rt.AddClient(cl)
	if err := cl.AssignRoute(rt); err != nil {
		return nil, err
	}

	return cl, nil
}

func (r *RRouter) ListShards() []string {
	var ret []string

	for _, sh := range config.RouterConfig().RulesConfig.ShardMapping {
		ret = append(ret, sh.Hosts[0].ConnAddr)
	}

	return ret
}

func (r *RRouter) ObsoleteRoute(key route.Key) error {
	rt := r.routePool.Obsolete(key)

	if err := rt.NofityClients(func(cl client.Client) error {
		return nil
	}); err != nil {
		return err
	}

	return nil
}

func (r *RRouter) AddRouteRule(key route.Key, befule *config.BERule, frRule *config.FRRule) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.backendRules[key] = befule
	r.frontendRules[key] = frRule

	return nil
}
