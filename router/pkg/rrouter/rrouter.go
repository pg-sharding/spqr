package rrouter

import (
	"crypto/tls"
	"log"
	"net"
	"os"
	"sync"

	"github.com/jackc/pgproto3/v2"
	"github.com/pg-sharding/spqr/pkg/client"
	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/qdb"
	client2 "github.com/pg-sharding/spqr/router/pkg/client"
	"github.com/pg-sharding/spqr/router/pkg/route"
	"github.com/pkg/errors"
	"github.com/wal-g/tracelog"
)

type RequestRouter interface {
	Shutdown() error
	PreRoute(conn net.Conn) (client2.RouterClient, error)
	ObsoleteRoute(key route.RouteKey) error
	AddRouteRule(key route.RouteKey, befule *config.BERule, frRule *config.FRRule) error

	AddDataShard(key qdb.ShardKey) error
	AddWorldShard(key qdb.ShardKey) error
	AddShardInstance(key qdb.ShardKey, cfg *config.InstanceCFG)
}

type RRouter struct {
	routePool RoutePool

	frontendRules map[route.RouteKey]*config.FRRule
	backendRules  map[route.RouteKey]*config.BERule

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

func NewRouter(tlscfg *tls.Config) (*RRouter, error) {
	frs := make(map[route.RouteKey]*config.FRRule)

	for _, frRule := range config.RouterConfig().RouterConfig.FrontendRules {
		frs[*route.NewRouteKey(frRule.RK.Usr, frRule.RK.DB)] = frRule
	}

	router := &RRouter{
		routePool:     NewRouterPoolImpl(config.RouterConfig().RouterConfig.ShardMapping),
		frontendRules: map[route.RouteKey]*config.FRRule{},
		backendRules:  map[route.RouteKey]*config.BERule{},
		lg:            log.New(os.Stdout, "worldmock", 0),
		wgs:           map[qdb.ShardKey]Watchdog{},
	}

	for _, berule := range config.RouterConfig().RouterConfig.BackendRules {
		key := *route.NewRouteKey(
			berule.RK.Usr, berule.RK.DB,
		)
		_ = router.AddRouteRule(key, berule, frs[key])
	}

	if config.RouterConfig().RouterConfig.TLSCfg.SslMode != config.SSLMODEDISABLE {
		router.cfg = tlscfg
	}

	return router, nil
}

func (r *RRouter) PreRoute(conn net.Conn) (client2.RouterClient, error) {

	cl := client2.NewPsqlClient(conn)

	if err := cl.Init(r.cfg, config.RouterConfig().RouterConfig.TLSCfg.SslMode); err != nil {
		return nil, err
	}

	// match client frontend rule
	key := *route.NewRouteKey(cl.Usr(), cl.DB())

	frRule, ok := r.frontendRules[key]
	if !ok {
		for _, msg := range []pgproto3.BackendMessage{
			&pgproto3.ErrorResponse{
				Message: "failed to preroute",
			},
		} {
			if err := cl.Send(msg); err != nil {
				return nil, errors.Wrap(err, "failed to make route failure resp")
			}
		}

		return nil, errors.New("Failed to preroute client")
	}

	beRule, ok := r.backendRules[key]
	if !ok {
		return nil, errors.New("Failed to route client")
	}

	_ = cl.AssignRule(frRule)

	if err := cl.Auth(); err != nil {
		return nil, err
	}
	tracelog.InfoLogger.Printf("client auth OK")

	rt, err := r.routePool.MatchRoute(key, beRule, frRule)

	if err != nil {
		tracelog.ErrorLogger.Fatal(err)
	}
	_ = rt.AddClient(cl)
	_ = cl.AssignRoute(rt)

	return cl, nil
}

func (r *RRouter) ListShards() []string {
	var ret []string

	for _, sh := range config.RouterConfig().RouterConfig.ShardMapping {
		ret = append(ret, sh.Hosts[0].ConnAddr)
	}

	return ret
}

func (r *RRouter) ObsoleteRoute(key route.RouteKey) error {
	route := r.routePool.Obsolete(key)

	if err := route.NofityClients(func(cl client.Client) error {
		return nil
	}); err != nil {
		return err
	}

	return nil
}

func (r *RRouter) AddRouteRule(key route.RouteKey, befule *config.BERule, frRule *config.FRRule) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.backendRules[key] = befule
	r.frontendRules[key] = frRule

	return nil
}
