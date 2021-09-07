package rrouter

import (
	"crypto/tls"
	"log"
	"net"
	"os"
	"sync"

	"github.com/jackc/pgproto3"
	"github.com/pg-sharding/spqr/internal/config"
	"github.com/pg-sharding/spqr/internal/qdb"
	"github.com/pkg/errors"
	"github.com/wal-g/tracelog"
)

type Router interface {
	Shutdown() error
	PreRoute(conn net.Conn) (Client, error)
	ObsoleteRoute(key routeKey) error
	AddRouteRule(key routeKey, befule *config.BERule, frRule *config.FRRule) error

	AddShard(key qdb.ShardKey) error
	AddInstance(key qdb.ShardKey, cfg *config.InstanceCFG)
}

type RRouter struct {
	routePool RoutePool

	frontendRules map[routeKey]*config.FRRule
	backendRules  map[routeKey]*config.BERule

	mu sync.Mutex

	cfg *tls.Config
	lg  *log.Logger

	wgs map[qdb.ShardKey]Watchdog
}

func (r *RRouter) AddInstance(key qdb.ShardKey, cfg *config.InstanceCFG) {

	panic("implement me")
}

func (r *RRouter) AddShard(key qdb.ShardKey) error {
	wg, err := NewShardWatchDog(r.cfg, key.Name, r.routePool)

	if err != nil {
		return err
	}

	wg.Run()

	r.mu.Lock()
	defer r.mu.Unlock()

	r.wgs[key] = wg

	return nil
}

var _ Router = &RRouter{}

func (r *RRouter) Shutdown() error {
	return r.routePool.Shutdown()
}

func NewRouter(tlscfg *tls.Config) (*RRouter, error) {
	frs := make(map[routeKey]*config.FRRule)

	for _, e := range config.Get().RouterConfig.FrontendRules {
		frs[routeKey{
			usr: e.RK.Usr,
			db:  e.RK.DB,
		}] = e
	}

	router := &RRouter{
		routePool:     NewRouterPoolImpl(config.Get().RouterConfig.ShardMapping),
		frontendRules: map[routeKey]*config.FRRule{},
		backendRules:  map[routeKey]*config.BERule{},
		lg:            log.New(os.Stdout, "router", 0),
		wgs:           map[qdb.ShardKey]Watchdog{},
	}

	for _, berule := range config.Get().RouterConfig.BackendRules {
		key := routeKey{
			usr: berule.RK.Usr,
			db:  berule.RK.DB,
		}
		_ = router.AddRouteRule(key, berule, frs[key])
	}

	if config.Get().RouterConfig.TLSCfg.SslMode != config.SSLMODEDISABLE {
		router.cfg = tlscfg
	}

	return router, nil
}

func (r *RRouter) PreRoute(conn net.Conn) (Client, error) {

	cl := NewClient(conn)

	if err := cl.Init(r.cfg, config.Get().RouterConfig.TLSCfg.SslMode); err != nil {
		return nil, err
	}

	// match client frontend rule
	key := routeKey{
		usr: cl.Usr(),
		db:  cl.DB(),
	}

	frRule, ok := r.frontendRules[key]
	if !ok {

		for _, msg := range []pgproto3.BackendMessage{
			&pgproto3.ErrorResponse{
				Message: "failed to route",
			},
		} {
			if err := cl.Send(msg); err != nil {
				return nil, errors.Wrap(err, "failed to make route failure resp")
			}
		}

		return nil, errors.New("Failed to route client")
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

	route, err := r.routePool.MatchRoute(key, beRule, frRule)

	if err != nil {
		tracelog.ErrorLogger.Fatal(err)
	}
	_ = route.AddClient(cl)
	_ = cl.AssignRoute(route)

	return cl, nil
}

func (r *RRouter) ListShards() []string {
	var ret []string

	for _, sh := range config.Get().RouterConfig.ShardMapping {
		ret = append(ret, sh.Hosts[0].ConnAddr)
	}

	return ret
}

func (r *RRouter) ObsoleteRoute(key routeKey) error {
	route := r.routePool.Obsolete(key)

	if err := route.NofityClients(func(cl Client) error {
		return nil
	}); err != nil {
		return err
	}

	return nil
}

func (r *RRouter) AddRouteRule(key routeKey, befule *config.BERule, frRule *config.FRRule) error {

	r.mu.Lock()
	defer r.mu.Unlock()

	r.backendRules[key] = befule
	r.frontendRules[key] = frRule

	return nil
}
