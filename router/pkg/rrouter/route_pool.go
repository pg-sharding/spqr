package rrouter

import (
	"sync"

	"github.com/pg-sharding/spqr/pkg/client"
	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/router/pkg/route"
	"github.com/wal-g/tracelog"
)

type RoutePool interface {
	MatchRoute(key route.Key,
		beRule *config.BERule,
		frRule *config.FRRule,
	) (*route.Route, error)

	Obsolete(key route.Key) *route.Route
	Shutdown() error
	NotifyRoutes(func(route *route.Route) error) error
}

type RoutePoolImpl struct {
	mu      sync.Mutex
	pool    map[route.Key]*route.Route
	mapping map[string]*config.ShardCfg
}

var _ RoutePool = &RoutePoolImpl{}

func NewRouterPoolImpl(mapping map[string]*config.ShardCfg) *RoutePoolImpl {
	return &RoutePoolImpl{
		mapping: mapping,
		pool:    map[route.Key]*route.Route{},
	}
}

func (r *RoutePoolImpl) NotifyRoutes(cb func(route *route.Route) error) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	for _, rt := range r.pool {
		go func(rt *route.Route) {
			if err := cb(rt); err != nil {
				tracelog.InfoLogger.Printf("error while notifying route %v", err)
			}
		}(rt)
	}

	return nil
}

func (r *RoutePoolImpl) Obsolete(key route.Key) *route.Route {
	r.mu.Lock()
	r.mu.Unlock()

	ret := r.pool[key]

	delete(r.pool, key)
	return ret
}

func (r *RoutePoolImpl) Shutdown() error {
	for _, rt := range r.pool {
		rt := rt
		go func() {
			_ = rt.NofityClients(func(cl client.Client) error {
				return cl.Shutdown()
			})
		}()
	}

	return nil
}

func (r *RoutePoolImpl) MatchRoute(key route.Key,
	beRule *config.BERule,
	frRule *config.FRRule) (*route.Route, error) {

	r.mu.Lock()
	defer r.mu.Unlock()

	if nroute, ok := r.pool[key]; ok {
		tracelog.InfoLogger.Printf("match route %v", key)
		return nroute, nil
	}

	tracelog.InfoLogger.Printf("allocate route %v", key)
	nroute := route.NewRoute(beRule, frRule, r.mapping)

	r.pool[key] = nroute
	return nroute, nil
}
