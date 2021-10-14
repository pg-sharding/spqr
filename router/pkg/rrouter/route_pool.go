package rrouter

import (
	"sync"

	"github.com/pg-sharding/spqr/pkg/client"
	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/wal-g/tracelog"
)

type RoutePool interface {
	MatchRoute(key routeKey,
		beRule *config.BERule,
		frRule *config.FRRule,
	) (*Route, error)

	Obsolete(key routeKey) *Route

	Shutdown() error

	NotifyRoutes(func(route *Route) error) error
}

type RoutePoolImpl struct {
	mu sync.Mutex

	pool map[routeKey]*Route

	mapping map[string]*config.ShardCfg
}

func (r *RoutePoolImpl) NotifyRoutes(cb func(route *Route) error) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	for _, route := range r.pool {
		go func(route *Route) {
			if err := cb(route); err != nil {
				tracelog.InfoLogger.Printf("error while notifying route %v", err)
			}
		}(route)
	}

	return nil
}

func (r *RoutePoolImpl) Obsolete(key routeKey) *Route {

	r.mu.Lock()
	r.mu.Unlock()

	ret := r.pool[key]

	delete(r.pool, key)

	return ret
}

func (r *RoutePoolImpl) Shutdown() error {
	for _, route := range r.pool {
		go route.NofityClients(func(cl client.Client) error {
			return cl.Shutdown()
		})
	}

	return nil
}

func (r *RoutePoolImpl) MatchRoute(key routeKey,
	beRule *config.BERule,
	frRule *config.FRRule) (*Route, error) {

	r.mu.Lock()
	defer r.mu.Unlock()

	if route, ok := r.pool[key]; ok {
		tracelog.InfoLogger.Printf("match route %v", key)

		return route, nil
	}

	tracelog.InfoLogger.Printf("allocate route %v", key)
	route := NewRoute(beRule, frRule, r.mapping)

	r.pool[key] = route

	return route, nil
}

var _ RoutePool = &RoutePoolImpl{}

func NewRouterPoolImpl(mapping map[string]*config.ShardCfg) *RoutePoolImpl {
	return &RoutePoolImpl{
		mapping: mapping,
		pool:    map[routeKey]*Route{},
	}
}
