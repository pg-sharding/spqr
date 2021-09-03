package internal

import (
	"github.com/pg-sharding/spqr/internal/config"
	"sync"
)

type RoutePool interface {
	MatchRoute(key routeKey,
		beRule *config.BERule,
		frRule *config.FRRule,
	) (*Route, error)
}


type RoutePoolImpl struct {
	mu sync.Mutex

	pool map[routeKey]*Route

	mapping map[string]*config.ShardCfg
}

func (r *RoutePoolImpl) MatchRoute(key routeKey,
	beRule *config.BERule,
	frRule *config.FRRule) (*Route, error) {
	var route *Route

	r.mu.Lock()
	defer r.mu.Unlock()

	if route, ok := r.pool[key]; ok {
		return route, nil
	} else {
		route = NewRoute(beRule, frRule, r.mapping)

		r.pool[key] = route
	}

	return route, nil
}


var _ RoutePool = &RoutePoolImpl{}

func NewRouterPoolImpl(	mapping map[string]*config.ShardCfg) *RoutePoolImpl {
	return &RoutePoolImpl{
		mapping: mapping,
	}
}