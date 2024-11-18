package rulerouter

import (
	"sync"

	"github.com/pg-sharding/spqr/pkg/client"
	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/pool"
	"github.com/pg-sharding/spqr/pkg/shard"
	"github.com/pg-sharding/spqr/pkg/spqrlog"
	"github.com/pg-sharding/spqr/router/route"
)

type RoutePool interface {
	shard.ShardIterator
	pool.PoolIterator

	MatchRoute(key route.Key,
		beRule *config.BackendRule,
		frRule *config.FrontendRule,
	) (*route.Route, error)

	Obsolete(key route.Key) *route.Route
	Shutdown() error
	NotifyRoutes(func(route *route.Route) error) error
}

type RoutePoolImpl struct {
	mu           sync.Mutex
	pool         map[route.Key]*route.Route
	shardMapping map[string]*config.Shard
}

var _ RoutePool = &RoutePoolImpl{}

func NewRouterPoolImpl(shardMapping map[string]*config.Shard) *RoutePoolImpl {
	return &RoutePoolImpl{
		shardMapping: shardMapping,
		pool:         map[route.Key]*route.Route{},
	}
}

// TODO : unit tests
func (r *RoutePoolImpl) ForEach(cb func(sh shard.Shardinfo) error) error {
	return r.NotifyRoutes(func(route *route.Route) error {
		return route.ServPool().ForEach(cb)
	})
}

// TODO : unit tests
func (r *RoutePoolImpl) NotifyRoutes(cb func(route *route.Route) error) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	for _, rt := range r.pool {
		if err := cb(rt); err != nil {
			spqrlog.Zero.Info().
				Err(err).
				Msg("error while notifying route")
			return err
		}
	}

	return nil
}

// TODO : unit tests
func (r *RoutePoolImpl) Obsolete(key route.Key) *route.Route {
	r.mu.Lock()
	defer r.mu.Unlock()

	ret := r.pool[key]

	delete(r.pool, key)
	return ret
}

// TODO : unit tests
func (r *RoutePoolImpl) Shutdown() error {
	r.mu.Lock()
	defer r.mu.Unlock()
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

// TODO : unit tests
func (r *RoutePoolImpl) MatchRoute(key route.Key,
	beRule *config.BackendRule,
	frRule *config.FrontendRule) (*route.Route, error) {

	r.mu.Lock()
	defer r.mu.Unlock()

	if nroute, ok := r.pool[key]; ok {
		spqrlog.Zero.Info().
			Str("user", key.Usr()).
			Str("db", key.DB()).
			Msg("match route")
		return nroute, nil
	}

	spqrlog.Zero.Debug().
		Str("user", key.Usr()).
		Str("db", key.DB()).
		Msg("allocate route")
	nroute := route.NewRoute(beRule, frRule, r.shardMapping)

	r.pool[key] = nroute
	return nroute, nil
}

// TODO : unit tests
func (r *RoutePoolImpl) ForEachPool(cb func(pool.Pool) error) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, route := range r.pool {
		_ = route.ForEachPool(cb)
	}
	return nil
}
