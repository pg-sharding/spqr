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
	/*  route.Key -> *route.Route */
	pool         sync.Map
	shardMapping map[string]*config.Shard
}

var _ RoutePool = &RoutePoolImpl{}

func NewRouterPoolImpl(shardMapping map[string]*config.Shard) *RoutePoolImpl {
	return &RoutePoolImpl{
		shardMapping: shardMapping,
		pool:         sync.Map{},
	}
}

// TODO : unit tests
func (r *RoutePoolImpl) ForEach(cb func(sh shard.ShardHostInfo) error) error {
	return r.NotifyRoutes(func(route *route.Route) error {
		return route.ServPool().ForEach(cb)
	})
}

// TODO : unit tests
func (r *RoutePoolImpl) NotifyRoutes(cb func(route *route.Route) error) error {
	var err error

	r.pool.Range(func(key, value any) bool {
		rt := value.(*route.Route)

		if err = cb(rt); err != nil {
			spqrlog.Zero.Info().
				Err(err).
				Msg("error while notifying route")
			return false
		}
		return true
	})

	return err
}

// TODO : unit tests
func (r *RoutePoolImpl) Obsolete(key route.Key) *route.Route {
	ret, ok := r.pool.LoadAndDelete(key)
	if ok {
		return ret.(*route.Route)
	}

	return nil
}

// TODO : unit tests
func (r *RoutePoolImpl) Shutdown() error {

	r.pool.Range(func(k, v any) bool {
		rt := v.(*route.Route)
		go func() {
			_ = rt.NofityClients(func(cl client.ClientInfo) error {
				return cl.Shutdown()
			})
		}()
		return true
	})

	return nil
}

// TODO : unit tests
func (r *RoutePoolImpl) MatchRoute(key route.Key,
	beRule *config.BackendRule,
	frRule *config.FrontendRule) (*route.Route, error) {

	if nroute, ok := r.pool.Load(key); ok {
		spqrlog.Zero.Info().
			Str("user", key.Usr()).
			Str("db", key.DB()).
			Msg("match route")
		return nroute.(*route.Route), nil
	}

	spqrlog.Zero.Debug().
		Str("user", key.Usr()).
		Str("db", key.DB()).
		Msg("allocate route")

	nroute := route.NewRoute(beRule, frRule, r.shardMapping)
	act, _ := r.pool.LoadOrStore(key, nroute)
	return act.(*route.Route), nil
}

// TODO : unit tests
func (r *RoutePoolImpl) ForEachPool(cb func(pool.Pool) error) error {

	r.pool.Range(func(k, v any) bool {
		route := v.(*route.Route)
		_ = route.ForEachPool(cb)
		return true
	})
	return nil
}
