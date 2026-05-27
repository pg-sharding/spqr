package rulerouter

import (
	"fmt"
	"sync"

	"github.com/pg-sharding/spqr/pkg/client"
	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/models/spqrerror"
	"github.com/pg-sharding/spqr/pkg/models/topology"
	"github.com/pg-sharding/spqr/pkg/pool"
	"github.com/pg-sharding/spqr/pkg/shard"
	"github.com/pg-sharding/spqr/pkg/spqrlog"
	"github.com/pg-sharding/spqr/router/route"
)

type RoutePool interface {
	shard.ShardHostIterator
	pool.PoolIterator

	MatchRoute(key route.Key,
		beRule *config.BackendRule,
		frRule *config.FrontendRule,
	) (*route.Route, error)

	Obsolete(key route.Key)
	Shutdown() error
	NotifyRoutes(func(route *route.Route) (bool, error)) error
}

type RoutePoolImpl struct {
	/*  route.Key -> *route.Route */
	pool sync.Map
	tmgr topology.TopologyMgr
}

var _ RoutePool = &RoutePoolImpl{}

func NewRouterPoolImpl(tmgr topology.TopologyMgr) *RoutePoolImpl {
	return &RoutePoolImpl{
		tmgr: tmgr,
		pool: sync.Map{},
	}
}

// TODO : unit tests
func (r *RoutePoolImpl) ForEach(cb func(sh shard.ShardHostCtl) error) error {
	return r.NotifyRoutes(func(route *route.Route) (bool, error) {
		return true, route.MultiShardPool().ForEach(cb)
	})
}

// TODO : unit tests
func (r *RoutePoolImpl) NotifyRoutes(cb func(route *route.Route) (bool, error)) error {
	var err error

	r.pool.Range(func(_, value any) bool {
		rt, ok := value.(*route.Route)
		if !ok {
			spqrlog.Zero.Error().Msg("unexpected route pool value type")
			return true
		}

		if cont, err := cb(rt); err != nil {
			spqrlog.Zero.Info().
				Err(err).
				Msg("error while notifying route")
			return false
		} else {
			return cont
		}
	})

	return err
}

// TODO : unit tests
func (r *RoutePoolImpl) Obsolete(key route.Key) {
	ret, ok := r.pool.LoadAndDelete(key)
	if ok {
		rt, ok := ret.(*route.Route)
		if !ok {
			return
		}

		/* Stop watchdogs, if any */
		rt.MultiShardPool().StopCacheWatchdog()
		/* XXX: do not .Close() or Shutdown client pool here, we
		* do not want to reset still active client connections */
	}
}

// TODO : unit tests
func (r *RoutePoolImpl) Shutdown() error {

	r.pool.Range(func(_, v any) bool {
		rt, ok := v.(*route.Route)
		if !ok {
			spqrlog.Zero.Error().Msg("unexpected route pool value type")
			return true
		}
		go func() {
			_ = rt.NotifyClients(func(cl client.ClientInfo) error {
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
		rt, ok := nroute.(*route.Route)
		if !ok {
			return nil, spqrerror.Newf(spqrerror.SPQR_METADATA_CORRUPTION, "internal: unexpected route type %T", nroute)
		}
		spqrlog.Zero.Debug().
			Str("user", key.Usr()).
			Str("db", key.DB()).
			Msg("match route OK")
		return rt, nil
	}

	spqrlog.Zero.Debug().
		Str("user", key.Usr()).
		Str("db", key.DB()).
		Msg("allocate route")

	nroute := route.NewRoute(
		beRule,
		frRule,
		r.tmgr,
		client.DefaultClientDeadCheckInterval)

	act, loaded := r.pool.LoadOrStore(key, nroute)

	if loaded {
		// conflict, release goroutines
		_ = nroute.Close()
	}
	rt, ok := act.(*route.Route)
	if !ok {
		return nil, fmt.Errorf("internal: unexpected route type %T", act)
	}
	return rt, nil
}

// TODO : unit tests
func (r *RoutePoolImpl) ForEachPool(cb func(pool.Pool) error) error {

	r.pool.Range(func(_, v any) bool {
		route, ok := v.(*route.Route)
		if !ok {
			spqrlog.Zero.Error().Msg("unexpected route pool value type")
			return true
		}
		_ = route.ForEachPool(cb)
		return true
	})
	return nil
}
