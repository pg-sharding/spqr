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
	"github.com/prometheus/client_golang/prometheus"
)

type RoutePool interface {
	shard.ShardHostIterator
	pool.PoolIterator

	MatchRoute(key route.Key,
		beRule *config.BackendRule,
		frRule *config.FrontendRule,
	) (*route.Route, error)

	Obsolete(key route.Key)
	GracShutdown()
	Shutdown() error
	NotifyRoutes(func(route *route.Route) (bool, error)) error
}

type RoutePoolImpl struct {
	/*  route.Key -> *route.Route */
	pool sync.Map
	tmgr topology.TopologyMgr

	// Serialize registration with removal, reload and shutdown. A route must never
	// attach to a monitor after that monitor has been closed.
	lifecycleMu sync.Mutex
	monitor     *pool.PrimaryMonitor
	closed      bool
}

var _ RoutePool = &RoutePoolImpl{}

func NewRouterPoolImpl(tmgr topology.TopologyMgr, routerConfig ...*config.Router) *RoutePoolImpl {
	cfg := config.RouterConfig()
	if len(routerConfig) != 0 {
		cfg = routerConfig[0]
	}
	return &RoutePoolImpl{
		tmgr:    tmgr,
		pool:    sync.Map{},
		monitor: newPrimaryMonitor(cfg),
	}
}

func newPrimaryMonitor(cfg *config.Router) *pool.PrimaryMonitor {
	return pool.NewPrimaryMonitor(cfg.DbpoolPrimaryCheckInterval,
		config.ValueOrDefaultDuration(cfg.DbpoolPrimaryCheckTimeout, config.DefaultDbpoolPrimaryCheckTimeout),
		config.ValueOrDefaultInt(cfg.DbpoolPrimaryCheckConcurrency, config.DefaultDbpoolPrimaryCheckConcurrency))
}

// Describe and Collect keep the registered collector stable when reload replaces
// the scheduler and all of its route-scoped metric state.
func (r *RoutePoolImpl) Describe(ch chan<- *prometheus.Desc) {
	r.lifecycleMu.Lock()
	defer r.lifecycleMu.Unlock()
	r.monitor.Describe(ch)
}

func (r *RoutePoolImpl) Collect(ch chan<- prometheus.Metric) {
	r.lifecycleMu.Lock()
	defer r.lifecycleMu.Unlock()
	r.monitor.Collect(ch)
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
	r.lifecycleMu.Lock()
	defer r.lifecycleMu.Unlock()
	r.obsolete(key)
}

func (r *RoutePoolImpl) obsolete(key route.Key) {
	ret, ok := r.pool.LoadAndDelete(key)
	if ok {
		rt, ok := ret.(*route.Route)
		if !ok {
			return
		}

		if dbpool, ok := rt.MultiShardPool().(*pool.DBPool); ok {
			r.monitor.Unregister(dbpool)
		}
		/* Stop watchdogs, if any */
		rt.MultiShardPool().StopCacheWatchdog()
		/* XXX: do not .Close() or .Shutdown() client pool here, we
		* do not want to reset still active client connections */
		rt.ClientPool().GracShutdown()
	}
}

func (r *RoutePoolImpl) GracShutdown() {
	r.stopMonitoring()
}

func (r *RoutePoolImpl) stopMonitoring() {
	r.lifecycleMu.Lock()
	defer r.lifecycleMu.Unlock()
	if r.closed {
		return
	}
	r.closed = true
	r.pool.Range(func(_, value any) bool {
		if rt, ok := value.(*route.Route); ok {
			if dbpool, ok := rt.MultiShardPool().(*pool.DBPool); ok {
				r.monitor.Unregister(dbpool)
			}
			rt.MultiShardPool().StopCacheWatchdog()
			rt.ClientPool().GracShutdown()
		}
		return true
	})
	r.monitor.Close()
}

// reconfigure expires existing routes before installing a new scheduler. Active
// application connections can finish using their old route, while new clients
// resolve backend credentials and topology from the new configuration.
func (r *RoutePoolImpl) reconfigure(tmgr topology.TopologyMgr, cfg *config.Router) []route.Key {
	r.lifecycleMu.Lock()
	defer r.lifecycleMu.Unlock()
	var keys []route.Key
	if r.closed {
		return keys
	}
	r.pool.Range(func(key, _ any) bool {
		if key, ok := key.(route.Key); ok {
			keys = append(keys, key)
			r.obsolete(key)
		}
		return true
	})
	r.monitor.Close()
	r.monitor = newPrimaryMonitor(cfg)
	r.tmgr = tmgr
	return keys
}

// TODO : unit tests
func (r *RoutePoolImpl) Shutdown() error {
	r.stopMonitoring()

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
	r.lifecycleMu.Lock()
	defer r.lifecycleMu.Unlock()
	if r.closed {
		return nil, fmt.Errorf("router route pool is shutting down")
	}

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
	if !loaded {
		if dbpool, ok := rt.MultiShardPool().(*pool.DBPool); ok {
			r.monitor.Register(dbpool)
		}
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
