package qrouter

import (
	"context"
	"math/rand"
	"sync"

	"sync/atomic"

	"github.com/pg-sharding/lyx/lyx"
	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/connmgr"
	"github.com/pg-sharding/spqr/pkg/meta"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/pkg/models/topology"
	"github.com/pg-sharding/spqr/pkg/session"
	"github.com/pg-sharding/spqr/pkg/spqrlog"
	"github.com/pg-sharding/spqr/router/cache"
	"github.com/pg-sharding/spqr/router/planner"
	"github.com/pg-sharding/spqr/router/rmeta"
)

type ProxyQrouter struct {
	planner.QueryPlanner

	mu sync.Mutex

	ColumnMapping map[string]struct{}
	LocalTables   map[string]struct{}

	// shards
	DataShardCfgs  map[string]*config.Shard
	WorldShardCfgs map[string]*config.Shard

	cfg *config.QRouter

	mgr          meta.EntityMgr
	csm          connmgr.ConnectionMgr
	schemaCache  *cache.SchemaCache
	idRangeCache planner.IdentityRouterCache

	initialized *atomic.Bool
	ready       *atomic.Bool
}

// AnalyzeQuery implements QueryRouter.
func (qr *ProxyQrouter) AnalyzeQuery(ctx context.Context,
	sph session.SessionParamsHolder,
	rule *config.FrontendRule, query string, stmt lyx.Node) (*rmeta.RoutingMetadataContext, error) {

	ro := true

	if config.RouterConfig().Qr.AutoRouteRoOnStandby {
		ro = planner.CheckRoOnlyQuery(stmt)
	}

	rm := rmeta.NewRoutingMetadataContext(sph, rule, query, stmt, qr.csm, qr.mgr)

	rm.SetRO(ro)

	if sph.ExecuteOn() == "" && !sph.ScatterQuery() {
		if err := planner.AnalyzeQueryV1(ctx, rm, rm.Stmt); err != nil {
			spqrlog.Zero.Debug().Err(err).Msg("failed to analyze query")

			/* XXX: below is very hacky */
			/* Does DRH force any executions? */
			for _, sh := range qr.DataShardsRoutes() {
				if sh.Name == rm.SPH.DefaultRouteBehaviour() {
					return rm, nil
				}
			}

			return nil, err
		}
	}
	return rm, nil
}

// IdRange implements QueryRouter.
func (qr *ProxyQrouter) IdRange() planner.IdentityRouterCache {
	return qr.idRangeCache
}

var _ QueryRouter = &ProxyQrouter{}

func (qr *ProxyQrouter) Initialized() bool {
	return qr.initialized.Load()
}

func (qr *ProxyQrouter) Initialize() bool {
	return qr.initialized.Swap(true)
}

func (qr *ProxyQrouter) Ready() bool {
	return qr.ready.Load()
}

func (qr *ProxyQrouter) SetReady(ready bool) {
	qr.ready.Store(ready)
}

func (qr *ProxyQrouter) Mgr() meta.EntityMgr {
	return qr.mgr
}

func (qr *ProxyQrouter) CSM() connmgr.ConnectionMgr {
	return qr.csm
}

func (qr *ProxyQrouter) SchemaCache() *cache.SchemaCache {
	return qr.schemaCache
}

// TODO : unit tests
func (qr *ProxyQrouter) DataShardsRoutes() []kr.ShardKey {
	rc, _ := qr.mgr.ListShards(context.TODO())
	rv := make([]kr.ShardKey, 0, len(rc))
	for _, el := range rc {
		rv = append(rv, kr.ShardKey{
			Name: el.ID,
		})
	}
	return rv
}

// TODO : unit tests
func (qr *ProxyQrouter) WorldShardsRoutes() []kr.ShardKey {
	qr.mu.Lock()
	defer qr.mu.Unlock()

	var ret []kr.ShardKey

	for name := range qr.WorldShardCfgs {
		ret = append(ret, kr.ShardKey{
			Name: name,
		})
	}

	// a sort of round robin

	rand.Shuffle(len(ret), func(i, j int) {
		ret[i], ret[j] = ret[j], ret[i]
	})
	return ret
}

var _ planner.QueryPlanner = &ProxyQrouter{}

func NewProxyRouter(shardMapping map[string]*config.Shard,
	mgr meta.EntityMgr,
	csm connmgr.ConnectionMgr,
	qcfg *config.QRouter,
	cache *cache.SchemaCache,
	idRangeCache planner.IdentityRouterCache,
) (*ProxyQrouter, error) {
	proxy := &ProxyQrouter{
		WorldShardCfgs: map[string]*config.Shard{},
		initialized:    &atomic.Bool{},
		ready:          &atomic.Bool{},
		cfg:            qcfg,
		mgr:            mgr,
		csm:            csm,
		schemaCache:    cache,
		idRangeCache:   idRangeCache,
	}

	ctx := context.TODO()

	/* XXX: since memqdb is persistent on disk, in some cases, we need to recreate the whole topology.
	* TODO: get this information from the coordinator, not the config.
	 */
	sds, err := mgr.ListShards(ctx)
	if err != nil {
		return nil, err
	}
	for _, ds := range sds {
		err := mgr.DropShard(ctx, ds.ID)
		if err != nil {
			return nil, err
		}
	}

	for name, shardCfg := range shardMapping {
		switch shardCfg.Type {
		case config.WorldShard:
		case config.DataShard:
			fallthrough // default is datashard
		default:
			if err := mgr.AddDataShard(ctx, &topology.DataShard{
				ID:  name,
				Cfg: shardCfg,
			}); err != nil {
				return nil, err
			}
		}
	}
	return proxy, nil
}
