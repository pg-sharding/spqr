package qrouter

import (
	"context"
	"sort"

	"sync/atomic"

	"github.com/pg-sharding/lyx/lyx"
	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/connmgr"
	"github.com/pg-sharding/spqr/pkg/meta"
	"github.com/pg-sharding/spqr/pkg/metrics"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/pkg/models/topology"
	"github.com/pg-sharding/spqr/pkg/session"
	"github.com/pg-sharding/spqr/pkg/spqrlog"
	"github.com/pg-sharding/spqr/router/cache"
	"github.com/pg-sharding/spqr/router/planner"
	"github.com/pg-sharding/spqr/router/rmeta"
	"github.com/pg-sharding/spqr/router/statistics"
)

type ProxyQrouter struct {
	planner.QueryPlanner

	ColumnMapping map[string]struct{}
	LocalTables   map[string]struct{}

	tmgr topology.TopologyMgr

	// shards

	cfg *config.QRouter

	mgr            meta.EntityMgr
	csm            connmgr.ConnectionMgr
	schemaCache    *cache.SchemaCache
	idRangeCache   planner.IdentityRouterCache
	metricRegistry *metrics.RouterMetricRegistry

	initialized *atomic.Bool
	ready       *atomic.Bool
}

// AnalyzeQuery implements QueryRouter.
func (qr *ProxyQrouter) AnalyzeQuery(ctx context.Context,
	sph session.SessionParamsHolder,
	rule *config.FrontendRule, query string, stmt lyx.Node, mCache *rmeta.MetadataCache) (*rmeta.RoutingMetadataContext, error) {

	ro := true

	if config.RouterConfig().Qr.AutoRouteRoOnStandby {
		ro = planner.CheckRoOnlyQuery(stmt)
	}

	rm := rmeta.NewRoutingMetadataContext(sph, rule, query, stmt, qr.csm, qr.mgr, mCache)

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

	rv := make([]kr.ShardKey, 0)

	for _, el := range qr.tmgr.Snap() {
		rv = append(rv, kr.ShardKey{
			Name: el.ID,
		})
	}

	sort.Slice(rv, func(i, j int) bool {
		return rv[i].Name < rv[j].Name
	})
	return rv
}

func (qr *ProxyQrouter) MetricRegistry() *metrics.RouterMetricRegistry {
	return qr.metricRegistry
}

var _ planner.QueryPlanner = &ProxyQrouter{}

func NewProxyRouter(tmgr topology.TopologyMgr,
	mgr meta.EntityMgr,
	csm connmgr.ConnectionMgr,
	qcfg *config.QRouter,
	cache *cache.SchemaCache,
	idRangeCache planner.IdentityRouterCache,
	metricRegistry *metrics.RouterMetricRegistry,
) (*ProxyQrouter, error) {

	proxy := &ProxyQrouter{
		initialized:    &atomic.Bool{},
		ready:          &atomic.Bool{},
		cfg:            qcfg,
		mgr:            mgr,
		tmgr:           tmgr,
		csm:            csm,
		schemaCache:    cache,
		idRangeCache:   idRangeCache,
		metricRegistry: metricRegistry,
	}
	proxy.registerMetrics()
	ctx := context.TODO()
	/* XXX: since memqdb is persistent on disk, in some cases, we need to recreate the whole topology.
	* TODO: get this information from the coordinator, not the config.
	 */

	exists, err := mgr.ListShards(ctx)
	if err != nil {
		return nil, err
	}
	skipMp := map[string]struct{}{}
	for _, k := range exists {
		skipMp[k.ID] = struct{}{}
	}

	/* XXX: fix this */
	for k, v := range tmgr.Snap() {
		if _, ok := skipMp[k]; !ok {
			if err := mgr.AddDataShard(ctx, v); err != nil {
				return nil, err
			}
		}
	}

	return proxy, nil
}

func (qr *ProxyQrouter) registerMetrics() {
	totalConnectionsMetric := &metrics.DynamicGauge{
		Name: metrics.ClientConnectionsTCPTotalName,
		Help: "Current number of client tcp connections",
		Getter: func() float64 {
			return float64(qr.csm.TotalTCPCount())
		},
		Value: 0,
	}

	inboundQueriesTotalMetric := &metrics.DynamicGauge{
		Name: metrics.InboundQueriesTotalName,
		Help: "Number of incoming queries",
		Getter: func() float64 {
			return float64(statistics.GetTotalRequests())
		},
		Value: 0,
	}

	qr.metricRegistry.RegisterDynamicGauge(totalConnectionsMetric)
	qr.metricRegistry.RegisterDynamicGauge(inboundQueriesTotalMetric)
}
