package qrouter

import (
	"context"
	"fmt"

	"sync/atomic"

	"github.com/pg-sharding/lyx/lyx"
	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/connmgr"
	"github.com/pg-sharding/spqr/pkg/meta"
	"github.com/pg-sharding/spqr/pkg/metrics"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/pkg/models/topology"
	"github.com/pg-sharding/spqr/pkg/plan"
	"github.com/pg-sharding/spqr/pkg/session"
	"github.com/pg-sharding/spqr/pkg/spqrlog"
	"github.com/pg-sharding/spqr/router/cache"
	"github.com/pg-sharding/spqr/router/planner"
	"github.com/pg-sharding/spqr/router/rmeta"
)

type LocalQrouter struct {
	ds    *topology.DataShard
	ready *atomic.Bool

	rm             *rmeta.RoutingMetadataContext
	metricRegistry *metrics.RouterMetricRegistry
}

// AnalyzeQuery implements QueryRouter.
func (qr *LocalQrouter) AnalyzeQuery(_ context.Context, _ session.SessionParamsHolder, _ *config.FrontendRule, _ string, stmt lyx.Node) (*rmeta.RoutingMetadataContext, error) {
	/* outer code expect this */
	qr.rm.Stmt = stmt
	return qr.rm, nil
}

// IdRange implements QueryRouter.
func (qr *LocalQrouter) IdRange() planner.IdentityRouterCache {
	return nil
}

func (qr *LocalQrouter) Ready() bool {
	return qr.ready.Load()
}

func (qr *LocalQrouter) SetReady(ready bool) {
	qr.ready.Store(ready)
}

// WorldShardsRoutes implements QueryRouter.
func (qr *LocalQrouter) WorldShardsRoutes() []kr.ShardKey {
	return []kr.ShardKey{}
}

var _ QueryRouter = &LocalQrouter{}

func NewLocalQrouter(shardMapping map[string]*topology.DataShard, metricRegistry *metrics.RouterMetricRegistry) (*LocalQrouter, error) {
	if len(shardMapping) != 1 {
		err := fmt.Errorf("local router support only single-datashard routing")
		spqrlog.Zero.Error().Err(err).Msg("")
		return nil, err
	}

	l := &LocalQrouter{
		ready:          &atomic.Bool{},
		rm:             &rmeta.RoutingMetadataContext{},
		metricRegistry: metricRegistry,
	}

	for k, v := range shardMapping {
		l.ds = topology.NewDataShard(k, v.Type, v.Options())
	}

	return l, nil
}

func (qr *LocalQrouter) Initialize() bool {
	return true
}

func (qr *LocalQrouter) Initialized() bool {
	return true
}

// TODO : unit tests
func (qr *LocalQrouter) AddDataShard(_ context.Context, ds *topology.DataShard) error {
	spqrlog.Zero.Debug().Str("shard", ds.ID).Msg("adding data shard")
	qr.ds = ds
	return nil
}

// TODO : unit tests
func (qr *LocalQrouter) PlanQuery(_ context.Context, rm *rmeta.RoutingMetadataContext) (plan.Plan, error) {
	return &plan.ShardDispatchPlan{
		PStmt: rm.Stmt,
		ExecTarget: kr.ShardKey{
			Name: qr.ds.ID,
		},
	}, nil
}

func (qr *LocalQrouter) SchemaCache() *cache.SchemaCache {
	return nil
}

// TODO : unit tests
func (qr *LocalQrouter) DataShardsRoutes() []kr.ShardKey {
	return []kr.ShardKey{
		{
			Name: qr.ds.ID,
		},
	}
}

func (qr *LocalQrouter) Mgr() meta.EntityMgr {
	return nil
}

func (qr *LocalQrouter) CSM() connmgr.ConnectionMgr {
	return nil
}

func (qr *LocalQrouter) SetQuery(_ *string) {}
func (qr *LocalQrouter) Query() *string     { return nil }

func (qr *LocalQrouter) MetricRegistry() *metrics.RouterMetricRegistry {
	return qr.metricRegistry
}
