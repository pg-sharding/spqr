package qrouter

import (
	"context"
	"fmt"

	"github.com/pg-sharding/lyx/lyx"
	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/connmgr"
	"github.com/pg-sharding/spqr/pkg/meta"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/pkg/models/topology"
	"github.com/pg-sharding/spqr/pkg/plan"
	"github.com/pg-sharding/spqr/pkg/session"
	"github.com/pg-sharding/spqr/router/cache"
	"github.com/pg-sharding/spqr/router/planner"
	"github.com/pg-sharding/spqr/router/rmeta"
)

type QueryRouter interface {
	AnalyzeQuery(ctx context.Context,
		sph session.SessionParamsHolder, rule *config.FrontendRule, query string, stmt lyx.Node) (*rmeta.RoutingMetadataContext, error)
	PlanQuery(ctx context.Context, rm *rmeta.RoutingMetadataContext) (plan.Plan, error)

	DataShardsRoutes() []kr.ShardKey

	Initialized() bool
	Initialize() bool

	Ready() bool
	SetReady(ready bool)

	IdRange() planner.IdentityRouterCache

	Mgr() meta.EntityMgr
	CSM() connmgr.ConnectionMgr
	SchemaCache() *cache.SchemaCache
}

func NewQrouter(qtype config.RouterMode,
	tmgr topology.TopologyMgr,
	mgr meta.EntityMgr,
	csm connmgr.ConnectionMgr,
	qcfg *config.QRouter,
	cache *cache.SchemaCache,
	idRangeCache planner.IdentityRouterCache,
) (QueryRouter, error) {
	switch qtype {
	case config.LocalMode:
		return NewLocalQrouter(tmgr.Snap())
	case config.ProxyMode:
		return NewProxyRouter(tmgr, mgr, csm, qcfg, cache, idRangeCache)
	default:
		return nil, fmt.Errorf("unknown qrouter type: %v", qtype)
	}
}
