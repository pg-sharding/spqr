package qrouter

import (
	"context"
	"fmt"

	"github.com/pg-sharding/lyx/lyx"
	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/connmgr"
	"github.com/pg-sharding/spqr/pkg/meta"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/pkg/plan"
	"github.com/pg-sharding/spqr/pkg/session"
	"github.com/pg-sharding/spqr/router/cache"
	"github.com/pg-sharding/spqr/router/planner"
	"github.com/pg-sharding/spqr/router/rmeta"
)

type QueryRouter interface {
	AnalyzeQuery(ctx context.Context,
		sph session.SessionParamsHolder, query string, stmt lyx.Node) (*rmeta.RoutingMetadataContext, error)
	PlanQuery(ctx context.Context, rm *rmeta.RoutingMetadataContext) (plan.Plan, error)

	WorldShardsRoutes() []kr.ShardKey
	DataShardsRoutes() []kr.ShardKey

	Initialized() bool
	Initialize() bool

	Ready() bool
	SetReady(ready bool)

	IdRange() planner.IdentityRouterCache

	Mgr() meta.EntityMgr
	CSM() connmgr.ConnectionStatMgr
	SchemaCache() *cache.SchemaCache
}

func NewQrouter(qtype config.RouterMode,
	shardMapping map[string]*config.Shard,
	mgr meta.EntityMgr,
	csm connmgr.ConnectionStatMgr,
	qcfg *config.QRouter,
	cache *cache.SchemaCache,
	idRangeCache planner.IdentityRouterCache,
) (QueryRouter, error) {
	switch qtype {
	case config.LocalMode:
		return NewLocalQrouter(shardMapping)
	case config.ProxyMode:
		return NewProxyRouter(shardMapping, mgr, csm, qcfg, cache, idRangeCache)
	default:
		return nil, fmt.Errorf("unknown qrouter type: %v", qtype)
	}
}
