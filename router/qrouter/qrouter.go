package qrouter

import (
	"context"
	"fmt"

	"github.com/pg-sharding/lyx/lyx"
	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/connmgr"
	"github.com/pg-sharding/spqr/pkg/meta"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/pkg/session"
	"github.com/pg-sharding/spqr/router/cache"
	"github.com/pg-sharding/spqr/router/plan"
	"github.com/pg-sharding/spqr/router/planner"
)

type QueryRouter interface {
	PlanQuery(ctx context.Context, OriginQuery string, stmt lyx.Node, sph session.SessionParamsHolder) (plan.Plan, error)

	WorldShardsRoutes() []kr.ShardKey
	DataShardsRoutes() []kr.ShardKey

	Initialized() bool
	Initialize() bool

	Ready() bool
	SetReady(ready bool)

	Mgr() meta.EntityMgr
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
