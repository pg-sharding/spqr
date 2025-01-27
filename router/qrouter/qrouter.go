package qrouter

import (
	"context"
	"fmt"

	"github.com/pg-sharding/spqr/pkg/models/kr"

	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/meta"
	"github.com/pg-sharding/spqr/pkg/session"
	"github.com/pg-sharding/spqr/router/cache"
	"github.com/pg-sharding/spqr/router/plan"
	"github.com/pkg/errors"

	"github.com/pg-sharding/lyx/lyx"
)

var MatchShardError = fmt.Errorf("failed to match datashard")

type QueryRouter interface {
	Route(ctx context.Context, stmt lyx.Node, sph session.SessionParamsHolder) (plan.Plan, error)

	WorldShardsRoutes() []*kr.ShardKey
	DataShardsRoutes() []*kr.ShardKey

	Initialized() bool
	Initialize() bool

	Mgr() meta.EntityMgr
	SchemaCache() *cache.SchemaCache
}

func NewQrouter(qtype config.RouterMode, shardMapping map[string]*config.Shard, mgr meta.EntityMgr, qcfg *config.QRouter, cache *cache.SchemaCache) (QueryRouter, error) {
	switch qtype {
	case config.LocalMode:
		return NewLocalQrouter(shardMapping)
	case config.ProxyMode:
		return NewProxyRouter(shardMapping, mgr, qcfg, cache)
	default:
		return nil, errors.Errorf("unknown qrouter type: %v", qtype)
	}
}
