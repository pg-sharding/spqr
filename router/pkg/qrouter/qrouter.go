package qrouter

import (
	"context"

	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/models/datashards"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/pkg/models/shrule"
	"github.com/pg-sharding/spqr/qdb"
	"github.com/pkg/errors"
	"golang.org/x/xerrors"
)

const NOSHARD = ""

type ShardRoute struct {
	Shkey     kr.ShardKey
	Matchedkr *kr.KeyRange
}

var MatchShardError = xerrors.New("failed to match datashard")

type RoutingState interface {
	iState()
}

type ShardMatchState struct {
	RoutingState

	Routes []*ShardRoute
}

type SkipRoutingState struct {
	RoutingState
}

type WolrdRouteState struct {
	RoutingState
}

type QueryRouter interface {
	kr.KeyRangeMgr
	shrule.ShardingRulesMgr

	// sharding rules
	Route(q string) (RoutingState, error)

	// do not use
	AddLocalTable(tname string) error

	// shards
	Shards() []string
	WorldShards() []string
	WorldShardsRoutes() []*ShardRoute

	AddDataShard(ctx context.Context, ds *datashards.DataShard) error
	ListDataShards(ctx context.Context) []*datashards.DataShard
	AddWorldShard(name string, cfg *config.ShardCfg) error

	Subscribe(krid string, keyRangeStatus *qdb.KeyRangeStatus, noitfyio chan<- interface{}) error
}

func NewQrouter(qtype config.QrouterType) (QueryRouter, error) {
	switch qtype {
	case config.LocalQrouter:
		return NewLocalQrouter(config.RouterConfig().QRouterCfg.LocalShard)
	case config.ProxyQrouter:
		return NewProxyRouter()
	default:
		return nil, errors.Errorf("unknown qrouter type: %v", config.RouterConfig().QRouterCfg.Qtype)
	}

}
