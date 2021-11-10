package qrouter

import (
	"github.com/pg-sharding/spqr/pkg/config"
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

var MatchShardError = xerrors.New("failed to match shard")

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

type Qrouter interface {
	kr.KeyRangeManager

	Route(q string) (RoutingState, error)

	// sharding rules
	AddShardingRule(shrule *shrule.ShardingRule) error
	ListShardingRules() []*shrule.ShardingRule
	// do not use
	AddLocalTable(tname string) error

	// shards
	Shards() []string
	WorldShards() []string
	WorldShardsRoutes() []*ShardRoute

	AddDataShard(name string, cfg *config.ShardCfg) error
	AddWorldShard(name string, cfg *config.ShardCfg) error

	Subscribe(krid string, keyRangeStatus *qdb.KeyRangeStatus, noitfyio chan<- interface{}) error
}

func NewQrouter(qtype config.QrouterType) (Qrouter, error) {
	switch qtype {
	case config.LocalQrouter:
		return NewLocalQrouter(config.RouterConfig().QRouterCfg.LocalShard)
	case config.ProxyQrouter:
		return NewProxyRouter()
	default:
		return nil, errors.Errorf("unknown qrouter type: %v", config.RouterConfig().QRouterCfg.Qtype)
	}

}
