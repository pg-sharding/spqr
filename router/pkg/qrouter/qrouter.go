package qrouter

import (
	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/qdb/qdb"
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

	Routes []ShardRoute
}

type SkipRoutingState struct {
	RoutingState
}

type WolrdRouteState struct {
	RoutingState
}

type ShardingRule struct {
	colunms []string
}

// local table sharding rule -> route to world

func NewShardingRule(cols []string) *ShardingRule {
	return &ShardingRule{
		colunms: cols,
	}
}

func (s *ShardingRule) Columns() []string {
	return s.colunms
}

type Qrouter interface {
	kr.KeyRangeManager

	Route(q string) (RoutingState, error)

	// sharding rules
	AddShardingRule(shrule *ShardingRule) error
	ListShardingRules() []*ShardingRule
	AddLocalTable(tname string) error

	// krs
	Shards() []string
	WorldShards() []string

	AddDataShard(name string, cfg *config.ShardCfg) error
	AddWorldShard(name string, cfg *config.ShardCfg) error

	Subscribe(krid string, keyRangeStatus *qdb.KeyRangeStatus, noitfyio chan<- interface{}) error
	WorldShardsRoutes() []ShardRoute
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
