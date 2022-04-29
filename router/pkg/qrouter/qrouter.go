package qrouter

import (
	"context"
	rparser "github.com/pg-sharding/spqr/router/pkg/parser"

	"github.com/pkg/errors"
	"golang.org/x/xerrors"

	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/models/datashards"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/pkg/models/shrule"
	"github.com/pg-sharding/spqr/qdb"
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

type WorldRouteState struct {
	RoutingState
}

type QueryRouter interface {
	kr.KeyRangeMgr
	shrule.ShardingRulesMgr

	Route(stmt rparser.QParser) (RoutingState, error)

	// AddLocalTable do not use
	AddLocalTable(tname string) error

	// Shards shards
	Shards() []string
	WorldShards() []string
	WorldShardsRoutes() []*ShardRoute

	AddDataShard(ctx context.Context, ds *datashards.DataShard) error
	ListDataShards(ctx context.Context) []*datashards.DataShard
	AddWorldShard(name string, cfg *config.ShardCfg) error

	Subscribe(krid string, keyRangeStatus *qdb.KeyRangeStatus, noitfyio chan<- interface{}) error
}

func NewQrouter(qtype config.QrouterType, rules config.RulesCfg) (QueryRouter, error) {
	switch qtype {
	case config.LocalQrouter:
		return NewLocalQrouter(rules)
	case config.ProxyQrouter:
		return NewProxyRouter(rules)
	default:
		return nil, errors.Errorf("unknown qrouter type: %v", config.RouterConfig().QRouterCfg.Qtype)
	}
}
