package qrouter

import (
	"context"
	"fmt"

	"github.com/pg-sharding/spqr/pkg/meta"

	pgquery "github.com/pganalyze/pg_query_go/v2"
	"github.com/pkg/errors"

	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/qdb"
)

const NOSHARD = ""

type ShardRoute interface {
}

type DataShardRoute struct {
	ShardRoute

	Shkey     kr.ShardKey
	Matchedkr *kr.KeyRange
}

var MatchShardError = fmt.Errorf("failed to match datashard")

type RoutingState interface {
	iState()
}

type ShardMatchState struct {
	RoutingState

	Routes []*DataShardRoute
	keys   []*kr.KeyRange
}

type MultiMatchRoute struct {
	ShardRoute
}

type MultiMatchState struct {
	RoutingState
}

type SkipRoutingState struct {
	RoutingState
}

type WorldRouteState struct {
	RoutingState
}

type QueryRouter interface {
	meta.EntityMgr

	Route(ctx context.Context, stmt *pgquery.ParseResult) (RoutingState, error)

	WorldShardsRoutes() []*DataShardRoute
	DataShardsRoutes() []*DataShardRoute

	Initialized() bool
	Initialize() bool

	Subscribe(krid string, keyRangeStatus *qdb.KeyRangeStatus, noitfyio chan<- interface{}) error
}

func NewQrouter(qtype config.RouterMode, shardMapping map[string]*config.Shard, qcfg *config.QRouter) (QueryRouter, error) {
	switch qtype {
	case config.LocalMode:
		return NewLocalQrouter(shardMapping)
	case config.ProxyMode:
		return NewProxyRouter(shardMapping, qcfg)
	default:
		return nil, errors.Errorf("unknown qrouter type: %v", qtype)
	}
}
