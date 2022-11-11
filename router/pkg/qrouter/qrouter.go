package qrouter

import (
	"context"
	"fmt"

	"github.com/jackc/pgproto3/v2"
	"github.com/pg-sharding/spqr/pkg/meta"
	"github.com/pg-sharding/spqr/router/pkg/parser"
	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/models/kr"
)

var ErrMatchShardError = fmt.Errorf("failed to match datashard")

type ShardRoute interface {
}

type DataShardRoute struct {
	ShardRoute

	Shkey     kr.ShardKey
	Matchedkr *kr.KeyRange
}


type RoutingState interface {
	iState()
}

type ShardMatchState struct {
	RoutingState

	Routes []*DataShardRoute
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

	Route(ctx context.Context) (RoutingState, error)

	Parse(q *pgproto3.Query) (parser.ParseState, error)
	WorldShardsRoutes() []*DataShardRoute
	DataShardsRoutes() []*DataShardRoute

	Initialized() bool
	Initialize() bool
}

func NewQrouter(qtype config.RouterMode, shardMapping map[string]*config.Shard) (QueryRouter, error) {
	switch qtype {
	case config.LocalMode:
		return NewLocalQrouter(shardMapping)
	case config.ProxyMode:
		return NewProxyRouter(shardMapping)
	default:
		return nil, fmt.Errorf("unknown qrouter type: %v", qtype)
	}
}
