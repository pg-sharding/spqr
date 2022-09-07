package qrouter

import (
	"context"
	"fmt"

	"github.com/pg-sharding/spqr/pkg/meta"

	"github.com/jackc/pgproto3/v2"
	"github.com/pg-sharding/spqr/router/pkg/parser"

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

	Route(ctx context.Context) (RoutingState, error)

	Parse(q *pgproto3.Query) (parser.ParseState, error)
	WorldShardsRoutes() []*DataShardRoute
	DataShardsRoutes() []*DataShardRoute

	Initialized() bool
	Initialize() bool

	Subscribe(krid string, keyRangeStatus *qdb.KeyRangeStatus, noitfyio chan<- interface{}) error
}

func NewQrouter(qtype config.RouterMode, shardMapping map[string]*config.Shard) (QueryRouter, error) {
	switch qtype {
	case config.LocalMode:
		return NewLocalQrouter(shardMapping)
	case config.ProxyMode:
		return NewProxyRouter(shardMapping)
	default:
		return nil, errors.Errorf("unknown qrouter type: %v", qtype)
	}
}
