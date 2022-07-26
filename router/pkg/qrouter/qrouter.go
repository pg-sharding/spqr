package qrouter

import (
	"context"

	"github.com/pg-sharding/spqr/pkg/meta"

	"github.com/jackc/pgproto3/v2"
	rparser "github.com/pg-sharding/spqr/router/pkg/parser"

	"github.com/pkg/errors"
	"golang.org/x/xerrors"

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

var MatchShardError = xerrors.New("failed to match datashard")

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
	meta.MetaMgr

	Route(ctx context.Context) (RoutingState, error)

	Parse(q *pgproto3.Query) (rparser.ParseState, error)
	WorldShardsRoutes() []*DataShardRoute
	DataShardsRoutes() []*DataShardRoute

	Subscribe(krid string, keyRangeStatus *qdb.KeyRangeStatus, noitfyio chan<- interface{}) error
}

func NewQrouter(qtype config.RouterMode, shardMapping map[string]*config.Shard) (QueryRouter, error) {
	switch qtype {
	case config.LocalMode:
		return NewLocalQrouter(shardMapping)
	case config.ProxyMode:
		return NewProxyRouter(shardMapping)
	default:
		return nil, errors.Errorf("unknown qrouter type: %v", config.RouterConfig().RouterMode)
	}
}
