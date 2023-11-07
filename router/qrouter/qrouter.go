package qrouter

import (
	"context"
	"fmt"

	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/meta"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/pkg/spqrlog"
	"github.com/pkg/errors"

	"github.com/pg-sharding/lyx/lyx"
)

const NOSHARD = ""

type ShardRoute interface {
}

func combine(sh1, sh2 ShardRoute) ShardRoute {
	spqrlog.Zero.Debug().
		Interface("route1", sh1).
		Interface("route2", sh2).
		Msg("combine two routes")
	switch shq1 := sh1.(type) {
	case *MultiMatchRoute:
		return sh2
	case *DataShardRoute:
		switch shq2 := sh2.(type) {
		case *MultiMatchRoute:
			return sh1
		case *DataShardRoute:
			if shq2.Shkey.Name == shq1.Shkey.Name {
				return sh1
			}
		}
	}
	return &MultiMatchRoute{}
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

	Routes             []*DataShardRoute
	TargetSessionAttrs string
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
	Route(ctx context.Context, stmt lyx.Node, dataspace string, params [][]byte) (RoutingState, error)

	WorldShardsRoutes() []*DataShardRoute
	DataShardsRoutes() []*DataShardRoute

	Initialized() bool
	Initialize() bool
}

func NewQrouter(qtype config.RouterMode, shardMapping map[string]*config.Shard, mgr meta.EntityMgr, qcfg *config.QRouter) (QueryRouter, error) {
	switch qtype {
	case config.LocalMode:
		return NewLocalQrouter(shardMapping)
	case config.ProxyMode:
		return NewProxyRouter(shardMapping, mgr, qcfg)
	default:
		return nil, errors.Errorf("unknown qrouter type: %v", qtype)
	}
}
