package routingstate

import (
	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/pkg/spqrlog"
)

const NOSHARD = ""

type ShardRoute interface {
}

func Combine(sh1, sh2 ShardRoute) ShardRoute {
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
