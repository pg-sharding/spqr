package routingstate

import (
	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/pkg/spqrlog"
	"github.com/pg-sharding/spqr/router/plan"
)

const NOSHARD = ""

// TODO : unit tests
func Combine(sh1, sh2 RoutingState) RoutingState {
	if sh1 == nil && sh2 == nil {
		return nil
	}
	if sh1 == nil {
		return sh2
	}
	if sh2 == nil {
		return sh1
	}

	spqrlog.Zero.Debug().
		Interface("route1", sh1).
		Interface("route2", sh2).
		Msg("combine two routes")
	switch shq1 := sh1.(type) {
	case MultiMatchState:
		return sh2
	case RandomMatchState:
		return sh2
	case ReferenceRelationState:
		return sh2
	case ShardMatchState:
		switch shq2 := sh2.(type) {
		case MultiMatchState:
			return sh1
		case ReferenceRelationState:
			return sh1
		case ShardMatchState:
			if shq2.Route.Shkey.Name == shq1.Route.Shkey.Name {
				return sh1
			}
		}
	}
	return &MultiMatchState{}
}

type RoutingState interface {
	iState()
}

type DataShardRoute struct {
	Shkey     kr.ShardKey
	Matchedkr *kr.KeyRange
}

type ShardMatchState struct {
	RoutingState

	Route              *DataShardRoute
	TargetSessionAttrs string
}

type MultiMatchState struct {
	RoutingState
	DistributedPlan plan.Plan
}

type DDLState struct {
	RoutingState
}

type SkipRoutingState struct {
	RoutingState
}

type RandomMatchState struct {
	RoutingState
}

type CopyState struct {
	RoutingState
}

type WorldRouteState struct {
	RoutingState
}

type ReferenceRelationState struct {
	RoutingState
	DistributedPlan plan.Plan
}
