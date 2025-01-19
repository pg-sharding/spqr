package plan

import (
	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/pkg/spqrlog"
)

type Plan interface {
	iPlan()
}

type ShardPlan struct {
	Plan
	Query string
}

type ScatterPlan struct {
	Plan
	SubPlan Plan
}

type ModifyTable struct {
	Plan
}

type DummyPlan struct {
	Plan
}

const NOSHARD = ""

// TODO : unit tests
func Combine(p1, p2 Plan) Plan {
	if p1 == nil && p2 == nil {
		return nil
	}
	if p1 == nil {
		return p2
	}
	if p2 == nil {
		return p1
	}

	spqrlog.Zero.Debug().
		Interface("plan1", p1).
		Interface("plan2", p2).
		Msg("combine two plans")

	switch shq1 := p1.(type) {
	case MultiMatchState:
		return p1
	case RandomMatchState:
		return p2
	case ReferenceRelationState:
		return p2
	case ShardMatchState:
		switch shq2 := p2.(type) {
		case MultiMatchState:
			return p2
		case ReferenceRelationState:
			return p1
		case ShardMatchState:
			if shq2.Route.Name == shq1.Route.Name {
				return p1
			}
		}
	}
	return MultiMatchState{}
}

type ShardMatchState struct {
	Plan

	Route              *kr.ShardKey
	TargetSessionAttrs string
}

type MultiMatchState struct {
	Plan
	DistributedPlan Plan
}

type DDLState struct {
	Plan
}

type SkipRoutingState struct {
	Plan
}

type RandomMatchState struct {
	Plan
}

type CopyState struct {
	Plan
}

type ReferenceRelationState struct {
	Plan
}
