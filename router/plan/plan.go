package plan

import (
	"sort"

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
	/* Empty means execute everywhere */
	ExecutionTargets []*kr.ShardKey
}

type ModifyTable struct {
	Plan
}

const NOSHARD = ""

func CombineExecutionTargets(p1, p2 []*kr.ShardKey) []*kr.ShardKey {
	res := append(p1, p2...)
	sort.Slice(res, func(i, j int) bool {
		return res[i].Name < res[j].Name
	})

	var ret []*kr.ShardKey
	for i := range res {
		if i == 0 {
			ret = append(ret, res[i])
			continue
		}
		if res[i].Name != res[i-1].Name {

			ret = append(ret, res[i])
		}
	}

	return ret
}

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
	case ScatterPlan:
		return p1
	case RandomShardScan:
		return p2
	case ReferenceRelationState:
		return p2
	case ShardMatchState:
		switch shq2 := p2.(type) {
		case ScatterPlan:
			return p2
		case ReferenceRelationState:
			return p1
		case ShardMatchState:
			if shq2.Route.Name == shq1.Route.Name {
				return p1
			}
		}
	}

	/* execute on all shards */
	return ScatterPlan{}
}

type ShardMatchState struct {
	Plan

	Route              *kr.ShardKey
	TargetSessionAttrs string
}

type DDLState struct {
	Plan
}

type SkipRoutingState struct {
	Plan
}

type RandomShardScan struct {
	Plan
}

type CopyState struct {
	Plan
}

type ReferenceRelationState struct {
	Plan
}
