package plan

import (
	"slices"

	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/pkg/spqrlog"
)

type Plan interface {
	iPlan()
	ExecutionTargets() []*kr.ShardKey
}

type ScatterPlan struct {
	Plan
	SubPlan Plan
	/* Empty means execute everywhere */
	ExecTargets []*kr.ShardKey
}

func (sp ScatterPlan) ExecutionTargets() []*kr.ShardKey {
	return sp.ExecTargets
}

type ModifyTable struct {
	Plan
}

func (mt ModifyTable) ExecutionTargets() []*kr.ShardKey {
	return nil
}

type ShardDispatchPlan struct {
	Plan

	ExecTarget         *kr.ShardKey
	TargetSessionAttrs string
}

func (sms ShardDispatchPlan) ExecutionTargets() []*kr.ShardKey {
	return []*kr.ShardKey{sms.ExecTarget}
}

type DDLState struct {
	Plan
}

func (ddl DDLState) ExecutionTargets() []*kr.ShardKey {
	return nil
}

type RandomDispatchPlan struct {
	Plan
}

func (rdp RandomDispatchPlan) ExecutionTargets() []*kr.ShardKey {
	return nil
}

type VirtualPlan struct {
	Plan
}

func (vp VirtualPlan) ExecutionTargets() []*kr.ShardKey {
	return nil
}

type CopyState struct {
	Plan
}

func (cs CopyState) ExecutionTargets() []*kr.ShardKey {
	return nil
}

type ReferenceRelationState struct {
	Plan
}

func (rrs ReferenceRelationState) ExecutionTargets() []*kr.ShardKey {
	return nil
}

const NOSHARD = ""

func mergeExecTargets(l, r []*kr.ShardKey) []*kr.ShardKey {
	/* XXX: nil means all */
	if l == nil {
		return nil
	}
	/* XXX: nil means all */
	if r == nil {
		return nil
	}
	ret := l

	for _, e := range r {
		if slices.ContainsFunc[[]*kr.ShardKey](ret, func(
			el *kr.ShardKey,
		) bool {
			return e.Name == el.Name
		}) {
			continue
		}
		ret = append(ret, e)
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
		return ScatterPlan{
			ExecTargets: mergeExecTargets(p1.ExecutionTargets(), p2.ExecutionTargets()),
		}
	case RandomDispatchPlan:
		return p2
	case ReferenceRelationState:
		return p2
	case ShardDispatchPlan:
		switch shq2 := p2.(type) {
		case ScatterPlan:
			return ScatterPlan{
				ExecTargets: mergeExecTargets(p1.ExecutionTargets(), p2.ExecutionTargets()),
			}
		case ReferenceRelationState:
			return p1
		case ShardDispatchPlan:
			if shq2.ExecTarget.Name == shq1.ExecTarget.Name {
				return p1
			} else {
				return ScatterPlan{
					ExecTargets: mergeExecTargets(p1.ExecutionTargets(), p2.ExecutionTargets()),
				}
			}
		}
	}

	/* execute on all shards */
	return ScatterPlan{}
}
