package plan

import (
	"maps"

	"github.com/pg-sharding/lyx/lyx"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/pkg/spqrlog"
	"github.com/pg-sharding/spqr/pkg/tsa"
	"github.com/pg-sharding/spqr/pkg/tupleslot"
)

type Plan interface {
	ExecutionTargets() []kr.ShardKey
	GetGangMemberMsg(sh kr.ShardKey) string
}

type ScatterPlan struct {
	Plan
	SubPlan Plan

	Stmt lyx.Node

	/* To decide if query is OK even in DRH = BLOCK */
	IsDDL  bool
	Forced bool
	IsCopy bool

	OverwriteQuery map[string]string
	/* Empty means execute everywhere */
	ExecTargets []kr.ShardKey
}

func (sp *ScatterPlan) ExecutionTargets() []kr.ShardKey {
	return sp.ExecTargets
}

func (s *ScatterPlan) GetGangMemberMsg(sh kr.ShardKey) string {
	if msg, ok := s.OverwriteQuery[sh.Name]; ok {
		return msg
	}
	return ""
}

var _ Plan = &ScatterPlan{}

type ModifyTable struct {
	Plan
	ExecTargets []kr.ShardKey
}

func (mt *ModifyTable) ExecutionTargets() []kr.ShardKey {
	return mt.ExecTargets
}

func (s *ModifyTable) GetGangMemberMsg(kr.ShardKey) string {
	return ""
}

var _ Plan = &ModifyTable{}

type ShardDispatchPlan struct {
	Plan

	ExecTarget         kr.ShardKey
	TargetSessionAttrs tsa.TSA
}

func (sms *ShardDispatchPlan) ExecutionTargets() []kr.ShardKey {
	return []kr.ShardKey{sms.ExecTarget}
}

func (s *ShardDispatchPlan) GetGangMemberMsg(kr.ShardKey) string {
	return ""
}

var _ Plan = &ShardDispatchPlan{}

type RandomDispatchPlan struct {
	Plan

	ExecTargets []kr.ShardKey
}

func (rdp *RandomDispatchPlan) ExecutionTargets() []kr.ShardKey {
	return rdp.ExecTargets
}

func (s *RandomDispatchPlan) GetGangMemberMsg(kr.ShardKey) string {
	return ""
}

var _ Plan = &RandomDispatchPlan{}

type VirtualPlan struct {
	Plan

	TTS     *tupleslot.TupleTableSlot
	SubPlan Plan
}

func (vp *VirtualPlan) ExecutionTargets() []kr.ShardKey {
	return nil
}

func (s *VirtualPlan) GetGangMemberMsg(kr.ShardKey) string {
	return ""
}

var _ Plan = &VirtualPlan{}

type DataRowFilter struct {
	Plan

	FilterIndex uint
	SubPlan     Plan
}

func (rf *DataRowFilter) ExecutionTargets() []kr.ShardKey {
	return rf.SubPlan.ExecutionTargets()
}

func (s *DataRowFilter) GetGangMemberMsg(sh kr.ShardKey) string {
	if s.SubPlan == nil {
		return ""
	}
	return s.SubPlan.GetGangMemberMsg(sh)
}

var _ Plan = &DataRowFilter{}

const NOSHARD = ""

func mergeExecTargets(l, r []kr.ShardKey) []kr.ShardKey {
	/* XXX: nil means all */
	if l == nil {
		return nil
	}
	/* XXX: nil means all */
	if r == nil {
		return nil
	}
	ret := l

	rightMap := map[string]struct{}{}
	for _, e := range l {
		rightMap[e.Name] = struct{}{}
	}

	for _, e := range r {
		if _, ok := rightMap[e.Name]; ok {
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

	switch v := p1.(type) {
	case *DataRowFilter:
		return &DataRowFilter{
			SubPlan: Combine(v.SubPlan, p2),
		}
	}

	switch v := p2.(type) {
	// let p2 be always non-virtual, except for p1 & p2 both virtual
	case *VirtualPlan:
		p1, p2 = p2, p1
	case *DataRowFilter:
		return &DataRowFilter{
			SubPlan: Combine(p1, v.SubPlan),
		}
	}

	switch shq1 := p1.(type) {
	case *VirtualPlan:
		return p2
	case *ScatterPlan:
		merged := make(map[string]string)
		maps.Copy(merged, shq1.OverwriteQuery)
		// XXX: is this bad?
		switch shq2 := p2.(type) {
		case *ScatterPlan:
			maps.Copy(merged, shq2.OverwriteQuery)
		}

		return &ScatterPlan{
			OverwriteQuery: merged,
			ExecTargets:    mergeExecTargets(p1.ExecutionTargets(), p2.ExecutionTargets()),
		}
	case *RandomDispatchPlan:
		return p2
	case *ShardDispatchPlan:
		switch shq2 := p2.(type) {
		case *RandomDispatchPlan:
			return p1
		case *ScatterPlan:
			return &ScatterPlan{
				ExecTargets: mergeExecTargets(p1.ExecutionTargets(), p2.ExecutionTargets()),
			}
		case *ShardDispatchPlan:
			if shq2.ExecTarget.Name == shq1.ExecTarget.Name {
				return p1
			} else {
				return &ScatterPlan{
					ExecTargets: mergeExecTargets(p1.ExecutionTargets(), p2.ExecutionTargets()),
				}
			}
		}
	}

	/* execute on all shards */
	return &ScatterPlan{}
}
