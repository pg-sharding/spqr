package plan

import (
	"fmt"
	"maps"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/pg-sharding/lyx/lyx"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/pkg/spqrlog"
	"github.com/pg-sharding/spqr/pkg/tsa"
	"github.com/pg-sharding/spqr/pkg/tupleslot"
	"github.com/pg-sharding/spqr/router/server"
)

type Plan interface {
	Stmt() lyx.Node
	SetStmt(lyx.Node)

	ExecutionTargets() []kr.ShardKey
	GetGangMemberMsg(sh kr.ShardKey) string

	/* get SubPlan if any */

	RunSlice(server.Server) error
	PrepareRunSlice(server.Server) error

	Subplan() Plan
}

type ScatterPlan struct {
	Plan
	SubPlan Plan

	/* explicitly set-up link to next slice */
	SubSlice Plan

	stmt lyx.Node

	/* To decide if query is OK even in DRH = BLOCK */
	IsDDL  bool
	Forced bool

	PrepareRunF func() error

	RunF func(server.Server) error

	OverwriteQuery map[string]string
	/* Empty means execute everywhere */
	ExecTargets []kr.ShardKey
}

func (sp *ScatterPlan) ExecutionTargets() []kr.ShardKey {
	if sp.ExecTargets == nil {
		if sp.SubSlice == nil {
			return nil
		}
		return sp.Subplan().ExecutionTargets()
	}

	return sp.ExecTargets
}

func (sp *ScatterPlan) Stmt() lyx.Node {
	return sp.stmt
}

func (sp *ScatterPlan) SetStmt(n lyx.Node) {
	sp.stmt = n
}

func (s *ScatterPlan) GetGangMemberMsg(sh kr.ShardKey) string {
	if msg, ok := s.OverwriteQuery[sh.Name]; ok {
		return msg
	}
	return ""
}

func (s *ScatterPlan) Subplan() Plan {
	return s.SubSlice
}

func (s *ScatterPlan) PrepareRunSlice(serv server.Server) error {
	if s.PrepareRunF != nil {
		/* prepare our slice */
		if err := s.PrepareRunF(); err != nil {
			return err
		}
	}
	return nil
}

func (s *ScatterPlan) RunSlice(serv server.Server) error {
	if s.RunF == nil {
		return fmt.Errorf("execution failed, run function missing")
	}
	return s.RunF(serv)
}

var _ Plan = &ScatterPlan{}

type ModifyTable struct {
	Plan
	stmt        lyx.Node
	ExecTargets []kr.ShardKey
}

func (mt *ModifyTable) ExecutionTargets() []kr.ShardKey {
	return mt.ExecTargets
}

func (sp *ModifyTable) Stmt() lyx.Node {
	return sp.stmt
}

func (sp *ModifyTable) SetStmt(n lyx.Node) {
	sp.stmt = n
}

func (s *ModifyTable) GetGangMemberMsg(kr.ShardKey) string {
	return ""
}

func (s *ModifyTable) Subplan() Plan {
	return nil
}

func (s *ModifyTable) RunSlice(server.Server) error {
	return fmt.Errorf("unexpected run function call")
}

func (s *ModifyTable) PrepareRunSlice(server.Server) error {
	return fmt.Errorf("unexpected run function call")
}

var _ Plan = &ModifyTable{}

type ShardDispatchPlan struct {
	Plan

	/* Subplan */

	SP   Plan
	runF func(server.Server) error

	PStmt              lyx.Node
	ExecTarget         kr.ShardKey
	TargetSessionAttrs tsa.TSA
}

func (sms *ShardDispatchPlan) ExecutionTargets() []kr.ShardKey {
	return []kr.ShardKey{sms.ExecTarget}
}

func (sp *ShardDispatchPlan) Stmt() lyx.Node {
	return sp.PStmt
}

func (sp *ShardDispatchPlan) SetStmt(n lyx.Node) {
	sp.PStmt = n
}

func (s *ShardDispatchPlan) GetGangMemberMsg(kr.ShardKey) string {
	return ""
}

func (s *ShardDispatchPlan) Subplan() Plan {
	return s.SP
}

func (s *ShardDispatchPlan) PrepareRunSlice(server.Server) error {
	return nil
}

func (s *ShardDispatchPlan) RunSlice(serv server.Server) error {
	if s.runF == nil {
		return fmt.Errorf("execution failed, run function missing")
	}
	return s.runF(serv)
}

var _ Plan = &ShardDispatchPlan{}

type RandomDispatchPlan struct {
	Plan

	stmt        lyx.Node
	ExecTargets []kr.ShardKey
}

func (rdp *RandomDispatchPlan) ExecutionTargets() []kr.ShardKey {
	return rdp.ExecTargets
}

func (sp *RandomDispatchPlan) Stmt() lyx.Node {
	return sp.stmt
}

func (sp *RandomDispatchPlan) SetStmt(n lyx.Node) {
	sp.stmt = n
}

func (s *RandomDispatchPlan) GetGangMemberMsg(kr.ShardKey) string {
	return ""
}

func (s *RandomDispatchPlan) Subplan() Plan {
	return nil
}

func (s *RandomDispatchPlan) RunSlice(server.Server) error {
	return fmt.Errorf("unexpected run function call")
}

func (s *RandomDispatchPlan) PrepareRunSlice(server.Server) error {
	return fmt.Errorf("unexpected run function call")
}

var _ Plan = &RandomDispatchPlan{}

type VirtualPlan struct {
	Plan

	stmt lyx.Node

	OverwriteCC *pgproto3.CommandComplete
	TTS         *tupleslot.TupleTableSlot
	SubPlan     Plan
}

func (vp *VirtualPlan) ExecutionTargets() []kr.ShardKey {
	if vp.SubPlan == nil {
		return nil
	}
	return vp.SubPlan.ExecutionTargets()
}

func (sp *VirtualPlan) Stmt() lyx.Node {
	return sp.stmt
}

func (sp *VirtualPlan) SetStmt(n lyx.Node) {
	sp.stmt = n
}

func (s *VirtualPlan) GetGangMemberMsg(kr.ShardKey) string {
	return ""
}

func (s *VirtualPlan) Subplan() Plan {
	return s.SubPlan
}

func (s *VirtualPlan) RunSlice(server.Server) error {
	return fmt.Errorf("unexpected run function call")
}

func (s *VirtualPlan) PrepareRunSlice(server.Server) error {
	return fmt.Errorf("unexpected run function call")
}

var _ Plan = &VirtualPlan{}

type DataRowFilter struct {
	Plan

	stmt        lyx.Node
	FilterIndex uint
	SubPlan     Plan
}

func (rf *DataRowFilter) ExecutionTargets() []kr.ShardKey {
	return rf.SubPlan.ExecutionTargets()
}

func (sp *DataRowFilter) Stmt() lyx.Node {
	return sp.stmt
}

func (sp *DataRowFilter) SetStmt(n lyx.Node) {
	sp.stmt = n
}

func (s *DataRowFilter) GetGangMemberMsg(sh kr.ShardKey) string {
	if s.SubPlan == nil {
		return ""
	}
	return s.SubPlan.GetGangMemberMsg(sh)
}

func (s *DataRowFilter) Subplan() Plan {
	return s.SubPlan.Subplan()
}

func (s *DataRowFilter) RunSlice(serv server.Server) error {
	return s.SubPlan.RunSlice(serv)
}

func (s *DataRowFilter) PrepareRunSlice(serv server.Server) error {
	return s.SubPlan.PrepareRunSlice(serv)
}

var _ Plan = &DataRowFilter{}

type CopyPlan struct {
	Plan

	stmt        lyx.Node
	ExecTargets []kr.ShardKey
}

func (cs *CopyPlan) ExecutionTargets() []kr.ShardKey {
	return cs.ExecTargets
}

func (sp *CopyPlan) Stmt() lyx.Node {
	return sp.stmt
}

func (sp *CopyPlan) SetStmt(n lyx.Node) {
	sp.stmt = n
}

func (s *CopyPlan) GetGangMemberMsg(kr.ShardKey) string {
	return ""
}

func (s *CopyPlan) Subplan() Plan {
	return nil
}

func (s *CopyPlan) RunSlice(server.Server) error {
	return fmt.Errorf("unexpected run function call")
}

func (s *CopyPlan) PrepareRunSlice(server.Server) error {
	return fmt.Errorf("unexpected run function call")
}

var _ Plan = &CopyPlan{}

const NOSHARD = ""

/* XXX: check subplan here? */
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
