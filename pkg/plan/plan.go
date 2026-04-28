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

	PrepareRunF func() error `json:"-"`

	RunF func(server.Server) error `json:"-"`

	OverwriteQuery map[string]string
	OverwriteCC    *pgproto3.CommandComplete
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

func (sp *ScatterPlan) GetGangMemberMsg(sh kr.ShardKey) string {
	if msg, ok := sp.OverwriteQuery[sh.Name]; ok {
		return msg
	}
	return ""
}

func (sp *ScatterPlan) Subplan() Plan {
	return sp.SubSlice
}

func (sp *ScatterPlan) PrepareRunSlice(_ server.Server) error {
	if sp.PrepareRunF != nil {
		/* prepare our slice */
		if err := sp.PrepareRunF(); err != nil {
			return err
		}
	}
	return nil
}

func (sp *ScatterPlan) RunSlice(serv server.Server) error {
	if sp.RunF == nil {
		return fmt.Errorf("execution failed, run function missing")
	}
	return sp.RunF(serv)
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

func (mt *ModifyTable) Stmt() lyx.Node {
	return mt.stmt
}

func (mt *ModifyTable) SetStmt(n lyx.Node) {
	mt.stmt = n
}

func (mt *ModifyTable) GetGangMemberMsg(kr.ShardKey) string {
	return ""
}

func (mt *ModifyTable) Subplan() Plan {
	return nil
}

func (mt *ModifyTable) RunSlice(server.Server) error {
	return fmt.Errorf("unexpected run function call")
}

func (mt *ModifyTable) PrepareRunSlice(server.Server) error {
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

func (sms *ShardDispatchPlan) Stmt() lyx.Node {
	return sms.PStmt
}

func (sms *ShardDispatchPlan) SetStmt(n lyx.Node) {
	sms.PStmt = n
}

func (sms *ShardDispatchPlan) GetGangMemberMsg(kr.ShardKey) string {
	return ""
}

func (sms *ShardDispatchPlan) Subplan() Plan {
	return sms.SP
}

func (sms *ShardDispatchPlan) PrepareRunSlice(server.Server) error {
	return nil
}

func (sms *ShardDispatchPlan) RunSlice(serv server.Server) error {
	if sms.runF == nil {
		return fmt.Errorf("execution failed, run function missing")
	}
	return sms.runF(serv)
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

func (rdp *RandomDispatchPlan) Stmt() lyx.Node {
	return rdp.stmt
}

func (rdp *RandomDispatchPlan) SetStmt(n lyx.Node) {
	rdp.stmt = n
}

func (rdp *RandomDispatchPlan) GetGangMemberMsg(kr.ShardKey) string {
	return ""
}

func (rdp *RandomDispatchPlan) Subplan() Plan {
	return nil
}

func (rdp *RandomDispatchPlan) RunSlice(server.Server) error {
	return fmt.Errorf("unexpected run function call")
}

func (rdp *RandomDispatchPlan) PrepareRunSlice(server.Server) error {
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

func (vp *VirtualPlan) Stmt() lyx.Node {
	return vp.stmt
}

func (vp *VirtualPlan) SetStmt(n lyx.Node) {
	vp.stmt = n
}

func (vp *VirtualPlan) GetGangMemberMsg(kr.ShardKey) string {
	return ""
}

func (vp *VirtualPlan) Subplan() Plan {
	return vp.SubPlan
}

func (vp *VirtualPlan) RunSlice(server.Server) error {
	return fmt.Errorf("unexpected run function call")
}

func (vp *VirtualPlan) PrepareRunSlice(server.Server) error {
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

func (rf *DataRowFilter) Stmt() lyx.Node {
	return rf.stmt
}

func (rf *DataRowFilter) SetStmt(n lyx.Node) {
	rf.stmt = n
}

func (rf *DataRowFilter) GetGangMemberMsg(sh kr.ShardKey) string {
	if rf.SubPlan == nil {
		return ""
	}
	return rf.SubPlan.GetGangMemberMsg(sh)
}

func (rf *DataRowFilter) Subplan() Plan {
	return rf.SubPlan.Subplan()
}

func (rf *DataRowFilter) RunSlice(serv server.Server) error {
	return rf.SubPlan.RunSlice(serv)
}

func (rf *DataRowFilter) PrepareRunSlice(serv server.Server) error {
	return rf.SubPlan.PrepareRunSlice(serv)
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

func (cs *CopyPlan) Stmt() lyx.Node {
	return cs.stmt
}

func (cs *CopyPlan) SetStmt(n lyx.Node) {
	cs.stmt = n
}

func (cs *CopyPlan) GetGangMemberMsg(kr.ShardKey) string {
	return ""
}

func (cs *CopyPlan) Subplan() Plan {
	return nil
}

func (cs *CopyPlan) RunSlice(server.Server) error {
	return fmt.Errorf("unexpected run function call")
}

func (cs *CopyPlan) PrepareRunSlice(server.Server) error {
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
		case *RandomDispatchPlan:
			return p1
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
