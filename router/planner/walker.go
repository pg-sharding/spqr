package planner

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/pg-sharding/lyx/lyx"
	"github.com/pg-sharding/spqr/pkg/catalog"
	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/plan"
	"github.com/pg-sharding/spqr/pkg/tupleslot"
	"github.com/pg-sharding/spqr/router/rmeta"
	"github.com/pg-sharding/spqr/router/virtual"
)

func PlanTargetList(ctx context.Context, rm *rmeta.RoutingMetadataContext, plr QueryPlanner, stmt *lyx.Select) (plan.Plan, error) {
	virtualRowCols := []pgproto3.FieldDescription{}
	virtualRowVals := [][]byte{}

	var p plan.Plan

	for _, expr := range stmt.TargetList {
		actualExpr := expr
		colname := "?column?"
		if rt, ok := expr.(*lyx.ResTarget); ok {
			actualExpr = rt.Value
			colname = rt.Name
		}

		switch e := actualExpr.(type) {
		case *lyx.SVFOP_CURRENT_USER:
			p = plan.Combine(p, &plan.VirtualPlan{})
			virtualRowCols = append(virtualRowCols,
				pgproto3.FieldDescription{
					Name:                 []byte(colname),
					DataTypeOID:          catalog.TEXTOID,
					TypeModifier:         -1,
					DataTypeSize:         -1,
					TableAttributeNumber: 0,
					TableOID:             0,
					Format:               0,
				})

			virtualRowVals = append(virtualRowVals, []byte(rm.SPH.Usr()))

		case *lyx.AExprNot:
			/* inspect our arg. If this is pg_is_in_recovery, apply NOT */
			switch arg := e.Arg.(type) {
			case *lyx.FuncApplication:
				if arg.Name == "pg_is_in_recovery" {
					p = plan.Combine(p, &plan.VirtualPlan{})
					virtualRowCols = append(virtualRowCols,
						pgproto3.FieldDescription{
							Name:                 []byte("pg_is_in_recovery"),
							DataTypeOID:          catalog.ARRAYOID,
							TypeModifier:         -1,
							DataTypeSize:         1,
							TableAttributeNumber: 0,
							TableOID:             0,
							Format:               0,
						})

					/* notice this sign */
					if rm.SPH.GetTsa() != config.TargetSessionAttrsRW {
						virtualRowVals = append(virtualRowVals, []byte{byte('f')})
					} else {
						virtualRowVals = append(virtualRowVals, []byte{byte('t')})
					}
					continue
				}
			}
		/* Special cases for SELECT current_schema(), SELECT set_config(...), and SELECT pg_is_in_recovery() */
		case *lyx.FuncApplication:

			/* for queries, that need to access data on shard, ignore these "virtual" func.	 */
			if e.Name == "pg_is_in_recovery" {
				p = plan.Combine(p, &plan.VirtualPlan{})
				virtualRowCols = append(virtualRowCols,
					pgproto3.FieldDescription{
						Name:                 []byte("pg_is_in_recovery"),
						DataTypeOID:          catalog.ARRAYOID,
						TypeModifier:         -1,
						DataTypeSize:         1,
						TableAttributeNumber: 0,
						TableOID:             0,
						Format:               0,
					})

				if rm.SPH.GetTsa() == config.TargetSessionAttrsRW {
					virtualRowVals = append(virtualRowVals, []byte{byte('f')})
				} else {
					virtualRowVals = append(virtualRowVals, []byte{byte('t')})
				}
				continue
			} else if e.Name == virtual.VirtualFuncIsReady {
				p = plan.Combine(p, &plan.VirtualPlan{})
				virtualRowCols = append(virtualRowCols,
					pgproto3.FieldDescription{
						Name:                 []byte(virtual.VirtualFuncIsReady),
						DataTypeOID:          catalog.ARRAYOID,
						TypeModifier:         -1,
						DataTypeSize:         1,
						TableAttributeNumber: 0,
						TableOID:             0,
						Format:               0,
					})

				if plr.Ready() {
					virtualRowVals = append(virtualRowVals, []byte{byte('t')})
				} else {
					virtualRowVals = append(virtualRowVals, []byte{byte('f')})
				}
				continue
			} else if e.Name == virtual.VirtualFuncHosts {
				p = plan.Combine(p, &plan.VirtualPlan{})
				virtualRowCols = append(virtualRowCols,
					pgproto3.FieldDescription{
						Name:                 []byte("host"),
						DataTypeOID:          catalog.TEXTOID,
						TypeModifier:         -1,
						DataTypeSize:         1,
						TableAttributeNumber: 0,
						TableOID:             0,
						Format:               0,
					},
					pgproto3.FieldDescription{
						Name:                 []byte("rw"),
						DataTypeOID:          catalog.TEXTOID,
						TypeModifier:         -1,
						DataTypeSize:         1,
						TableAttributeNumber: 0,
						TableOID:             0,
						Format:               0,
					},
				)
				if len(e.Args) == 1 {
					var k string

					switch vv := e.Args[0].(type) {
					case *lyx.AExprSConst:
						k = vv.Value
					default:
						return nil, fmt.Errorf("incorrect argument type for %s", virtual.VirtualFuncHosts)
					}

					if v, ok := rm.CSM.InstanceHealthChecks()[k]; ok {
						virtualRowVals = append(virtualRowVals,
							[]byte(k), fmt.Appendf(nil, "%v", v.CR.RW))
					} else {
						return nil, fmt.Errorf("incorrect first argument for %s", virtual.VirtualFuncHosts)
					}
				} else {
					return nil, fmt.Errorf("incorrect argument number for %s", virtual.VirtualFuncHosts)
				}

				continue
			} else if e.Name == "current_setting" && len(e.Args) == 1 {
				if val, ok := e.Args[0].(*lyx.AExprSConst); ok && val.Value == "transaction_read_only" {
					p = plan.Combine(p, &plan.VirtualPlan{})
					virtualRowCols = append(virtualRowCols,
						pgproto3.FieldDescription{
							Name:                 []byte("current_setting"),
							DataTypeOID:          catalog.ARRAYOID,
							TypeModifier:         -1,
							DataTypeSize:         1,
							TableAttributeNumber: 0,
							TableOID:             0,
							Format:               0,
						})

					if rm.SPH.GetTsa() == config.TargetSessionAttrsRW {
						virtualRowVals = append(virtualRowVals, []byte{byte('f')})
					} else {
						virtualRowVals = append(virtualRowVals, []byte{byte('t')})
					}
					continue
				}
			}

			if e.Name == "current_schema" || e.Name == "now" || e.Name == "set_config" || e.Name == "version" || e.Name == "current_setting" {
				p = plan.Combine(p, &plan.RandomDispatchPlan{})
				continue
			}
			deduced := false
			for _, innerExp := range e.Args {
				switch iE := innerExp.(type) {
				case *lyx.Select:
					if tmp, err := plr.PlanQueryTopLevel(ctx, rm, iE); err != nil {
						return nil, err
					} else {
						p = plan.Combine(p, tmp)
						deduced = true
					}
				}
			}
			if !deduced {
				/* very questionable. */
				p = plan.Combine(p, &plan.RandomDispatchPlan{})
			}
		/* Expression like SELECT 1, SELECT 'a', SELECT 1.0, SELECT true, SELECT false */
		case *lyx.AExprSConst:

			p = plan.Combine(p, &plan.VirtualPlan{})
			virtualRowCols = append(virtualRowCols,
				pgproto3.FieldDescription{
					Name:                 []byte(colname),
					DataTypeOID:          catalog.TEXTOID,
					TypeModifier:         -1,
					DataTypeSize:         -1,
					TableAttributeNumber: 0,
					TableOID:             0,
					Format:               0,
				})

			virtualRowVals = append(virtualRowVals, []byte(e.Value))

		case *lyx.AExprIConst:

			p = plan.Combine(p, &plan.VirtualPlan{})
			virtualRowCols = append(virtualRowCols,
				pgproto3.FieldDescription{
					Name:                 []byte(colname),
					DataTypeOID:          catalog.INT4OID,
					TypeModifier:         -1,
					DataTypeSize:         4,
					TableAttributeNumber: 0,
					TableOID:             0,
					Format:               0,
				})

			virtualRowVals = append(virtualRowVals, fmt.Appendf(nil, "%d", e.Value))
		case *lyx.AExprNConst, *lyx.AExprBConst:
			p = plan.Combine(p, &plan.RandomDispatchPlan{})

		/* Special case for SELECT current_schema */
		case *lyx.ColumnRef:
			if e.ColName == "current_schema" {
				p = plan.Combine(p, &plan.RandomDispatchPlan{})
			}
		case *lyx.Select:
			if tmp, err := plr.PlanQueryTopLevel(ctx, rm, e); err != nil {
				return nil, err
			} else {
				p = plan.Combine(p, tmp)
			}
		}
	}

	switch q := p.(type) {
	case *plan.VirtualPlan:
		q.TTS = &tupleslot.TupleTableSlot{
			Desc: virtualRowCols,
			Raw:  [][][]byte{virtualRowVals},
		}
	}

	return p, nil
}

func PlanWithClause(ctx context.Context, rm *rmeta.RoutingMetadataContext, plr QueryPlanner, WithClause []*lyx.CommonTableExpr) (plan.Plan, error) {
	var p plan.Plan
	for _, cte := range WithClause {
		switch qq := cte.SubQuery.(type) {
		case *lyx.ValueClause:
			/* special case */
			for _, vv := range qq.Values {
				for i, name := range cte.NameList {
					if i < len(cte.NameList) && i < len(vv) {
						/* XXX: currently only one-tuple aux values supported */
						rm.RecordAuxExpr(cte.Name, name, vv[i])
					}
				}
			}
		default:
			if tmp, err := plr.PlanQueryTopLevel(ctx, rm, cte.SubQuery); err != nil {
				return nil, err
			} else {
				p = plan.Combine(p, tmp)
			}
		}
	}

	return p, nil
}
