package planner

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/pg-sharding/lyx/lyx"
	"github.com/pg-sharding/spqr/pkg/catalog"
	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/models/distributions"
	"github.com/pg-sharding/spqr/pkg/models/hashfunction"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/pkg/models/spqrerror"
	"github.com/pg-sharding/spqr/pkg/plan"
	"github.com/pg-sharding/spqr/pkg/prepstatement"
	"github.com/pg-sharding/spqr/pkg/spqrlog"
	"github.com/pg-sharding/spqr/qdb"
	"github.com/pg-sharding/spqr/router/rerrors"
	"github.com/pg-sharding/spqr/router/rfqn"
	"github.com/pg-sharding/spqr/router/rmeta"
	"github.com/pg-sharding/spqr/router/virtual"
	spqrparser "github.com/pg-sharding/spqr/yacc/console"
)

const (
	EnhancedEngineVersion = "v2"
)

type PlannerV2 struct {
	// Nothing?
}

// PlanQueryTopLevel implements QueryPlanner.
func (p *PlannerV2) PlanQueryTopLevel(ctx context.Context, rm *rmeta.RoutingMetadataContext, s lyx.Node) (plan.Plan, error) {
	return p.PlanDistributedQuery(ctx, rm, s, true)
}

// Ready implements QueryPlanner.
func (p *PlannerV2) Ready() bool {
	return true
}

func PlanCreateTable(ctx context.Context, rm *rmeta.RoutingMetadataContext, v *lyx.CreateTable) (plan.Plan, error) {
	if val := rm.SPH.AutoDistribution(); val != "" {

		switch q := v.TableRv.(type) {
		case *lyx.RangeVar:

			/* pre-attach relation to its distribution
			 * sic! this is not transactional nor abortable
			 */
			spqrlog.Zero.Debug().Str("relation", q.RelationName).Str("distribution", val).Msg("attaching relation")

			if val == distributions.REPLICATED {
				err := rmeta.CreateReferenceRelation(ctx, rm.Mgr, q)
				if err != nil {
					return nil, err
				}
			} else {
				if err := rm.Mgr.AlterDistributionAttach(ctx, val, []*distributions.DistributedRelation{
					{
						Name:               q.RelationName,
						ReplicatedRelation: val == distributions.REPLICATED,
						DistributionKey: []distributions.DistributionKeyEntry{
							{
								Column: rm.SPH.DistributionKey(),
								/* support hash function here */
							},
						},
					},
				}); err != nil {
					return nil, err
				}
			}
		}
	}
	/* TODO: support */
	/*
	 * Disallow to create table which does not contain any sharding column
	 */
	if err := CheckTableIsRoutable(ctx, rm.Mgr, v); err != nil {
		return nil, err
	}

	/*XXX: fix this */
	return &plan.ScatterPlan{
		IsDDL: true,
	}, nil
}

func (p *PlannerV2) PlanReferenceRelationModifyWithSubquery(ctx context.Context,
	rm *rmeta.RoutingMetadataContext,
	q *lyx.RangeVar, subquery lyx.Node,
	allowDistr bool,
	allowRewrite bool) (plan.Plan, error) {

	qualName := &rfqn.RelationFQN{
		RelationName: q.RelationName,
		SchemaName:   q.SchemaName,
	}

	if ds, err := rm.GetRelationDistribution(ctx, qualName); err != nil {
		return nil, rerrors.ErrComplexQuery
	} else if ds.Id != distributions.REPLICATED {
		if allowDistr {
			if subquery == nil {
				return &plan.ScatterPlan{
					SubPlan: &plan.ModifyTable{
						ExecTargets: nil,
					},
					ExecTargets: nil,
				}, nil
			} else {
				subPlan, err := p.PlanDistributedQuery(ctx, rm, subquery, allowRewrite)
				if err != nil {
					return nil, err
				}
				/* XXX: fix that */
				switch subPlan.(type) {
				case *plan.ScatterPlan:
					return &plan.ScatterPlan{
						SubPlan: &plan.ModifyTable{
							ExecTargets: nil,
						},
						ExecTargets: nil,
					}, nil
				default:
					return nil, rerrors.ErrComplexQuery
				}
			}
		} else {
			return nil, rerrors.ErrComplexQuery
		}
	}

	var shs []kr.ShardKey

	if rmeta.IsRelationCatalog(qualName) {
		shs = nil
	} else {
		r, err := rm.Mgr.GetReferenceRelation(ctx, qualName)
		if err != nil {
			return nil, err
		}
		// column rewrite is not yet supported to planner v2
		if len(r.ColumnSequenceMapping) != 0 && !allowRewrite {
			return nil, rerrors.ErrComplexQuery
		}

		shs = r.ListStorageRoutes()
	}

	if subquery == nil {
		return &plan.ScatterPlan{
			SubPlan: &plan.ModifyTable{
				ExecTargets: shs,
			},
			ExecTargets: shs,
		}, nil
	}
	/* Plan sub-select here. TODO: check that modified relation is a ref relation */
	subPlan, err := p.PlanDistributedQuery(ctx, rm, subquery, allowRewrite)
	if err != nil {
		return nil, err
	}
	switch subPlan.(type) {
	case *plan.ScatterPlan:
		return &plan.ScatterPlan{
			SubPlan: &plan.ModifyTable{
				ExecTargets: shs,
			},
			ExecTargets: shs,
		}, nil
	default:
		return nil, rerrors.ErrComplexQuery
	}
}

func PlanReferenceRelationInsertValues(ctx context.Context,
	rm *rmeta.RoutingMetadataContext,
	columns []string,
	rv *lyx.RangeVar,
	values *lyx.ValueClause,
	idCache IdentityRouterCache,
) (plan.Plan, error) {

	/*  XXX: use interface call here */
	qualName := rfqn.RelationFQNFromRangeRangeVar(rv)

	rel, err := rm.Mgr.GetReferenceRelation(ctx, qualName)
	if err != nil {
		return nil, err
	}

	if q, err := InsertSequenceValue(ctx, rm.Query, rel.ColumnSequenceMapping, idCache); err != nil {
		return nil, err
	} else {
		return &plan.ScatterPlan{
			OverwriteQuery: q,
			ExecTargets:    rel.ListStorageRoutes(),
		}, nil
	}
}

func CalculateRoutingListTupleItemValue(
	rm *rmeta.RoutingMetadataContext,
	relation *distributions.DistributedRelation,
	tp string,
	expr lyx.Node, queryParamsFormatCodes []int16) (any, error) {

	v, err := rmeta.ParseExprValue(tp, expr)
	if err != nil {
		return nil, err
	}

	switch q := v.(type) {
	case rmeta.ParamRef:

		// TODO: switch column type here
		// only works for one value
		ind := q.Indx
		if len(queryParamsFormatCodes) < ind {
			return nil, plan.ErrResolvingValue
		}

		fc := queryParamsFormatCodes[ind]

		singleVal, err := plan.ParseResolveParamValue(fc, ind, tp, rm.SPH.BindParams())
		if err != nil {
			return nil, err
		}

		v = singleVal
	}
	return v, nil
}

func PlanDistributedRelationInsert(ctx context.Context, routingList [][]lyx.Node, rm *rmeta.RoutingMetadataContext, stmt *lyx.Insert) ([]kr.ShardKey, error) {

	insertColsPos, qualName, err := ProcessInsertFromSelectOffsets(ctx, stmt, rm)
	if err != nil {
		return nil, err
	}

	var ds *distributions.Distribution

	if ds, err = rm.GetRelationDistribution(ctx, qualName); err != nil {
		return nil, err
	}

	/* Omit distributed relations */
	if ds.Id == distributions.REPLICATED {
		/* should not happen */
		return nil, rerrors.ErrComplexQuery
	}

	krs, err := rm.Mgr.ListKeyRanges(ctx, ds.Id)
	if err != nil {
		return nil, err
	}

	queryParamsFormatCodes := prepstatement.GetParams(rm.SPH.BindParamFormatCodes(), rm.SPH.BindParams())
	tupleShards := make([]kr.ShardKey, len(routingList))
	relation := ds.GetRelation(qualName)

	for i := range routingList {
		tup := make([]any, len(ds.ColTypes))

		for j, tp := range ds.ColTypes {

			/* Do not return err here.
			* This particular insert stmt is un-routable, but still, give it a try
			* and continue parsing.
			* Example: INSERT INTO xx SELECT * FROM xx a WHERE a.w_id = 20;
			* we have no insert cols specified, but still able to route on select
			 */

			hf, err := hashfunction.HashFunctionByName(relation.DistributionKey[j].HashFunction)
			if err != nil {
				spqrlog.Zero.Debug().Err(err).Msg("failed to resolve hash function")
				return nil, err
			}

			if len(relation.DistributionKey[j].Column) == 0 {

				if len(relation.DistributionKey[j].Expr.ColRefs) == 0 {
					return nil, rerrors.ErrComplexQuery
				}

				acc := []byte{}

				for _, cr := range relation.DistributionKey[j].Expr.ColRefs {

					val, ok := insertColsPos[cr.ColName]

					if !ok {
						return nil, nil
					}

					if len(routingList[i]) <= val {
						return nil, nil
					}

					switch routingList[i][val].(type) {
					case *lyx.AExprIConst, *lyx.AExprBConst, *lyx.AExprSConst, *lyx.ParamRef, *lyx.AExprNConst:
					default:
						return nil, nil
					}

					/* this is always non-ident hash function */
					itemVal, err := CalculateRoutingListTupleItemValue(rm,
						relation, cr.ColType,
						routingList[i][val],
						queryParamsFormatCodes)

					if err != nil {
						spqrlog.Zero.Debug().Err(err).Msg("failed to apply hash function")
						return nil, err
					}

					lExpr, err := hashfunction.ApplyNonIdentHashFunction(itemVal, cr.ColType, hf)

					if err != nil {
						spqrlog.Zero.Debug().Err(err).Msg("failed to apply hash function")
						return nil, err
					}

					acc = append(acc, hashfunction.EncodeUInt64(uint64(lExpr))...)
				}

				/* because we take hash of bytes */
				tup[j], err = hashfunction.ApplyHashFunction(acc, qdb.ColumnTypeVarcharHashed, hf)

				if err != nil {
					spqrlog.Zero.Debug().Err(err).Msg("failed to apply hash function")
					return nil, err
				}

			} else {
				val, ok := insertColsPos[relation.DistributionKey[j].Column]

				if !ok {
					return nil, nil
				}

				if len(routingList[i]) <= val {
					return nil, nil
				}

				switch routingList[i][val].(type) {
				case *lyx.AExprIConst, *lyx.AExprBConst, *lyx.AExprSConst, *lyx.ParamRef, *lyx.AExprNConst:
				default:
					return nil, nil
				}

				itemVal, err := CalculateRoutingListTupleItemValue(rm,
					relation, tp,
					routingList[i][val],
					queryParamsFormatCodes)

				if err != nil {
					spqrlog.Zero.Debug().Err(err).Msg("failed to apply hash function")
					return nil, err
				}

				tup[j], err = hashfunction.ApplyHashFunction(itemVal, ds.ColTypes[j], hf)

				if err != nil {
					spqrlog.Zero.Debug().Err(err).Msg("failed to apply hash function")
					return nil, err
				}
			}
		}

		tupleShard, err := rm.DeparseKeyWithRangesInternal(ctx, tup, krs)
		if err != nil {
			spqrlog.Zero.Debug().Interface("composite key", tup).Err(err).Msg("encountered the route error")
			return nil, err
		}

		spqrlog.Zero.Debug().
			Interface("tuple shard", tupleShard).
			Msg("calculated route for single insert tuple")

			/* this is modify stmt */
		tupleShards[i] = tupleShard
	}
	return tupleShards, nil
}

func PlanVirtualFunctionCall(ctx context.Context, rm *rmeta.RoutingMetadataContext, fname string, args []lyx.Node) (plan.Plan, error) {
	switch fname {
	case virtual.VirtualShow:

		/*  XXX: unite this code with client interactor internals */

		if len(args) != 1 {
			return nil, fmt.Errorf("%s function only accept single arg", virtual.VirtualShow)
		}

		switch v := args[0].(type) {
		case *lyx.AExprSConst:
			switch v.Value {
			case spqrparser.KeyRangesStr:
				ranges, err := rm.Mgr.ListAllKeyRanges(ctx)
				if err != nil {
					return nil, err
				}
				locksKr, err := rm.Mgr.ListKeyRangeLocks(ctx)
				if err != nil {
					return nil, err
				}
				return plan.KeyRangeVirtualPlan(ranges, locksKr), nil
			case spqrparser.HostsStr:

				shards, err := rm.Mgr.ListShards(ctx)
				if err != nil {
					return nil, err
				}

				ihc := rm.CSM.InstanceHealthChecks()
				return plan.HostsVirtualPlan(shards, ihc), nil
			default:
				return nil, rerrors.ErrComplexQuery
			}
		default:
			return nil, rerrors.ErrComplexQuery
		}

		/*  De-support? use __spqr__show(shards)*/
	case virtual.VirtualShards:
		p := &plan.VirtualPlan{
			VirtualRowCols: []pgproto3.FieldDescription{
				{
					Name:                 []byte("shard name"),
					DataTypeOID:          catalog.TEXTOID,
					TypeModifier:         -1,
					DataTypeSize:         1,
					TableAttributeNumber: 0,
					TableOID:             0,
					Format:               0,
				},
			},
		}
		shs, err := rm.Mgr.ListShards(ctx)
		if err != nil {
			return nil, err
		}
		for _, sh := range shs {
			p.VirtualRowVals = append(p.VirtualRowVals, [][]byte{[]byte(sh.ID)})
		}

		return p, nil
	case virtual.VirtualFuncHosts:
		if rm.CSM == nil {
			return nil, fmt.Errorf("spqr metadata uninitialized")
		}
		p := &plan.VirtualPlan{
			VirtualRowCols: []pgproto3.FieldDescription{
				{
					Name:                 []byte("host"),
					DataTypeOID:          catalog.TEXTOID,
					TypeModifier:         -1,
					DataTypeSize:         1,
					TableAttributeNumber: 0,
					TableOID:             0,
					Format:               0,
				},
				{
					Name:                 []byte("rw"),
					DataTypeOID:          catalog.TEXTOID,
					TypeModifier:         -1,
					DataTypeSize:         1,
					TableAttributeNumber: 0,
					TableOID:             0,
					Format:               0,
				},
			},
		}

		if len(args) == 1 {
			var k string

			switch vv := args[0].(type) {
			case *lyx.AExprSConst:
				k = vv.Value
			default:
				return nil, fmt.Errorf("incorrect argument type for %s", virtual.VirtualFuncHosts)
			}

			if v, ok := rm.CSM.InstanceHealthChecks()[k]; ok {
				p.VirtualRowVals = append(p.VirtualRowVals,
					[][]byte{[]byte(k),
						fmt.Appendf(nil, "%v", v.CR.RW)})
			} else {
				return nil, fmt.Errorf("incorrect first argument for %s", virtual.VirtualFuncHosts)
			}
		} else {
			return nil, fmt.Errorf("incorrect argument number for %s", virtual.VirtualFuncHosts)
		}

		return p, nil
	}
	return nil, fmt.Errorf("unknown virtual spqr function: %s", fname)
}

func (plnr *PlannerV2) PlanDistributedQuery(ctx context.Context,
	rm *rmeta.RoutingMetadataContext,
	stmt lyx.Node, allowRewrite bool) (plan.Plan, error) {

	switch v := stmt.(type) {
	/* TDB: comments? */
	case *lyx.ValueClause:
		return &plan.ScatterPlan{
			IsDDL: true,
		}, nil
	case *lyx.ExplainStmt:
		return plnr.PlanDistributedQuery(ctx, rm, v.Query, allowRewrite)
	case *lyx.Select:
		/* Should be single-relation scan or values. Join to be supported */
		if len(v.FromClause) == 0 {

			// try to plan query, if query is virtual-only

			// is query a single function call?

			if len(v.TargetList) == 1 {
				switch q := v.TargetList[0].(type) {
				case *lyx.FuncApplication:
					if strings.HasPrefix(q.Name, "__spqr__") {
						return PlanVirtualFunctionCall(ctx, rm, q.Name, q.Args)
					}
				}
			}

			/* Should we try to recurse here? */
			/* try to run planner on target list  */

			p, err := PlanTargetList(ctx, rm, plnr, v)
			if err != nil {
				return nil, err
			}

			return p, nil
		}

		if len(v.FromClause) > 1 {
			return nil, rerrors.ErrComplexQuery
		}

		/* we only support reference relation here */

		s := plan.Scan{}
		switch q := v.FromClause[0].(type) {
		case *lyx.RangeVar:
			s.Relation = q

			qualName := rfqn.RelationFQNFromRangeRangeVar(q)

			// CTE, skip
			if rm.RFQNIsCTE(qualName) {
				// is that ok?
				return &plan.ScatterPlan{}, nil
			}

			if ds, err := rm.GetRelationDistribution(ctx, qualName); err != nil {
				return nil, err
			} else if ds.ID() != distributions.REPLICATED {
				return nil, rerrors.ErrComplexQuery
			}
		default:
			return nil, rerrors.ErrComplexQuery
		}

		s.Projection = v.TargetList

		/* Todo: support grouping columns */
		return &plan.ScatterPlan{
			SubPlan: s,
		}, nil
	case *lyx.Insert:
		switch q := v.TableRef.(type) {
		case *lyx.RangeVar:
			return plnr.PlanReferenceRelationModifyWithSubquery(ctx, rm, q, v.SubSelect, false, allowRewrite)
		default:
			return nil, rerrors.ErrComplexQuery
		}

	case *lyx.Update:
		switch q := v.TableRef.(type) {
		case *lyx.RangeVar:
			return plnr.PlanReferenceRelationModifyWithSubquery(ctx, rm, q, nil, true, allowRewrite)
		default:
			return nil, rerrors.ErrComplexQuery
		}

	case *lyx.Delete:
		switch q := v.TableRef.(type) {
		case *lyx.RangeVar:
			return plnr.PlanReferenceRelationModifyWithSubquery(ctx, rm, q, nil, true, allowRewrite)
		default:
			return nil, rerrors.ErrComplexQuery
		}
	default:
		return nil, spqrerror.NewByCode(spqrerror.SPQR_NOT_IMPLEMENTED)
	}
}

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
		q.VirtualRowCols = virtualRowCols
		q.VirtualRowVals = [][][]byte{virtualRowVals}
	}

	return p, nil
}

func PlanUtility(ctx context.Context, rm *rmeta.RoutingMetadataContext, stmt lyx.Node) (plan.Plan, error) {

	switch node := stmt.(type) {

	/* TDB: comments? */

	case *lyx.VariableSetStmt:
		/* TBD: maybe skip all set stmts? */
		/*
		 * SET x = y etc., do not dispatch any statement to shards, just process this in router
		 */
		return &plan.RandomDispatchPlan{}, nil

	case *lyx.VariableShowStmt:
		/*
		 if we want to reroute to execute this stmt, route to random shard
		 XXX: support intelligent show support, without direct query dispatch
		*/
		return &plan.RandomDispatchPlan{}, nil

	// XXX: need alter table which renames sharding column to non-sharding column check
	case *lyx.CreateSchema:
		return &plan.ScatterPlan{
			IsDDL: true,
		}, nil

	case *lyx.DefineStmt:
		return &plan.ScatterPlan{
			IsDDL: true,
		}, nil

	case *lyx.CreateExtension:
		return &plan.ScatterPlan{
			IsDDL: true,
		}, nil
	case *lyx.Grant:
		return &plan.ScatterPlan{
			IsDDL: true,
		}, nil
	case *lyx.CreateTable:
		ds, err := PlanCreateTable(ctx, rm, node)
		if err != nil {
			return nil, err
		}
		/*
		 * Disallow to create table which does not contain any sharding column
		 */
		if err := CheckTableIsRoutable(ctx, rm.Mgr, node); err != nil {
			return nil, err
		}
		return ds, nil
	case *lyx.VacuumStmt:
		/* Send vacuum to each shard */
		return &plan.ScatterPlan{
			IsDDL: true,
		}, nil

	case *lyx.Cluster:
		/* Send vacuum to each shard */
		return &plan.ScatterPlan{
			IsDDL: true,
		}, nil
	case *lyx.Index:
		/*
		 * Disallow to index on table which does not contain any sharding column
		 */
		// XXX: do it
		return &plan.ScatterPlan{
			IsDDL: true,
		}, nil

	case *lyx.Alter, *lyx.Drop, *lyx.Truncate:
		// support simple ddl commands, route them to every chard
		// this is not fully ACID (not atomic at least)
		return &plan.ScatterPlan{
			IsDDL: true,
		}, nil
		/*
			 case *pgquery.Node_DropdbStmt, *pgquery.Node_DropRoleStmt:
				 // forbid under separate setting
				 return MultiMatchState{}, nil
		*/
	case *lyx.CreateRole, *lyx.CreateDatabase:
		// forbid under separate setting
		return &plan.ScatterPlan{
			IsDDL: true,
		}, nil
	case *lyx.Delete, *lyx.Update, *lyx.Select, *lyx.Insert, *lyx.ValueClause, *lyx.ExplainStmt:
		/* do not bother with those */
		return nil, nil
	case *lyx.Copy:
		return &plan.CopyPlan{}, nil
	default:
		return nil, spqrerror.NewByCode(spqrerror.SPQR_NOT_IMPLEMENTED)
	}
}
