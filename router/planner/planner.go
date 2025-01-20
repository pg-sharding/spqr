package planner

import (
	"context"
	"strings"

	"github.com/pg-sharding/lyx/lyx"
	"github.com/pg-sharding/spqr/pkg/models/distributions"
	"github.com/pg-sharding/spqr/pkg/models/hashfunction"
	"github.com/pg-sharding/spqr/pkg/spqrlog"
	"github.com/pg-sharding/spqr/router/plan"
	"github.com/pg-sharding/spqr/router/rerrors"
	"github.com/pg-sharding/spqr/router/rfqn"
	"github.com/pg-sharding/spqr/router/rmeta"
)

func ResolveShardForCompositeKey(rm *rmeta.RoutingMetadataContext, relation distributions.DistributedRelation, ds distributions.Distribution, vals []interface{}) (plan.Plan, error) {
	var route plan.Plan

	krs, err := rm.Mgr.ListKeyRanges(context.TODO(), ds.Id)
	if err != nil {
		return nil, err
	}
	compositeKey := make([]interface{}, len(relation.DistributionKey))

	// TODO: multi-column routing. This works only for one-dim routing
	for i := range len(relation.DistributionKey) {
		hf, err := hashfunction.HashFunctionByName(relation.DistributionKey[i].HashFunction)
		if err != nil {
			spqrlog.Zero.Debug().Err(err).Msg("failed to resolve hash function")
			break
		}

		// col := relation.DistributionKey[i].Column

		/* TODO: support extended proto */
		//vals, valOk := qr.resolveValue(rm, rfqn, col, meta.SPH.BindParams(), queryParamsFormatCodes)

		// if !valOk {
		// 	break
		// }

		/* TODO: correct support for composite keys here */

		for _, val := range vals {
			compositeKey[i], err = hashfunction.ApplyHashFunction(val, ds.ColTypes[i], hf)
			spqrlog.Zero.Debug().Interface("key", val).Interface("hashed key", compositeKey[i]).Msg("applying hash function on key")

			if err != nil {
				spqrlog.Zero.Debug().Err(err).Msg("failed to apply hash function")
				return nil, err
			}

			currroute, err := rm.DeparseKeyWithRangesInternal(compositeKey, krs)
			if err != nil {
				spqrlog.Zero.Debug().Interface("composite key", compositeKey).Err(err).Msg("encoutered the route error")
				return nil, err
			}

			spqrlog.Zero.Debug().
				Interface("currroute", currroute).
				Str("table", relation.Name).
				Msg("calculated route for table/cols")

			route = plan.Combine(route, plan.ShardMatchState{
				Route: currroute,
				/* todo: TSA */
				// TargetSessionAttrs: tsa,
			})
		}
	}

	return route, nil
}

func ResolveValueShardEngineV2(rm *rmeta.RoutingMetadataContext, val lyx.Node) (plan.Plan, error) {
	switch val.(type) {
	case *lyx.AExprIConst:

	default:
		return nil, rerrors.ErrComplexQuery
	}
}

func ResolveExecutionByWhereEngineV2(ctx context.Context, rm *rmeta.RoutingMetadataContext, stmt lyx.Node) (plan.Plan, error) {
	switch v := stmt.(type) {
	case *lyx.AExprIn:
		switch vv := v.SubLink.(type) {
		case *lyx.AExprList:
			var res plan.Plan = nil
			/* go for each list value and recheck shard */
			for _, e := range vv.List {
				p1, err := ResolveValueShardEngineV2(rm, e)
				if err != nil {
					return nil, err
				}

				res = plan.Combine(res, p1)
			}

			return res, nil
		}
	case *lyx.AExprOp:
		switch v.Op {
		case "=":

			switch lft := v.Left.(type) {
			case *lyx.ColumnRef:

				alias, colname := lft.TableAlias, lft.ColName

				/* simple key-value pair */
				switch rght := v.Right.(type) {
				case *lyx.ParamRef, *lyx.AExprSConst, *lyx.AExprIConst:
					// else  error out?

					// TBD: postpone routing from here to root of parsing tree
					// maybe expimely inefficient. Will be fixed in SPQR-2.0
					if err := rm.ProcessConstExpr(alias, colname, rght); err != nil {
						return nil, err
					}

				case *lyx.AExprList:
					for _, expr := range rght.List {
						if err := rm.ProcessConstExpr(alias, colname, expr); err != nil {
							return nil, err
						}
					}
				case *lyx.FuncApplication:
					// there are several types of queries like DELETE FROM rel WHERE colref = func_applicion
					// and func_applicion is actually routable statement.
					// ANY(ARRAY(subselect)) if one type.

					if strings.ToLower(rght.Name) == "any" {
						if len(rght.Args) > 0 {
							// maybe we should consider not only first arg.
							// however, consider only it

							switch argexpr := rght.Args[0].(type) {
							case *lyx.SubLink:

								// ignore all errors.
								_ = qr.DeparseSelectStmt(ctx, argexpr.SubSelect, meta)
							}
						}
					}

				}
			}

		case "AND":
			p1, err := ResolveExecutionByWhereEngineV2(ctx, rm, v.Left)
			if err != nil {
				return nil, err
			}
			p2, err := ResolveExecutionByWhereEngineV2(ctx, rm, v.Right)
			if err != nil {
				return nil, err
			}
			return plan.Combine(p1, p2), nil

		default:
		}
	}

	return nil, rerrors.ErrComplexQuery
}

/* Plans top-level query */
func PlanDistributedQuery(ctx context.Context, rm *rmeta.RoutingMetadataContext, stmt lyx.Node) (plan.Plan, error) {

	switch v := stmt.(type) {

	/* TDB: comments? */

	case *lyx.VariableSetStmt:
		/* TBD: maybe skip all set stmts? */
		/*
		 * SET x = y etc., do not dispatch any statement to shards, just process this in router
		 */
		return plan.RandomShardScan{}, nil
	case *lyx.VariableShowStmt:
		/*
		 if we want to reroute to execute this stmt, route to random shard
		 XXX: support intelegent show support, without direct query dispatch
		*/
		return plan.RandomShardScan{}, nil
	// XXX: need alter table which renames sharding column to non-sharding column check
	case *lyx.CreateTable:
		if val := rm.SPH.AutoDistribution(); val != "" {

			switch q := v.TableRv.(type) {
			case *lyx.RangeVar:

				/* pre-attach relation to its distribution
				 * sic! this is not transactional not abortable
				 */
				if err := rm.Mgr.AlterDistributionAttach(ctx, val, []*distributions.DistributedRelation{
					{
						Name: q.RelationName,
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
		/*
		 * TODO:
		 * Disallow to create table which does not contain any sharding column
		 */
		return plan.DDLState{}, nil
	case *lyx.Vacuum:
		/* Send vacuum to each shard */
		return plan.DDLState{}, nil
	case *lyx.Analyze:
		/* Send vacuum to each shard */
		return plan.DDLState{}, nil
	case *lyx.Cluster:
		/* Send vacuum to each shard */
		return plan.DDLState{}, nil
	case *lyx.Index:
		/*
		 * Disallow to index on table which does not contain any sharding column
		 */
		// XXX: do it
		return plan.DDLState{}, nil

	case *lyx.Alter, *lyx.Drop, *lyx.Truncate:
		// support simple ddl commands, route them to every chard
		// this is not fully ACID (not atomic at least)
		return plan.DDLState{}, nil
	case *lyx.CreateRole, *lyx.CreateDatabase:
		// forbid under separate setting
		return plan.DDLState{}, nil

	case *lyx.ValueClause:
		return plan.ScatterPlan{}, nil
	case *lyx.Select:
		/* Should be single-relation scan or values. Join to be supported */
		if len(v.FromClause) == 0 {
			return plan.ScatterPlan{}, nil
		}
		if len(v.FromClause) > 1 {
			return nil, rerrors.ErrComplexQuery
		}

		s := plan.Scan{}
		switch q := v.FromClause[0].(type) {
		case *lyx.RangeVar:
			s.Relation = q
		default:
			return nil, rerrors.ErrComplexQuery
		}

		s.Projection = v.TargetList

		/* Todo: support grouping columns */
		return plan.ScatterPlan{
			SubPlan: s,
		}, nil
	case *lyx.Insert:

		switch q := v.TableRef.(type) {
		case *lyx.RangeVar:
			rfqn := rfqn.RelationFQN{
				RelationName: q.RelationName,
				SchemaName:   q.SchemaName,
			}

			if ds, err := rm.GetRelationDistribution(ctx, rfqn); err != nil {
				return nil, rerrors.ErrComplexQuery
			} else if ds.Id != distributions.REPLICATED {
				return nil, rerrors.ErrComplexQuery
			}
			/* Plan sub-select here. TODO: check that modified relation is a ref relation */
			subPlan, err := PlanDistributedQuery(ctx, rm, v.SubSelect)
			if err != nil {
				return nil, err
			}
			switch subPlan.(type) {
			case plan.ScatterPlan:
				return plan.ScatterPlan{
					SubPlan: plan.ModifyTable{},
				}, nil
			default:
				return nil, rerrors.ErrComplexQuery
			}
		default:
			return nil, rerrors.ErrComplexQuery
		}
	case *lyx.Update:

		switch q := v.TableRef.(type) {
		case *lyx.RangeVar:
			rfqn := rfqn.RelationFQN{
				RelationName: q.RelationName,
				SchemaName:   q.SchemaName,
			}

			if ds, err := rm.GetRelationDistribution(ctx, rfqn); err != nil {
				return nil, rerrors.ErrComplexQuery
			} else if ds.Id != distributions.REPLICATED {
				/* distributed relation update. We should resolve if query can be scattered to
				* some subset of execution segments.
				 */

				p, err := ResolveExecutionByWhereEngineV2(ctx, rm, v.Where)
				if err != nil {
					return nil, err
				}

				switch p.(type) {
				case *plan.ScatterPlan:
					/* execute update on all segments provided by where */
					return p, nil
				default:
					return nil, rerrors.ErrComplexQuery
				}
			}

			return plan.ScatterPlan{
				SubPlan: plan.ModifyTable{},
				// nil execution segments means all segmetns
			}, nil

		default:
			return nil, rerrors.ErrComplexQuery
		}
	case *lyx.Delete:

		switch q := v.TableRef.(type) {
		case *lyx.RangeVar:
			rfqn := rfqn.RelationFQN{
				RelationName: q.RelationName,
				SchemaName:   q.SchemaName,
			}

			if ds, err := rm.GetRelationDistribution(ctx, rfqn); err != nil {
				return nil, rerrors.ErrComplexQuery
			} else if ds.Id != distributions.REPLICATED {
				return nil, rerrors.ErrComplexQuery
			}

			return plan.ScatterPlan{
				SubPlan: plan.ModifyTable{},
			}, nil

		default:
			return nil, rerrors.ErrComplexQuery
		}
	}

	return nil, rerrors.ErrComplexQuery
}
