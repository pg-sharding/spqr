package planner

import (
	"context"

	"github.com/pg-sharding/lyx/lyx"
	"github.com/pg-sharding/spqr/pkg/models/distributions"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/pkg/models/rrelation"
	"github.com/pg-sharding/spqr/pkg/models/spqrerror"
	"github.com/pg-sharding/spqr/pkg/spqrlog"
	"github.com/pg-sharding/spqr/router/plan"
	"github.com/pg-sharding/spqr/router/rerrors"
	"github.com/pg-sharding/spqr/router/rfqn"
	"github.com/pg-sharding/spqr/router/rmeta"
)

func PlanCreateTable(ctx context.Context, rm *rmeta.RoutingMetadataContext, v *lyx.CreateTable) (plan.Plan, error) {
	if val := rm.SPH.AutoDistribution(); val != "" {

		switch q := v.TableRv.(type) {
		case *lyx.RangeVar:

			/* pre-attach relation to its distribution
			 * sic! this is not transactional nor abortable
			 */
			spqrlog.Zero.Debug().Str("relation", q.RelationName).Str("distribution", val).Msg("attaching relation")

			if val == distributions.REPLICATED {

				shs, err := rm.Mgr.ListShards(ctx)
				if err != nil {
					return nil, err
				}
				shardIds := []string{}
				for _, sh := range shs {
					shardIds = append(shardIds, sh.ID)
				}
				err = rm.Mgr.CreateReferenceRelation(ctx, &rrelation.ReferenceRelation{
					TableName:     q.RelationName,
					SchemaVersion: 1,
					ShardIds:      shardIds,
				}, nil)
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
	// /*
	//  * Disallow to create table which does not contain any sharding column
	//  */
	// if err := qr.CheckTableIsRoutable(ctx, node); err != nil {
	// 	return nil, false, err
	// }
	return plan.DDLState{}, nil
}

func PlanReferenceRelationModifyWithSubquery(ctx context.Context,
	rm *rmeta.RoutingMetadataContext,
	q *lyx.RangeVar, subquery lyx.Node,
	allowDistr bool) (plan.Plan, error) {

	qualName := &rfqn.RelationFQN{
		RelationName: q.RelationName,
		SchemaName:   q.SchemaName,
	}

	if ds, err := rm.GetRelationDistribution(ctx, qualName); err != nil {
		return nil, rerrors.ErrComplexQuery
	} else if ds.Id != distributions.REPLICATED {
		if allowDistr {
			if subquery == nil {
				return plan.ScatterPlan{
					SubPlan: plan.ModifyTable{
						ExecTargets: nil,
					},
					ExecTargets: nil,
				}, nil
			} else {
				subPlan, err := PlanDistributedQuery(ctx, rm, subquery)
				if err != nil {
					return nil, err
				}
				/* XXX: fix that */
				switch subPlan.(type) {
				case plan.ScatterPlan:
					return plan.ScatterPlan{
						SubPlan: plan.ModifyTable{
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

	var shs []*kr.ShardKey

	if rmeta.IsRelationCatalog(qualName) {
		shs = nil
	} else {
		r, err := rm.Mgr.GetReferenceRelation(ctx, qualName)
		if err != nil {
			return nil, err
		}
		shs = r.ListStorageRoutes()
	}

	if subquery == nil {
		return plan.ScatterPlan{
			SubPlan: plan.ModifyTable{
				ExecTargets: shs,
			},
			ExecTargets: shs,
		}, nil
	}
	/* Plan sub-select here. TODO: check that modified relation is a ref relation */
	subPlan, err := PlanDistributedQuery(ctx, rm, subquery)
	if err != nil {
		return nil, err
	}
	switch subPlan.(type) {
	case plan.ScatterPlan:
		return plan.ScatterPlan{
			SubPlan: plan.ModifyTable{
				ExecTargets: shs,
			},
			ExecTargets: shs,
		}, nil
	default:
		return nil, rerrors.ErrComplexQuery
	}
}

func insertSequenceValue(ctx context.Context, meta *rmeta.RoutingMetadataContext, qrouter_query *string, rel *rrelation.ReferenceRelation) error {

	query := *qrouter_query

	for colName, seqName := range rel.ColumnSequenceMapping {
		nextval, err := meta.Mgr.NextVal(ctx, seqName)
		if err != nil {
			return err
		}
		newQuery, err := ModifyQuery(query, colName, nextval)
		if err != nil {
			return err
		}
		query = newQuery
	}

	*qrouter_query = query
	return nil
}

func PlanReferenceRelationInsertValues(ctx context.Context, qrouter_query *string, rm *rmeta.RoutingMetadataContext, columns []string, rv *lyx.RangeVar, values *lyx.ValueClause) (plan.Plan, error) {

	/*  XXX: use interface call here */
	qualName := rfqn.RelationFQNFromRangeRangeVar(rv)

	rel, err := rm.Mgr.GetReferenceRelation(ctx, qualName)
	if err != nil {
		return nil, err
	}

	if err := insertSequenceValue(ctx, rm, qrouter_query, rel); err != nil {
		return nil, err
	}

	return plan.ScatterPlan{
		ExecTargets: rel.ListStorageRoutes(),
	}, nil
}

func PlanDistributedQuery(ctx context.Context, rm *rmeta.RoutingMetadataContext, stmt lyx.Node) (plan.Plan, error) {

	switch v := stmt.(type) {
	/* TDB: comments? */

	case *lyx.VariableSetStmt:
		/* TBD: maybe skip all set stmts? */
		/*
		 * SET x = y etc., do not dispatch any statement to shards, just process this in router
		 */
		return plan.RandomDispatchPlan{}, nil

	case *lyx.VariableShowStmt:
		/*
		 if we want to reroute to execute this stmt, route to random shard
		 XXX: support intelligent show support, without direct query dispatch
		*/
		return plan.RandomDispatchPlan{}, nil

	case *lyx.CreateSchema:
		return plan.DDLState{}, nil

	// XXX: need alter table which renames sharding column to non-sharding column check
	case *lyx.CreateTable:
		return PlanCreateTable(ctx, rm, v)
	case *lyx.Vacuum:
		/* Send vacuum to each shard */
		return plan.DDLState{}, nil
	case *lyx.Analyze:
		/* Send analyze to each shard */
		return plan.DDLState{}, nil
	case *lyx.Cluster:
		/* Send cluster to each shard */
		return plan.DDLState{}, nil
	case *lyx.Index:
		/*
		 * Disallow to index on table which does not contain any sharding column
		 */
		// XXX: do it
		return plan.DDLState{}, nil
	case *lyx.CreateExtension:
		return plan.DDLState{}, nil
	case *lyx.Alter, *lyx.Drop, *lyx.Truncate:
		// support simple ddl commands, route them to every chard
		// this is not fully ACID (not atomic at least)
		return plan.DDLState{}, nil

	case *lyx.CreateRole, *lyx.CreateDatabase:
		/* XXX: should we forbid under separate setting?  */
		return plan.DDLState{}, nil
	case *lyx.Copy:
		return plan.CopyState{}, nil

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
			return PlanReferenceRelationModifyWithSubquery(ctx, rm, q, v.SubSelect, false)
		default:
			return nil, rerrors.ErrComplexQuery
		}

	case *lyx.Update:
		switch q := v.TableRef.(type) {
		case *lyx.RangeVar:
			return PlanReferenceRelationModifyWithSubquery(ctx, rm, q, nil, true)
		default:
			return nil, rerrors.ErrComplexQuery
		}

	case *lyx.Delete:
		switch q := v.TableRef.(type) {
		case *lyx.RangeVar:
			return PlanReferenceRelationModifyWithSubquery(ctx, rm, q, nil, true)
		default:
			return nil, rerrors.ErrComplexQuery
		}
	default:
		return nil, spqrerror.NewByCode(spqrerror.SPQR_NOT_IMPLEMENTED)
	}
}
