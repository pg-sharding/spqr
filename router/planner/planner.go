package planner

import (
	"context"

	"github.com/pg-sharding/lyx/lyx"
	"github.com/pg-sharding/spqr/pkg/models/distributions"
	"github.com/pg-sharding/spqr/pkg/models/spqrerror"
	"github.com/pg-sharding/spqr/router/plan"
	"github.com/pg-sharding/spqr/router/rerrors"
	"github.com/pg-sharding/spqr/router/rfqn"
	"github.com/pg-sharding/spqr/router/rmeta"
)

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
		 XXX: support intelegent show support, without direct query dispatch
		*/
		return plan.RandomDispatchPlan{}, nil

	case *lyx.CreateSchema:
		return plan.DDLState{}, nil

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

		/* TODO: support */
		// /*
		//  * Disallow to create table which does not contain any sharding column
		//  */
		// if err := qr.CheckTableIsRoutable(ctx, node); err != nil {
		// 	return nil, false, err
		// }
		return plan.DDLState{}, nil
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
		/* Plan sub-select here. TODO: check that modified relation is a ref relation */
		return plan.ScatterPlan{
			SubPlan: plan.ModifyTable{},
		}, nil
	case *lyx.Delete:
		/* Plan sub-select here. TODO: check that modified relation is a ref relation */
		return plan.ScatterPlan{
			SubPlan: plan.ModifyTable{},
		}, nil
	default:
		return nil, spqrerror.NewByCode(spqrerror.SPQR_NOT_IMPLEMENTED)
	}
}
