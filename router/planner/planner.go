package planner

import (
	"context"

	"github.com/pg-sharding/lyx/lyx"
	"github.com/pg-sharding/spqr/pkg/models/distributions"
	"github.com/pg-sharding/spqr/router/plan"
	"github.com/pg-sharding/spqr/router/rfqn"
	"github.com/pg-sharding/spqr/router/rmeta"
)

func PlanDistributedQuery(ctx context.Context, rm *rmeta.RoutingMetadataContext, stmt lyx.Node) plan.Plan {

	switch v := stmt.(type) {
	case *lyx.ValueClause:
		return plan.ScatterPlan{}
	case *lyx.Select:
		/* Should be single-relation scan or values. Join to be supported */
		if len(v.FromClause) == 0 {
			return plan.ScatterPlan{}
		}
		if len(v.FromClause) > 1 {
			return plan.DummyPlan{}
		}

		s := plan.Scan{}
		switch q := v.FromClause[0].(type) {
		case *lyx.RangeVar:
			s.Relation = q
		default:
			return plan.DummyPlan{}
		}

		s.Projection = v.TargetList

		/* Todo: support grouping columns */
		return plan.ScatterPlan{
			SubPlan: s,
		}
	case *lyx.Insert:

		switch q := v.TableRef.(type) {
		case *lyx.RangeVar:
			rfqn := rfqn.RelationFQN{
				RelationName: q.RelationName,
				SchemaName:   q.SchemaName,
			}

			if ds, err := rm.GetRelationDistribution(ctx, rm.Mgr, rfqn); err != nil {
				return plan.DummyPlan{}
			} else if ds.Id != distributions.REPLICATED {

				return plan.DummyPlan{}
			}
			/* Plan sub-select here. TODO: check that modified relation is a ref relation */
			subPlan := PlanDistributedQuery(ctx, rm, v.SubSelect)
			switch subPlan.(type) {
			case plan.ScatterPlan:
				return plan.ScatterPlan{
					SubPlan: plan.ModifyTable{},
				}
			}
		default:
			return plan.DummyPlan{}
		}
	case *lyx.Update:
		/* Plan sub-select here. TODO: check that modified relation is a ref relation */
		return plan.ScatterPlan{
			SubPlan: plan.ModifyTable{},
		}
	case *lyx.Delete:
		/* Plan sub-select here. TODO: check that modified relation is a ref relation */
		return plan.ScatterPlan{
			SubPlan: plan.ModifyTable{},
		}
	}

	return plan.DummyPlan{}
}
