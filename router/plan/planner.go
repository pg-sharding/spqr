package plan

import (
	"context"

	"github.com/pg-sharding/lyx/lyx"
)

func PlanDistributedQuery(ctx context.Context, stmt lyx.Node) Plan {

	switch v := stmt.(type) {
	case *lyx.ValueClause:
		return ScatterPlan{}
	case *lyx.Select:
		/* Should be single-relation scan or values. Join to be supported */
		if len(v.FromClause) == 0 {
			return ScatterPlan{}
		}
		if len(v.FromClause) > 1 {
			return DummyPlan{}
		}

		s := Scan{}
		switch q := v.FromClause[0].(type) {
		case *lyx.RangeVar:
			s.Relation = q
		default:
			return DummyPlan{}
		}

		s.Projection = v.TargetList

		/* Todo: support grouping columns */
		return ScatterPlan{
			SubPlan: s,
		}
	case *lyx.Insert:
		/* Plan sub-select here. TODO: check that modified relation is a ref relation */
		subPlan := PlanDistributedQuery(ctx, v.SubSelect)
		switch subPlan.(type) {
		case ScatterPlan:
			return ScatterPlan{
				SubPlan: ModifyTable{},
			}
		}
	case *lyx.Update:
		/* Plan sub-select here. TODO: check that modified relation is a ref relation */
		return ScatterPlan{
			SubPlan: ModifyTable{},
		}
	case *lyx.Delete:
		/* Plan sub-select here. TODO: check that modified relation is a ref relation */
		return ScatterPlan{
			SubPlan: ModifyTable{},
		}
	}

	return DummyPlan{}
}
