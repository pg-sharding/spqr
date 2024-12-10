package plan

import (
	"context"

	"github.com/pg-sharding/lyx/lyx"
)

func PlanDistributedQuery(ctx context.Context, stmt lyx.Node) Plan {

	switch v := stmt.(type) {
	case *lyx.Select:
		/* Should be single-relation scan. Join to be supported */
		if len(v.FromClause) != 1 {
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
		return s
	}

	return DummyPlan{}
}
