package plan

import (
	"context"

	"github.com/pg-sharding/lyx/lyx"
)

/*
* This is basically proxxy_routing.go done right.
* But, to avoid any unexpected bugs, we do use it only for reference relations.
 */

func build_select_values(vv []lyx.Node) string {
	return "VALUES()"
}

func PlanDistributedQuery(ctx context.Context, stmt lyx.Node) Plan {

	switch v := stmt.(type) {
	case *lyx.Select:
		/* VALUES() case */
		if len(v.FromClause) == 0 {

			sqs := SimpleQueryScan{
				Query: build_select_values(v.TargetList),
			}

			return sqs
		}

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
	case *lyx.Insert:
		/* There will be outer INSERT slice and inner SELECT slice */

		rt := ModifyTable{
			Source: PlanDistributedQuery(ctx, v.SubSelect),
		}
		return rt
	}

	return DummyPlan{}
}
