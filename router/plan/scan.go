package plan

import "github.com/pg-sharding/lyx/lyx"

type Scan struct {
	Plan
	Relation        *lyx.RangeVar
	Projection      []lyx.Node
	GroupingColumns []string
}
