package plan

import "github.com/pg-sharding/lyx/lyx"

type Scan struct {
	Plan
	Relation        *lyx.RangeVar
	Projection      []lyx.Node
	GroupingColumns []string
}

type SimpleQueryScan struct {
	Plan
	Query string
}

type ModifyTable struct {
	Plan
	Relation *lyx.RangeVar

	Source Plan
}
