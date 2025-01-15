package plan

type Plan interface {
	iPlan()
}

type ShardPlan struct {
	Plan
	Query string
}

type ScatterPlan struct {
	Plan
	SubPlan Plan
}

type ModifyTable struct {
	Plan
}

type DummyPlan struct {
	Plan
}
