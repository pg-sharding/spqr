package plan

type Plan interface {
	iPlan()
}

type ShardPlan struct {
	Plan
	Query string
}

type DummyPlan struct {
	Plan
}
