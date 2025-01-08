package plan

/* Distinguish planner from executor? */
type Plan interface {
	iPlan()

	Run()
}

type ShardPlan struct {
	Plan
	Query string
}

type DummyPlan struct {
	Plan
}
