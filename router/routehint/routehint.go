package routehint

import "github.com/pg-sharding/spqr/router/plan"

type RouteHint interface {
	iRouteHint()
}

type EmptyRouteHint struct {
	RouteHint
}

type TargetRouteHint struct {
	RouteHint
	State plan.Plan
}

type ScatterRouteHint struct {
	RouteHint
}
