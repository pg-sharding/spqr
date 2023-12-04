package routehint

import "github.com/pg-sharding/spqr/router/routingstate"

type RouteHint interface {
	iRouteHint()
}

type EmptyRouteHint struct {
	RouteHint
}

type TargetRouteHint struct {
	RouteHint
	State routingstate.RoutingState
}

type ScatterRouteHint struct {
	RouteHint
}
