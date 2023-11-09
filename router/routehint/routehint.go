package routehint

import "github.com/pg-sharding/spqr/router/routingstate"

type RouteHint interface {
	iRouteHint()
}

type EmptyRouteHint struct {
	RouteHint
}

type TargetRouteHint struct {
	State routingstate.RoutingState
}

func (t *TargetRouteHint) iRouteHint() {}
