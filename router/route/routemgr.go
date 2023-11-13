package route

import (
	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/router/routehint"
	"github.com/pg-sharding/spqr/router/routingstate"
)

type RouteMgr interface {
	// Parse and analyze user query, and decide which shard routes
	// will participate in query execturion
	Reroute(params [][]byte, rh routehint.RouteHint) error
	// Acquire (prepare) connection to any random shard route
	// Without any user query analysis
	RerouteToRandomRoute() error
	RerouteToTargetRoute(route *routingstate.DataShardRoute) error

	CurrentRoutes() []*routingstate.DataShardRoute
	/* Unroute Routines */
	UnRouteWithError(shkey []kr.ShardKey, errmsg error) error
	Unroute(shkey []kr.ShardKey) error
	UnrouteRoutes(routes []*routingstate.DataShardRoute) error
}
