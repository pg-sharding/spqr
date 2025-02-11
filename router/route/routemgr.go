package route

import (
	"github.com/pg-sharding/spqr/pkg/models/kr"
)

type RouteMgr interface {
	// Parse and analyze user query, and decide which shard routes
	// will participate in query execturion
	Reroute() error
	// Acquire (prepare) connection to any random shard route
	// Without any user query analysis
	RerouteToRandomRoute() error
	RerouteToTargetRoute(route *kr.ShardKey) error

	CurrentRoutes() []kr.ShardKey
	/* Unroute Routines */
	UnRouteWithError(shkey []kr.ShardKey, errmsg error) error
	Unroute(shkey []kr.ShardKey) error
}
