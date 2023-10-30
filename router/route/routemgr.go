package route

import (
	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/router/qrouter"
)

type RouteMgr interface {
	// Parse and analyze user query, and decide which shard routes
	// will participate in query execturion
	Reroute(params [][]byte) error
	// Acquire (prepare) connection to any random shard route
	// Without any user query analysis
	RerouteToRandomRoute() error
	RerouteToTargetRoute(route *qrouter.DataShardRoute) error

	CurrentRoutes() []*qrouter.DataShardRoute
	/* Unroute Routines */
	UnRouteWithError(shkey []kr.ShardKey, errmsg error) error
	Unroute(shkey []kr.ShardKey) error
	UnrouteRoutes(routes []*qrouter.DataShardRoute) error
}
