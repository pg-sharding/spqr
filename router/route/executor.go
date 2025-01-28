package route

import (
	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/router/plan"
)

type QueryExecutor interface {
	// Parse and analyze user query, and decide which shard routes
	// will participate in query execturion
	Reroute() (plan.Plan, error)
	// Acquire (prepare) connection to any random shard route
	// Without any user query analysis
	RerouteToRandomRoute() (plan.Plan, error)
	RerouteToTargetRoute(route *kr.ShardKey) (plan.Plan, error)

	CurrentRoutes() []*kr.ShardKey
	/* Unroute Routines */
	UnRouteWithError(shkey []kr.ShardKey, errmsg error) error
	Unroute(shkey []kr.ShardKey) error
	UnrouteRoutes(routes []*kr.ShardKey) error
}
