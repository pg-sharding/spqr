package route

import (
	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/router/plan"
)

type ExecutionSliceMgr interface {
	// Parse and analyze user query, and decide which shard routes
	// will participate in query execution
	CreateSlicePlan() (plan.Plan, error)

	/* Unroute Routines */
	UnRouteWithError(shkey []kr.ShardKey, errmsg error) error
	Unroute(shkey []kr.ShardKey) error
}
