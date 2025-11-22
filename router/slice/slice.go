package slice

import (
	"context"

	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/pkg/plan"
	"github.com/pg-sharding/spqr/router/rmeta"
)

type ExecutionSliceMgr interface {
	// Parse and analyze user query, and decide which shard routes
	// will participate in query execution
	CreateSlicedPlan(ctx context.Context, rm *rmeta.RoutingMetadataContext) (plan.Plan, error)

	/* Unroute Routines */
	UnRouteWithError(shkey []kr.ShardKey, errmsg error) error
	Unroute(shkey []kr.ShardKey) error
}
