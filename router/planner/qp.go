package planner

import (
	"context"

	"github.com/pg-sharding/lyx/lyx"
	"github.com/pg-sharding/spqr/pkg/plan"
	"github.com/pg-sharding/spqr/router/rmeta"
)

type QueryPlanner interface {
	PlanQueryTopLevel(context.Context, *rmeta.RoutingMetadataContext, lyx.Node) (plan.Plan, error)

	// XXX: find a better place
	Ready() bool
}

type DistributedPlanner interface {
	QueryPlanner

	PlanDistributedQuery(ctx context.Context,
		rm *rmeta.RoutingMetadataContext,
		stmt lyx.Node, allowRewrite bool) (plan.Plan, error)
}
