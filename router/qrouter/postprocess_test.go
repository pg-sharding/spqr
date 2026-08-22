package qrouter

import (
	"context"
	"testing"

	"github.com/pg-sharding/lyx/lyx"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/pkg/plan"
	"github.com/pg-sharding/spqr/router/rmeta"
)

func TestAddAggregateToPlanUppercaseCount(t *testing.T) {
	const query = "SELECT COUNT(*) FROM users"

	statements, _, err := lyx.Parse(query)
	if err != nil {
		t.Fatal(err)
	}

	got, err := (&ProxyQrouter{}).addAggregateToPlan(context.Background(), &rmeta.RoutingMetadataContext{
		Query: query,
		Stmt:  statements[0],
	}, &plan.ScatterPlan{ExecTargets: []kr.ShardKey{{Name: "sh1"}, {Name: "sh2"}}})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := got.(*plan.VirtualPlan); !ok {
		t.Fatalf("expected *plan.VirtualPlan, got %T", got)
	}
}
