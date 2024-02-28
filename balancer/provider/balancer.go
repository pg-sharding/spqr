package provider

import (
	"context"
	"github.com/pg-sharding/spqr/balancer"
)

type BalancerImpl struct {
}

func (b *BalancerImpl) RunBalancer(ctx context.Context) {
	//TODO implement me
	panic("implement me")
}

var _ balancer.Balancer = &BalancerImpl{}
