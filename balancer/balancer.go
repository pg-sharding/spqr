package balancer

import "context"

type Balancer interface {
	RunBalancer(ctx context.Context)
}
