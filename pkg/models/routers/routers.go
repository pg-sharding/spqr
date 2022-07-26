package routers

import (
	"context"

	"github.com/pg-sharding/spqr/pkg/client"
)

type Router struct {
	Id      string
	AdmAddr string
}

type RouterMgr interface {
	RegisterRouter(ctx context.Context, r *Router) error
	ListRouters(ctx context.Context) ([]*Router, error)
	UnregisterRouter(ctx context.Context, id string) error
	ConfigureNewRouter(ctx context.Context, router *Router, client client.Client) error
}
