package topology

import (
	"context"
)

type Router struct {
	Id      string
	AdmAddr string
}

type RouterMgr interface {
	RegisterRouter(ctx context.Context, r *Router) error
	ListRouters(ctx context.Context) ([]*Router, error)
	UnregisterRouter(ctx context.Context, id string) error
	SyncRouterMetadata(ctx context.Context, router *Router) error
}
