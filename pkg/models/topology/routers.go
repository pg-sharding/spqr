package topology

import (
	"context"

	protos "github.com/pg-sharding/spqr/pkg/protos"
)

type Router struct {
	ID      string
	Address string
}

type RouterMgr interface {
	RegisterRouter(ctx context.Context, r *Router) error
	ListRouters(ctx context.Context) ([]*Router, error)
	UnregisterRouter(ctx context.Context, id string) error
	SyncRouterMetadata(ctx context.Context, router *Router) error
}

func RouterToProto(r *Router) *protos.Router {
	return &protos.Router{
		Id:     r.ID,
		Adress: r.Address,
	}
}
