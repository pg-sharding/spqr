package topology

import (
	"context"

	protos "github.com/pg-sharding/spqr/pkg/protos"
	"github.com/pg-sharding/spqr/qdb"
)

type Router struct {
	ID      string
	Address string
	State   qdb.RouterState
}

type RouterMgr interface {
	RegisterRouter(ctx context.Context, r *Router) error
	ListRouters(ctx context.Context) ([]*Router, error)
	UnregisterRouter(ctx context.Context, id string) error
	SyncRouterMetadata(ctx context.Context, router *Router) error
}

func RouterToProto(r *Router) *protos.Router {
	return &protos.Router{
		Id:      r.ID,
		Address: r.Address,
	}
}

func RouterFromProto(r *protos.Router) *Router {
	return &Router{
		ID:      r.Id,
		Address: r.Address,
	}
}

func RouterToDB(r *Router) *qdb.Router {
	return &qdb.Router{
		ID:      r.ID,
		Address: r.Address,
	}
}
