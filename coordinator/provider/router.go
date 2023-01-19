package provider

import (
	context "context"

	"github.com/pg-sharding/spqr/coordinator"
	"github.com/pg-sharding/spqr/pkg/models/topology"
	protos "github.com/pg-sharding/spqr/pkg/protos"
)

type RouterService struct {
	protos.UnimplementedRouterServiceServer

	impl coordinator.Coordinator
}

func (r RouterService) ListRouters(ctx context.Context, request *protos.ListRoutersRequest) (*protos.ListRoutersReply, error) {
	routers, err := r.impl.ListRouters(ctx)
	if err != nil {
		return nil, err
	}

	var routersReply []*protos.Router

	for _, router := range routers {
		routersReply = append(routersReply, topology.RouterToProto(router))
	}

	return &protos.ListRoutersReply{
		Routers: routersReply,
	}, nil
}

func (r RouterService) AddRouter(ctx context.Context, request *protos.AddRouterRequest) (*protos.AddRouterReply, error) {
	routers, err := r.impl.ListRouters(ctx)
	if err != nil {
		return nil, err
	}

	var routersReply []*protos.Router

	for _, router := range routers {
		routersReply = append(routersReply, topology.RouterToProto(router))
	}

	return &protos.AddRouterReply{}, nil
}

var _ protos.RouterServiceServer = &RouterService{}

func NewRouterService(impl coordinator.Coordinator) *RouterService {
	return &RouterService{
		impl: impl,
	}
}
