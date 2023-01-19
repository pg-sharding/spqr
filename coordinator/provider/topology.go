package provider

import (
	context "context"

	"github.com/pg-sharding/spqr/coordinator"
	"github.com/pg-sharding/spqr/pkg/models/topology"
	protos "github.com/pg-sharding/spqr/pkg/protos"
)

type TopologyService struct {
	protos.UnimplementedTopologyServiceServer

	impl coordinator.Coordinator
}

func (r *TopologyService) ListRouters(ctx context.Context, request *protos.ListRouterRequest) (*protos.ListRouterReply, error) {
	routers, err := r.impl.ListRouters(ctx)
	if err != nil {
		return nil, err
	}

	var routersReply []*protos.Router

	for _, router := range routers {
		routersReply = append(routersReply, topology.RouterToProto(router))
	}

	return &protos.ListRouterReply{
		Routers: routersReply,
	}, nil
}

func (r *TopologyService) AddRouters(ctx context.Context, request *protos.AddRouterRequest) (*protos.AddRouterReply, error) {
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

func NewTopologyService(impl coordinator.Coordinator) *TopologyService {
	return &TopologyService{
		impl: impl,
	}
}
