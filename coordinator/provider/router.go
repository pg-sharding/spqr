package provider

import (
	context "context"

	"github.com/pg-sharding/spqr/coordinator"
	"github.com/pg-sharding/spqr/pkg/models/topology"
	protos "github.com/pg-sharding/spqr/pkg/protos"
	"github.com/pg-sharding/spqr/pkg/spqrlog"
	"google.golang.org/protobuf/types/known/emptypb"
)

type RouterService struct {
	protos.UnimplementedRouterServiceServer

	impl coordinator.Coordinator
}

// TODO : unit tests
func (r RouterService) ListRouters(ctx context.Context, _ *emptypb.Empty) (*protos.ListRoutersReply, error) {
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

// TODO : unit tests
func (r RouterService) AddRouter(ctx context.Context, request *protos.AddRouterRequest) (*protos.AddRouterReply, error) {
	spqrlog.Zero.Debug().
		Str("router-id", request.Router.Id).
		Msg("register router in coordinator")
	err := r.impl.RegisterRouter(ctx, topology.RouterFromProto(request.Router))
	if err != nil {
		return nil, err
	}
	return &protos.AddRouterReply{
		Id: request.Router.Id,
	}, nil
}

// TODO : unit tests
func (r RouterService) RemoveRouter(ctx context.Context, request *protos.RemoveRouterRequest) (*emptypb.Empty, error) {
	spqrlog.Zero.Debug().
		Str("router-id", request.Id).
		Msg("unregister router in coordinator")
	err := r.impl.UnregisterRouter(ctx, request.Id)
	if err != nil {
		return nil, err
	}
	return nil, nil
}

// TODO : unit tests
func (r RouterService) SyncMetadata(ctx context.Context, request *protos.SyncMetadataRequest) (*emptypb.Empty, error) {
	spqrlog.Zero.Debug().
		Str("router-id", request.Router.Id).
		Msg("sync router metadata in coordinator")
	err := r.impl.SyncRouterMetadata(ctx, topology.RouterFromProto(request.Router))
	if err != nil {
		return nil, err
	}
	return nil, nil
}

var _ protos.RouterServiceServer = &RouterService{}

func NewRouterService(impl coordinator.Coordinator) *RouterService {
	return &RouterService{
		impl: impl,
	}
}
