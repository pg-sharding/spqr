package provider

import (
	context "context"
	"github.com/pg-sharding/spqr/coordinator"
	shards "github.com/pg-sharding/spqr/router/protos"
)

type RouterService struct {
	shards.UnimplementedRoutersServiceServer

	impl coordinator.Coordinator
}

func (r RouterService) ListRouters(ctx context.Context, request *shards.ListRoutersRequest) (*shards.ListRoutersReply, error) {
	panic("implement me")
}

func (r RouterService) AddRouters(ctx context.Context, request *shards.AddRoutersRequest) (*shards.AddRoutersReply, error) {
	panic("implement me")
}

func (r RouterService) ShutdownRouter(ctx context.Context, request *shards.ShutdownRouterRequest) (*shards.ShutdownRouterReply, error) {
	panic("implement me")
}


func NewRoutersService(impl coordinator.Coordinator) *RouterService {
	return &RouterService{
		impl: impl,
	}
}