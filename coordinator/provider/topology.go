package provider

import (
	"context"
	"github.com/pg-sharding/spqr/pkg/models/spqrerror"

	"github.com/pg-sharding/spqr/coordinator"
	protos "github.com/pg-sharding/spqr/pkg/protos"
)

type TopologyService struct {
	protos.UnimplementedTopologyServiceServer

	impl coordinator.Coordinator
}

// TODO : implement
func (r *TopologyService) OpenRouter(ctx context.Context, request *protos.OpenRouterRequest) (*protos.OpenRouterReply, error) {
	return nil, spqrerror.New(spqrerror.SPQR_NOT_IMPLEMENTED, "OpenRouter method unimplemented")
}

// TODO : implement
func (r *TopologyService) CloseRouter(ctx context.Context, request *protos.CloseRouterRequest) (*protos.CloseRouterReply, error) {
	return nil, spqrerror.New(spqrerror.SPQR_NOT_IMPLEMENTED, "CloseRouter method unimplemented")
}

// TODO : implement
func (r *TopologyService) UpdateCoordinator(ctx context.Context, in *protos.UpdateCoordinatorRequest) (*protos.UpdateCoordinatorResponse, error) {
	return nil, spqrerror.New(spqrerror.SPQR_NOT_IMPLEMENTED, "UpdateCoordinator method unimplemented")
}

func NewTopologyService(impl coordinator.Coordinator) *TopologyService {
	return &TopologyService{
		impl: impl,
	}
}
