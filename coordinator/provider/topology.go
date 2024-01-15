package provider

import (
	context "context"
	"fmt"

	"github.com/pg-sharding/spqr/coordinator"
	protos "github.com/pg-sharding/spqr/pkg/protos"
)

type TopologyService struct {
	protos.UnimplementedTopologyServiceServer

	impl coordinator.Coordinator
}

// TODO : implement
func (r *TopologyService) OpenRouter(ctx context.Context, request *protos.OpenRouterRequest) (*protos.OpenRouterReply, error) {
	return nil, fmt.Errorf("unimplemented")
}

// TODO : implement
func (r *TopologyService) CloseRouter(ctx context.Context, request *protos.CloseRouterRequest) (*protos.CloseRouterReply, error) {
	return nil, fmt.Errorf("unimplemented")
}

// TODO : implement
func (r *TopologyService) UpdateCoordinator(ctx context.Context, in *protos.UpdateCoordinatorRequest) (*protos.UpdateCoordinatorResponse, error) {
	return nil, fmt.Errorf("unimplemented")
}

func NewTopologyService(impl coordinator.Coordinator) *TopologyService {
	return &TopologyService{
		impl: impl,
	}
}
