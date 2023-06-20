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

func (r *TopologyService) OpenRouter(ctx context.Context, request *protos.OpenRouterRequest) (*protos.OpenRouterReply, error) {
	return nil, fmt.Errorf("implement OpenRouter")
}

func (r *TopologyService) CloseRouter(ctx context.Context, request *protos.CloseRouterRequest) (*protos.CloseRouterReply, error) {
	return nil, fmt.Errorf("implemented CloseRouter")
}

func NewTopologyService(impl coordinator.Coordinator) *TopologyService {
	return &TopologyService{
		impl: impl,
	}
}
