package provider

import (
	"context"

	"github.com/pg-sharding/spqr/pkg/models/spqrerror"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/pg-sharding/spqr/coordinator"
	protos "github.com/pg-sharding/spqr/pkg/protos"
)

type TopologyService struct {
	protos.UnimplementedTopologyServiceServer

	impl coordinator.Coordinator
}

// TODO : implement
func (r *TopologyService) OpenRouter(_ context.Context, _ *emptypb.Empty) (*emptypb.Empty, error) {
	return nil, spqrerror.New(spqrerror.SPQR_NOT_IMPLEMENTED, "OpenRouter method not implemented")
}

// TODO : implement
func (r *TopologyService) CloseRouter(_ context.Context, _ *emptypb.Empty) (*emptypb.Empty, error) {
	return nil, spqrerror.New(spqrerror.SPQR_NOT_IMPLEMENTED, "CloseRouter method not implemented")
}

// TODO : implement
func (r *TopologyService) UpdateCoordinator(_ context.Context, _ *protos.UpdateCoordinatorRequest) (*emptypb.Empty, error) {
	return nil, spqrerror.New(spqrerror.SPQR_NOT_IMPLEMENTED, "UpdateCoordinator method not implemented")
}

func NewTopologyService(impl coordinator.Coordinator) *TopologyService {
	return &TopologyService{
		impl: impl,
	}
}
