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
func (r *TopologyService) OpenRouter(ctx context.Context, _ *emptypb.Empty) (*emptypb.Empty, error) {
	return nil, spqrerror.New(spqrerror.SPQR_NOT_IMPLEMENTED, "OpenRouter method unimplemented")
}

// TODO : implement
func (r *TopologyService) CloseRouter(ctx context.Context, _ *emptypb.Empty) (*emptypb.Empty, error) {
	return nil, spqrerror.New(spqrerror.SPQR_NOT_IMPLEMENTED, "CloseRouter method unimplemented")
}

// TODO : implement
func (r *TopologyService) UpdateCoordinator(ctx context.Context, in *protos.UpdateCoordinatorRequest) (*emptypb.Empty, error) {
	return nil, spqrerror.New(spqrerror.SPQR_NOT_IMPLEMENTED, "UpdateCoordinator method unimplemented")
}

func NewTopologyService(impl coordinator.Coordinator) *TopologyService {
	return &TopologyService{
		impl: impl,
	}
}
