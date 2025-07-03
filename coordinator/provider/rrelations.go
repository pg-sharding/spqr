package provider

import (
	"context"

	"github.com/pg-sharding/spqr/coordinator"
	protos "github.com/pg-sharding/spqr/pkg/protos"
	"google.golang.org/protobuf/types/known/emptypb"

	rrelations "github.com/pg-sharding/spqr/pkg/models/rrelation"
)

type ReferenceRelationServer struct {
	protos.UnimplementedReferenceRelationsServiceServer

	impl coordinator.Coordinator
}

func NewReferenceRelationServer(impl coordinator.Coordinator) *ReferenceRelationServer {
	return &ReferenceRelationServer{
		impl: impl,
	}
}

func (rr *ReferenceRelationServer) CreateReferenceRelations(ctx context.Context, req *protos.CreateReferenceRelationsRequest) (*emptypb.Empty, error) {
	if err := rr.impl.CreateReferenceRelation(ctx,
		rrelations.RefRelationFromProto(req.Relation),
		rrelations.AutoIncrementEntriesFromProto(req.Entries)); err != nil {
		return nil, err
	}
	return nil, nil
}

func (rr *ReferenceRelationServer) DropReferenceRelations(ctx context.Context, req *protos.DropReferenceRelationsRequest) (*emptypb.Empty, error) {
	for _, id := range req.GetIds() {
		if err := rr.impl.DropReferenceRelation(ctx, id); err != nil {
			return nil, err
		}
	}
	return nil, nil
}
