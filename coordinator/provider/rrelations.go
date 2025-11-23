package provider

import (
	"context"

	"github.com/pg-sharding/spqr/coordinator"
	protos "github.com/pg-sharding/spqr/pkg/protos"
	"github.com/pg-sharding/spqr/router/rfqn"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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
		return nil, status.Error(codes.Internal, err.Error())
	}
	return nil, nil
}

func (rr *ReferenceRelationServer) DropReferenceRelations(ctx context.Context, req *protos.DropReferenceRelationsRequest) (*emptypb.Empty, error) {
	for _, qualName := range req.GetRelations() {
		if err := rr.impl.DropReferenceRelation(ctx, &rfqn.RelationFQN{
			RelationName: qualName.RelationName,
			SchemaName:   qualName.SchemaName,
		}); err != nil {
			return nil, err
		}
	}
	return nil, nil
}
