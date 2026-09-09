package provider

import (
	"context"
	"fmt"

	"github.com/pg-sharding/spqr/coordinator"
	protos "github.com/pg-sharding/spqr/pkg/protos"
	"github.com/pg-sharding/spqr/router/rfqn"
	"google.golang.org/protobuf/types/known/emptypb"

	rrelations "github.com/pg-sharding/spqr/pkg/models/rrelation"
)

type ReferenceRelationServer struct {
	protos.UnimplementedReferenceRelationsServiceServer

	impl coordinator.Coordinator
}

var _ protos.ReferenceRelationsServiceServer = &ReferenceRelationServer{}

func NewReferenceRelationServer(impl coordinator.Coordinator) *ReferenceRelationServer {
	return &ReferenceRelationServer{
		impl: impl,
	}
}

func (rr *ReferenceRelationServer) CreateReferenceRelations(ctx context.Context, req *protos.CreateReferenceRelationsRequest) (*emptypb.Empty, error) {
	return nil, rr.impl.CreateReferenceRelation(ctx,
		rrelations.RefRelationFromProto(req.Relation),
		rrelations.AutoIncrementEntriesFromProto(req.Entries))
}

func (rr *ReferenceRelationServer) DropReferenceRelations(ctx context.Context, req *protos.DropReferenceRelationsRequest) (*emptypb.Empty, error) {
	for _, qualName := range req.GetRelations() {
		if err := rr.impl.DropReferenceRelation(ctx, rfqn.RelationFQNFromProto(qualName)); err != nil {
			return nil, err
		}
	}
	return nil, nil
}

func (rr *ReferenceRelationServer) AlterReferenceRelationStorageAdvanced(ctx context.Context, req *protos.AlterReferenceRelationStorageRequest) (*emptypb.Empty, error) {
	return nil, rr.impl.AlterReferenceRelationStorage(ctx, rfqn.RelationFQNFromProto(req.Relation), req.ShardIds)
}

func (rr *ReferenceRelationServer) ListReferenceRelations(ctx context.Context, _ *emptypb.Empty) (*protos.ListReferenceRelationsReply, error) {
	rrels, err := rr.impl.ListReferenceRelations(ctx)
	if err != nil {
		return nil, err
	}
	relsProto := make([]*protos.ReferenceRelation, len(rrels))
	for i, rel := range rrels {
		relsProto[i] = rrelations.RefRelationToProto(rel)
	}
	return &protos.ListReferenceRelationsReply{Relations: relsProto}, nil
}

// AlterReferenceRelationStorage implements [proto.ReferenceRelationsServiceServer].
func (rr *ReferenceRelationServer) AlterReferenceRelationStorage(context.Context, *protos.AlterReferenceRelationStorageRequest) (*emptypb.Empty, error) {
	return nil, fmt.Errorf("AlterReferenceRelationStorage is unsupported in coordinator")
}

// SyncReferenceRelations implements [proto.ReferenceRelationsServiceServer].
func (rr *ReferenceRelationServer) SyncReferenceRelations(context.Context, *protos.SyncReferenceRelationsRequest) (*emptypb.Empty, error) {
	return nil, fmt.Errorf("SyncReferenceRelations is unsupported in coordinator")
}
