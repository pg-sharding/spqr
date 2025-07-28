package provider

import (
	"context"

	"github.com/pg-sharding/spqr/coordinator"
	"github.com/pg-sharding/spqr/pkg/models/distributions"
	protos "github.com/pg-sharding/spqr/pkg/protos"
	"github.com/pg-sharding/spqr/router/rfqn"
	"google.golang.org/protobuf/types/known/emptypb"
)

type DistributionsServer struct {
	protos.UnimplementedDistributionServiceServer

	impl coordinator.Coordinator
}

func NewDistributionServer(impl coordinator.Coordinator) *DistributionsServer {
	return &DistributionsServer{
		impl: impl,
	}
}

var _ protos.DistributionServiceServer = &DistributionsServer{}

func (d *DistributionsServer) CreateDistribution(ctx context.Context, req *protos.CreateDistributionRequest) (*emptypb.Empty, error) {
	for _, ds := range req.Distributions {
		if err := d.impl.CreateDistribution(ctx, distributions.DistributionFromProto(ds)); err != nil {
			return nil, err
		}
	}
	return nil, nil
}

func (d *DistributionsServer) DropDistribution(ctx context.Context, req *protos.DropDistributionRequest) (*emptypb.Empty, error) {
	for _, id := range req.GetIds() {
		if err := d.impl.DropDistribution(ctx, id); err != nil {
			return nil, err
		}
	}
	return nil, nil
}

func (d *DistributionsServer) ListDistributions(ctx context.Context, _ *emptypb.Empty) (*protos.ListDistributionsReply, error) {
	dss, err := d.impl.ListDistributions(ctx)
	if err != nil {
		return nil, err
	}
	return &protos.ListDistributionsReply{
		Distributions: func() []*protos.Distribution {
			res := make([]*protos.Distribution, len(dss))
			for i, ds := range dss {
				res[i] = distributions.DistributionToProto(ds)
			}
			return res
		}(),
	}, nil
}

func (d *DistributionsServer) AlterDistributionAttach(ctx context.Context, req *protos.AlterDistributionAttachRequest) (*emptypb.Empty, error) {
	return nil, d.impl.AlterDistributionAttach(ctx, req.GetId(), func() []*distributions.DistributedRelation {
		res := make([]*distributions.DistributedRelation, len(req.GetRelations()))
		for i, rel := range req.GetRelations() {
			res[i] = distributions.DistributedRelationFromProto(rel)
		}
		return res
	}())
}

func (d *DistributionsServer) AlterDistributionDetach(ctx context.Context, req *protos.AlterDistributionDetachRequest) (*emptypb.Empty, error) {
	for _, rel := range req.GetRelNames() {
		qualifiedName := &rfqn.RelationFQN{RelationName: rel.RelationName, SchemaName: rel.SchemaName}
		if err := d.impl.AlterDistributionDetach(ctx, req.GetId(), qualifiedName); err != nil {
			return nil, err
		}
	}
	return nil, nil
}

func (d *DistributionsServer) AlterDistributedRelation(ctx context.Context, req *protos.AlterDistributedRelationRequest) (*emptypb.Empty, error) {
	err := d.impl.AlterDistributedRelation(ctx, req.GetId(), distributions.DistributedRelationFromProto(req.GetRelation()))
	return nil, err
}

func (d *DistributionsServer) GetDistribution(ctx context.Context, req *protos.GetDistributionRequest) (*protos.GetDistributionReply, error) {
	ds, err := d.impl.GetDistribution(ctx, req.GetId())
	if err != nil {
		return nil, err
	}
	return &protos.GetDistributionReply{Distribution: distributions.DistributionToProto(ds)}, nil
}

func (d *DistributionsServer) GetRelationDistribution(ctx context.Context, req *protos.GetRelationDistributionRequest) (*protos.GetRelationDistributionReply, error) {
	qualifiedName := rfqn.RelationFQN{RelationName: req.Name, SchemaName: req.SchemaName}
	ds, err := d.impl.GetRelationDistribution(ctx, &qualifiedName)
	if err != nil {
		return nil, err
	}
	return &protos.GetRelationDistributionReply{Distribution: distributions.DistributionToProto(ds)}, nil
}

func (d *DistributionsServer) NextVal(ctx context.Context, req *protos.NextRangeRequest) (*protos.NextRangeReply, error) {
	val, err := d.impl.NextRange(ctx, req.Seq, uint64(req.RangeSize))
	if err != nil {
		return nil, err
	}
	return &protos.NextRangeReply{Left: val.Left, Right: val.Right}, nil
}

func (d *DistributionsServer) CurrVal(ctx context.Context, req *protos.CurrValRequest) (*protos.CurrValReply, error) {
	val, err := d.impl.CurrVal(ctx, req.Seq)
	if err != nil {
		return nil, err
	}
	return &protos.CurrValReply{Value: val}, nil
}
