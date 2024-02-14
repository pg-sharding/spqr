package provider

import (
	"context"
	"github.com/pg-sharding/spqr/coordinator"
	"github.com/pg-sharding/spqr/pkg/models/distributions"
	protos "github.com/pg-sharding/spqr/pkg/protos"
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

func (d *DistributionsServer) CreateDistribution(ctx context.Context, req *protos.CreateDistributionRequest) (*protos.CreateDistributionReply, error) {
	for _, ds := range req.Distributions {
		if err := d.impl.CreateDistribution(ctx, distributions.DistributionFromProto(ds)); err != nil {
			return nil, err
		}
	}
	return &protos.CreateDistributionReply{}, nil
}

func (d *DistributionsServer) DropDistribution(ctx context.Context, req *protos.DropDistributionRequest) (*protos.DropDistributionReply, error) {
	for _, id := range req.GetIds() {
		if err := d.impl.DropDistribution(ctx, id); err != nil {
			return nil, err
		}
	}
	return &protos.DropDistributionReply{}, nil
}

func (d *DistributionsServer) ListDistributions(ctx context.Context, req *protos.ListDistributionsRequest) (*protos.ListDistributionsReply, error) {
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

func (d *DistributionsServer) AlterDistributionAttach(ctx context.Context, req *protos.AlterDistributionAttachRequest) (*protos.AlterDistributionAttachReply, error) {
	return &protos.AlterDistributionAttachReply{}, d.impl.AlterDistributionAttach(ctx, req.GetId(), func() []*distributions.DistributedRelation {
		res := make([]*distributions.DistributedRelation, len(req.GetRelations()))
		for i, rel := range req.GetRelations() {
			res[i] = distributions.DistributedRelationFromProto(rel)
		}
		return res
	}())
}

func (d *DistributionsServer) AlterDistributionDetach(ctx context.Context, req *protos.AlterDistributionDetachRequest) (*protos.AlterDistributionDetachReply, error) {
	for _, rel := range req.GetRelNames() {
		if err := d.impl.AlterDistributionDetach(ctx, req.GetId(), rel); err != nil {
			return nil, err
		}
	}
	return &protos.AlterDistributionDetachReply{}, nil
}

func (d *DistributionsServer) GetDistribution(ctx context.Context, req *protos.GetDistributionRequest) (*protos.GetDistributionReply, error) {
	ds, err := d.impl.GetDistribution(ctx, req.GetId())
	if err != nil {
		return nil, err
	}
	return &protos.GetDistributionReply{Distribution: distributions.DistributionToProto(ds)}, nil
}

func (d *DistributionsServer) GetRelationDistribution(ctx context.Context, req *protos.GetRelationDistributionRequest) (*protos.GetRelationDistributionReply, error) {
	ds, err := d.impl.GetRelationDistribution(ctx, req.GetId())
	if err != nil {
		return nil, err
	}
	return &protos.GetRelationDistributionReply{Distribution: distributions.DistributionToProto(ds)}, nil
}
