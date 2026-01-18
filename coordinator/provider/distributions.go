package provider

import (
	"context"
	"fmt"

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

func (d *DistributionsServer) CreateDistribution(ctx context.Context, req *protos.CreateDistributionRequest) (*protos.CreateDistributionReply, error) {
	reply := protos.CreateDistributionReply{MetaCmdList: make([]*protos.MetaTransactionGossipCommand, 0),
		CmdList: make([]*protos.QdbTransactionCmd, 0)}
	for _, ds := range req.Distributions {
		mds, err := distributions.DistributionFromProto(ds)
		if err != nil {
			return nil, err
		}
		if tranChunk, err := d.impl.CreateDistribution(ctx, mds); err != nil {
			return nil, err
		} else {
			if len(tranChunk.QdbStatements) == 0 {
				return nil, fmt.Errorf("transaction chunk must have a qdb statement (DistributionsServer.CreateDistribution)")
			}
			for _, qdbStmt := range tranChunk.QdbStatements {
				reply.CmdList = append(reply.CmdList, qdbStmt.ToProto())
			}
			reply.MetaCmdList = append(reply.MetaCmdList, tranChunk.GossipRequests...)
		}

	}
	return &reply, nil
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

	res := make([]*distributions.DistributedRelation, len(req.GetRelations()))
	for i, rel := range req.GetRelations() {
		var err error
		res[i], err = distributions.DistributedRelationFromProto(rel, map[string]*distributions.UniqueIndex{})
		if err != nil {
			return nil, err
		}
	}

	return nil, d.impl.AlterDistributionAttach(ctx, req.GetId(), res)
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
	ds, err := d.impl.GetRelationDistribution(ctx, &rfqn.RelationFQN{RelationName: req.Relation.Name, SchemaName: req.Relation.SchemaName})
	if err != nil {
		return nil, err
	}
	curRel, ok := ds.Relations[req.Relation.Name]
	if !ok {
		return nil, fmt.Errorf("relation \"%s\" not found in distribution \"%s\"", req.Relation.Name, ds.Id)
	}
	rel, err := distributions.DistributedRelationFromProto(req.GetRelation(), curRel.UniqueIndexesByColumn)
	if err != nil {
		return nil, err
	}
	return nil, d.impl.AlterDistributedRelation(ctx, req.GetId(), rel)
}

func (d *DistributionsServer) AlterDistributedRelationSchema(ctx context.Context, req *protos.AlterDistributedRelationSchemaRequest) (*emptypb.Empty, error) {
	return nil, d.impl.AlterDistributedRelationSchema(ctx, req.GetId(), req.GetRelationName(), req.GetSchemaName())
}

func (d *DistributionsServer) AlterDistributedRelationDistributionKey(ctx context.Context, req *protos.AlterDistributedRelationDistributionKeyRequest) (*emptypb.Empty, error) {
	key, err := distributions.DistributionKeyFromProto(req.GetDistributionKey())
	if err != nil {
		return nil, err
	}
	return nil, d.impl.AlterDistributedRelationDistributionKey(ctx, req.GetId(), req.GetRelationName(), key)
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

func (d *DistributionsServer) NextRange(ctx context.Context, req *protos.NextRangeRequest) (*protos.NextRangeReply, error) {
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

func (d *DistributionsServer) DropSequence(ctx context.Context, req *protos.DropSequenceRequest) (*emptypb.Empty, error) {
	return nil, d.impl.DropSequence(ctx, req.Name, req.Force)
}

func (d *DistributionsServer) ListRelationSequences(ctx context.Context, req *protos.ListRelationSequencesRequest) (*protos.ListRelationSequencesReply, error) {
	val, err := d.impl.ListRelationSequences(ctx, rfqn.RelationFQNFromFullName(req.SchemaName, req.Name))
	if err != nil {
		return nil, err
	}
	return &protos.ListRelationSequencesReply{ColumnSequences: val}, nil
}

func (d *DistributionsServer) ListSequences(ctx context.Context, _ *emptypb.Empty) (*protos.ListSequencesReply, error) {
	seqs, err := d.impl.ListSequences(ctx)
	if err != nil {
		return nil, err
	}
	return &protos.ListSequencesReply{Names: seqs}, nil
}

func (d *DistributionsServer) CreateUniqueIndex(ctx context.Context, req *protos.CreateUniqueIndexRequest) (*emptypb.Empty, error) {
	return nil, d.impl.CreateUniqueIndex(ctx, req.DistributionId, distributions.UniqueIndexFromProto(req.Idx))
}

func (d *DistributionsServer) DropUniqueIndex(ctx context.Context, req *protos.DropUniqueIndexRequest) (*emptypb.Empty, error) {
	return nil, d.impl.DropUniqueIndex(ctx, req.IdxId)
}

func (d *DistributionsServer) ListUniqueIndexes(ctx context.Context, _ *emptypb.Empty) (*protos.ListUniqueIndexesReply, error) {
	idxs, err := d.impl.ListUniqueIndexes(ctx)
	if err != nil {
		return nil, err
	}
	res := make(map[string]*protos.UniqueIndex)
	for id, idx := range idxs {
		res[id] = distributions.UniqueIndexToProto(idx)
	}
	return &protos.ListUniqueIndexesReply{Indexes: res}, nil
}

func (d *DistributionsServer) ListDistributionUniqueIndexes(ctx context.Context, req *protos.ListDistributionUniqueIndexesRequest) (*protos.ListUniqueIndexesReply, error) {
	idxs, err := d.impl.ListDistributionIndexes(ctx, req.DistributionId)
	if err != nil {
		return nil, err
	}
	res := make(map[string]*protos.UniqueIndex)
	for id, idx := range idxs {
		res[id] = distributions.UniqueIndexToProto(idx)
	}
	return &protos.ListUniqueIndexesReply{Indexes: res}, nil
}

func (d *DistributionsServer) ListRelationUniqueIndexes(ctx context.Context, req *protos.ListRelationUniqueIndexesRequest) (*protos.ListUniqueIndexesReply, error) {
	idxs, err := d.impl.ListRelationIndexes(ctx, &rfqn.RelationFQN{
		RelationName: req.RelationName})
	if err != nil {
		return nil, err
	}
	res := make(map[string]*protos.UniqueIndex)
	for id, idx := range idxs {
		res[id] = distributions.UniqueIndexToProto(idx)
	}
	return &protos.ListUniqueIndexesReply{Indexes: res}, nil
}
