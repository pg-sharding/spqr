package provider

import (
	"context"

	"github.com/pg-sharding/spqr/pkg/models/spqrerror"

	"github.com/pg-sharding/spqr/coordinator"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	protos "github.com/pg-sharding/spqr/pkg/protos"
)

type CoordinatorService struct {
	protos.UnimplementedKeyRangeServiceServer

	impl coordinator.Coordinator
}

// TODO : unit tests
func (c *CoordinatorService) AddKeyRange(ctx context.Context, request *protos.AddKeyRangeRequest) (*protos.ModifyReply, error) {
	err := c.impl.AddKeyRange(ctx, kr.KeyRangeFromProto(request.KeyRangeInfo))
	if err != nil {
		return nil, err
	}

	return &protos.ModifyReply{}, nil
}

// TODO : unit tests
func (c *CoordinatorService) LockKeyRange(ctx context.Context, request *protos.LockKeyRangeRequest) (*protos.ModifyReply, error) {
	for _, id := range request.Id {
		_, err := c.impl.LockKeyRange(ctx, id)
		if err != nil {
			return nil, err
		}
	}
	return &protos.ModifyReply{}, nil
}

// TODO : unit tests
func (c *CoordinatorService) UnlockKeyRange(ctx context.Context, request *protos.UnlockKeyRangeRequest) (*protos.ModifyReply, error) {
	for _, id := range request.Id {
		if err := c.impl.UnlockKeyRange(ctx, id); err != nil {
			return nil, err
		}
	}
	return &protos.ModifyReply{}, nil
}

// TODO : unit tests
func (c *CoordinatorService) SplitKeyRange(ctx context.Context, request *protos.SplitKeyRangeRequest) (*protos.ModifyReply, error) {
	splitKR := &kr.SplitKeyRange{
		Bound:     request.Bound,
		Krid:      request.NewId,
		SourceID:  request.SourceId,
		SplitLeft: request.SplitLeft,
	}

	if err := c.impl.Split(ctx, splitKR); err != nil {
		return nil, err
	}

	return &protos.ModifyReply{}, nil
}

// TODO : unit tests
func (c *CoordinatorService) ListKeyRange(ctx context.Context, request *protos.ListKeyRangeRequest) (*protos.KeyRangeReply, error) {

	krsqb, err := c.impl.ListKeyRanges(ctx, request.Distribution)
	if err != nil {
		return nil, err
	}

	var krs []*protos.KeyRangeInfo

	for _, keyRange := range krsqb {
		krs = append(krs, keyRange.ToProto())
	}

	return &protos.KeyRangeReply{
		KeyRangesInfo: krs,
	}, nil
}

func (c *CoordinatorService) ListAllKeyRanges(ctx context.Context, _ *protos.ListAllKeyRangesRequest) (*protos.KeyRangeReply, error) {
	krsDb, err := c.impl.ListAllKeyRanges(ctx)
	if err != nil {
		return nil, err
	}

	krs := make([]*protos.KeyRangeInfo, len(krsDb))

	for i, krg := range krsDb {
		krs[i] = krg.ToProto()
	}

	return &protos.KeyRangeReply{KeyRangesInfo: krs}, nil
}

// TODO : unit tests
func (c *CoordinatorService) MoveKeyRange(ctx context.Context, request *protos.MoveKeyRangeRequest) (*protos.ModifyReply, error) {
	if err := c.impl.Move(ctx, &kr.MoveKeyRange{
		Krid:    request.Id,
		ShardId: request.ToShardId,
	}); err != nil {
		return nil, err
	}

	return &protos.ModifyReply{}, nil
}

// TODO : unit tests
func (c *CoordinatorService) MergeKeyRange(ctx context.Context, request *protos.MergeKeyRangeRequest) (*protos.ModifyReply, error) {
	if err := c.impl.Unite(ctx, &kr.UniteKeyRange{
		BaseKeyRangeId:      request.GetBaseId(),
		AppendageKeyRangeId: request.GetAppendageId(),
	}); err != nil {
		return nil, spqrerror.Newf(spqrerror.SPQR_KEYRANGE_ERROR, "failed to unite key ranges: %s", err.Error())
	}

	return &protos.ModifyReply{}, nil
}

var _ protos.KeyRangeServiceServer = &CoordinatorService{}

func NewKeyRangeService(impl coordinator.Coordinator) protos.KeyRangeServiceServer {
	return &CoordinatorService{
		impl: impl,
	}
}
