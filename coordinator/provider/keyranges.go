package provider

import (
	"context"

	"github.com/pg-sharding/spqr/pkg/models/tasks"

	"github.com/pg-sharding/spqr/pkg/models/spqrerror"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/pg-sharding/spqr/coordinator"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	protos "github.com/pg-sharding/spqr/pkg/protos"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type CoordinatorService struct {
	protos.UnimplementedKeyRangeServiceServer

	impl coordinator.Coordinator
}

// DropAllKeyRanges implements proto.KeyRangeServiceServer.
func (c *CoordinatorService) DropAllKeyRanges(ctx context.Context, request *emptypb.Empty) (*protos.DropAllKeyRangesResponse, error) {
	err := c.impl.DropKeyRangeAll(ctx)
	if err != nil {
		return nil, err
	}

	return &protos.DropAllKeyRangesResponse{}, nil
}

// DropKeyRange implements proto.KeyRangeServiceServer.
func (c *CoordinatorService) DropKeyRange(ctx context.Context, request *protos.DropKeyRangeRequest) (*protos.ModifyReply, error) {
	for _, id := range request.Id {
		err := c.impl.DropKeyRange(ctx, id)
		if err != nil {
			return nil, err
		}
	}

	return &protos.ModifyReply{}, nil
}

// TODO : unit tests
func (c *CoordinatorService) CreateKeyRange(ctx context.Context, request *protos.CreateKeyRangeRequest) (*protos.ModifyReply, error) {
	ds, err := c.impl.GetDistribution(ctx, request.KeyRangeInfo.DistributionId)
	if err != nil {
		return nil, status.Error(codes.NotFound, err.Error())
	}
	protoKR, err := kr.KeyRangeFromProto(request.KeyRangeInfo, ds.ColTypes)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	err = c.impl.CreateKeyRange(ctx, protoKR)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
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
		Bound:     [][]byte{request.Bound}, // fix multidim case
		Krid:      request.NewId,
		SourceID:  request.SourceId,
		SplitLeft: request.SplitLeft,
	}

	if err := c.impl.Split(ctx, splitKR); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	return &protos.ModifyReply{}, nil
}

// GetKeyRange gets key ranges with given ids
// TODO unit tests
func (c *CoordinatorService) GetKeyRange(ctx context.Context, request *protos.GetKeyRangeRequest) (*protos.KeyRangeReply, error) {
	res := make([]*protos.KeyRangeInfo, 0)
	for _, id := range request.Ids {
		krg, err := c.impl.GetKeyRange(ctx, id)
		if err != nil {
			return nil, err
		}
		if krg != nil {
			res = append(res, krg.ToProto())
		}
	}
	return &protos.KeyRangeReply{KeyRangesInfo: res}, nil
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

func (c *CoordinatorService) ListAllKeyRanges(ctx context.Context, _ *emptypb.Empty) (*protos.KeyRangeReply, error) {
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

func (c *CoordinatorService) ListKeyRangeLocks(ctx context.Context, _ *emptypb.Empty) (*protos.KeyRangeLocksReply, error) {
	krLocks, err := c.impl.ListKeyRangeLocks(ctx)
	if err != nil {
		return nil, err
	}

	return &protos.KeyRangeLocksReply{KeyRangesLocks: krLocks}, nil
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
		spqrError := spqrerror.Newf(spqrerror.SPQR_KEYRANGE_ERROR, "failed to unite key ranges: %s", err.Error())
		return nil, status.Error(codes.Internal, spqrError.Error())
	}

	return &protos.ModifyReply{}, nil
}

// TODO: unit tests
func (c *CoordinatorService) BatchMoveKeyRange(ctx context.Context, request *protos.BatchMoveKeyRangeRequest) (*emptypb.Empty, error) {
	return nil, c.impl.BatchMoveKeyRange(ctx, &kr.BatchMoveKeyRange{
		KrId:     request.Id,
		DestKrId: request.ToKrId,
		ShardId:  request.ToShardId,
		Limit: func() int64 {
			switch request.LimitType {
			case protos.RedistributeLimitType_RedistributeAllKeys:
				return -1
			case protos.RedistributeLimitType_RedistributeKeysLimit:
				return request.Limit
			default:
				panic("unknown redistribution key limit")
			}
		}(),
		BatchSize: int(request.BatchSize),
		Type: func() tasks.SplitType {
			switch request.SplitType {
			case protos.SplitType_SplitLeft:
				return tasks.SplitLeft
			case protos.SplitType_SplitRight:
				return tasks.SplitRight
			default:
				panic("incorrect split type")
			}
		}(),
	})
}

// TODO: unit tests
func (c *CoordinatorService) RedistributeKeyRange(ctx context.Context, request *protos.RedistributeKeyRangeRequest) (*emptypb.Empty, error) {
	return nil, c.impl.RedistributeKeyRange(ctx, &kr.RedistributeKeyRange{
		KrId:      request.Id,
		ShardId:   request.ShardId,
		BatchSize: int(request.BatchSize),
		Check:     request.Check,
		Apply:     request.Apply,
	})
}

// TODO: unit tests
func (c *CoordinatorService) RenameKeyRange(ctx context.Context, request *protos.RenameKeyRangeRequest) (*emptypb.Empty, error) {
	return nil, c.impl.RenameKeyRange(ctx, request.KeyRangeId, request.NewKeyRangeId)
}

var _ protos.KeyRangeServiceServer = &CoordinatorService{}

func NewKeyRangeService(impl coordinator.Coordinator) protos.KeyRangeServiceServer {
	return &CoordinatorService{
		impl: impl,
	}
}
