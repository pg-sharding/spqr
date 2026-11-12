package provider

import (
	"context"
	"fmt"
	"sort"

	"github.com/pg-sharding/spqr/pkg/models/tasks"

	"github.com/pg-sharding/spqr/pkg/models/spqrerror"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/pg-sharding/spqr/coordinator"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	protos "github.com/pg-sharding/spqr/pkg/protos"
	"github.com/pg-sharding/spqr/qdb"
)

type CoordinatorService struct {
	protos.UnimplementedKeyRangeServiceServer

	impl coordinator.Coordinator
}

// DropAllKeyRanges implements proto.KeyRangeServiceServer.
func (c *CoordinatorService) DropAllKeyRanges(ctx context.Context, _ *emptypb.Empty) (*protos.DropAllKeyRangesResponse, error) {
	krs, err := c.impl.ListAllKeyRanges(ctx)
	if err != nil {
		return nil, err
	}
	ids := make([]string, 0, len(krs))
	for _, keyRange := range krs {
		ids = append(ids, keyRange.ID)
	}
	operationLocks, err := c.lockKeyRangeOperations(ctx, ids, true)
	if err != nil {
		return nil, err
	}
	defer c.releaseKeyRangeOperations(ctx, operationLocks)

	if err := c.impl.DropKeyRangeAll(ctx); err != nil {
		return nil, err
	}

	return &protos.DropAllKeyRangesResponse{}, nil
}

// DEPRECATED
func (c *CoordinatorService) DropKeyRange(_ context.Context, _ *protos.DropKeyRangeRequest) (*protos.ModifyReply, error) {
	return nil, fmt.Errorf("DEPRECATED (CreateKeyRange in CoordinatorService). Use ExecuteNoTran or CommitTran")
}

// DEPRECATED
func (c *CoordinatorService) CreateKeyRange(_ context.Context, _ *protos.CreateKeyRangeRequest) (*protos.ModifyReply, error) {
	return nil, fmt.Errorf("DEPRECATED (CreateKeyRange in CoordinatorService). Use ExecuteNoTran or CommitTran")
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

func (c *CoordinatorService) UnlockKeyRange(ctx context.Context, request *protos.UnlockKeyRangeRequest) (*protos.ModifyReply, error) {
	for _, id := range request.Id {
		keyRange, err := c.impl.GetKeyRange(ctx, id)
		if err != nil {
			return nil, err
		}
		// ClusteredCoordinator propagates an unlock after it has already
		// released the shared QDB lock. Treat that as an idempotent router update.
		if keyRange != nil && !keyRange.IsLocked {
			continue
		}

		operationLocks, err := c.lockKeyRangeOperations(ctx, []string{id}, false)
		if err != nil {
			return nil, err
		}
		if err := c.impl.UnlockKeyRange(ctx, id); err != nil {
			c.releaseKeyRangeOperations(ctx, operationLocks)
			return nil, err
		}
		if err := c.releaseKeyRangeOperations(ctx, operationLocks); err != nil {
			return nil, err
		}
	}
	return &protos.ModifyReply{}, nil
}

func (c *CoordinatorService) lockKeyRangeOperations(ctx context.Context, ids []string, rejectDataLock bool) ([]string, error) {
	sort.Strings(ids)
	locked := make([]string, 0, len(ids))
	for _, id := range ids {
		keyRange, err := c.impl.QDB().LockKeyRangeOps(ctx, id)
		if err != nil {
			_ = c.releaseKeyRangeOperations(ctx, locked)
			return nil, err
		}
		locked = append(locked, id)

		moveLister, ok := c.impl.QDB().(interface {
			ListKeyRangeMoves(context.Context) ([]*qdb.MoveKeyRange, error)
		})
		if !ok {
			_ = c.releaseKeyRangeOperations(ctx, locked)
			return nil, fmt.Errorf("QDB does not support key range move lookup")
		}
		moves, err := moveLister.ListKeyRangeMoves(ctx)
		if err != nil {
			_ = c.releaseKeyRangeOperations(ctx, locked)
			return nil, err
		}
		for _, move := range moves {
			if move.KeyRangeID == id {
				_ = c.releaseKeyRangeOperations(ctx, locked)
				return nil, spqrerror.Newf(spqrerror.SPQR_KEYRANGE_ERROR,
					"cannot modify key range %q while move task %q is unfinished", id, move.MoveId)
			}
		}
		if rejectDataLock && keyRange.Locked {
			_ = c.releaseKeyRangeOperations(ctx, locked)
			return nil, spqrerror.Newf(spqrerror.SPQR_KEYRANGE_ERROR, "cannot drop key range %q while it is locked", id)
		}
	}
	return locked, nil
}

func (c *CoordinatorService) releaseKeyRangeOperations(ctx context.Context, ids []string) error {
	var firstErr error
	for i := len(ids) - 1; i >= 0; i-- {
		if err := c.impl.QDB().UnlockKeyRangeOps(ctx, ids[i]); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// TODO : unit tests
func (c *CoordinatorService) SplitKeyRange(ctx context.Context, request *protos.SplitKeyRangeRequest) (*protos.ModifyReply, error) {
	splitKR := &kr.SplitKeyRange{
		Bound:      [][]byte{request.Bound}, // fix multidim case
		KeyRangeID: request.NewId,
		SourceID:   request.SourceId,
		SplitLeft:  request.SplitLeft,
	}

	if err := c.impl.Split(ctx, splitKR); err != nil {
		return nil, err
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
		KeyRangeID: request.Id,
		ShardID:    request.ToShardId,
		MetaOnly:   request.MetaOnly,
	}, nil); err != nil {
		return nil, err
	}

	return &protos.ModifyReply{}, nil
}

// TODO : unit tests
func (c *CoordinatorService) MergeKeyRange(ctx context.Context, request *protos.MergeKeyRangeRequest) (*protos.ModifyReply, error) {
	if err := c.impl.Unite(ctx, &kr.UniteKeyRange{
		BaseKeyRangeID:      request.GetBaseId(),
		AppendageKeyRangeID: request.GetAppendageId(),
	}); err != nil {
		return nil, spqrerror.New(spqrerror.SPQR_KEYRANGE_ERROR, err.Error())
	}

	return &protos.ModifyReply{}, nil
}

// TODO: unit tests
func (c *CoordinatorService) BatchMoveKeyRange(ctx context.Context, request *protos.BatchMoveKeyRangeRequest) (*emptypb.Empty, error) {
	return nil, c.impl.BatchMoveKeyRange(ctx, &kr.BatchMoveKeyRange{
		TaskGroupID:    request.TaskGroupId,
		KeyRangeID:     request.KeyRangeId,
		DestKeyRangeID: request.ToKrId,
		ShardID:        request.ToShardId,
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
	}, tasks.TaskGroupIssuerFromProto(request.Issuer), nil)
}

// TODO: unit tests
func (c *CoordinatorService) RedistributeKeyRange(ctx context.Context, request *protos.RedistributeKeyRangeRequest) (*emptypb.Empty, error) {
	return nil, c.impl.RedistributeKeyRange(ctx, &kr.RedistributeKeyRange{
		TaskGroupID: request.TaskGroupId,
		KeyRangeID:  request.Krid,
		ShardID:     request.ShardId,
		BatchSize:   int(request.BatchSize),
		Check:       request.Check,
		Apply:       request.Apply,
		NoWait:      request.NoWait,
	}, nil)
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
