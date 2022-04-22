package provider

import (
	"context"
	"errors"

	"github.com/wal-g/tracelog"

	"github.com/pg-sharding/spqr/coordinator"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	protos "github.com/pg-sharding/spqr/router/protos"
)

type CoordinatorService struct {
	protos.UnimplementedKeyRangeServiceServer

	impl coordinator.Coordinator
}

func (c CoordinatorService) AddKeyRange(ctx context.Context, request *protos.AddKeyRangeRequest) (*protos.ModifyReply, error) {
	err := c.impl.AddKeyRange(ctx, &kr.KeyRange{
		LowerBound: []byte(request.KeyRangeInfo.KeyRange.LowerBound),
		UpperBound: []byte(request.KeyRangeInfo.KeyRange.UpperBound),
		ID:         request.KeyRangeInfo.Krid,
		ShardID:    request.KeyRangeInfo.ShardId,
	})
	if err != nil {
		return nil, err
	}

	return &protos.ModifyReply{}, nil
}

func (c CoordinatorService) LockKeyRange(ctx context.Context, request *protos.LockKeyRangeRequest) (*protos.ModifyReply, error) {
	keyRangeID, err := c.getKeyRangeIDByBounds(ctx, request.GetKeyRange())
	if err != nil {
		return nil, err
	}

	_, err = c.impl.Lock(ctx, keyRangeID)
	return &protos.ModifyReply{}, err
}

func (c CoordinatorService) UnlockKeyRange(ctx context.Context, request *protos.UnlockKeyRangeRequest) (*protos.ModifyReply, error) {
	keyRangeID, err := c.getKeyRangeIDByBounds(ctx, request.GetKeyRange())
	if err != nil {
		return nil, err
	}

	err = c.impl.Unlock(ctx, keyRangeID)
	return &protos.ModifyReply{}, err
}

func (c CoordinatorService) getKeyRangeIDByBounds(ctx context.Context, keyRange *protos.KeyRange) (string, error) {
	krsqb, err := c.impl.ListKeyRange(ctx)
	if err != nil {
		return "", err
	}

	// TODO: choose a key range without matching to exact bounds.
	for _, krqb := range krsqb {
		if string(krqb.LowerBound) == keyRange.GetLowerBound() &&
			string(krqb.UpperBound) == keyRange.GetUpperBound() {
			return krqb.ID, nil
		}
	}

	return "", errors.New("no found key range")
}

func (c CoordinatorService) SplitKeyRange(ctx context.Context, request *protos.SplitKeyRangeRequest) (*protos.ModifyReply, error) {
	err := c.impl.Split(ctx, &kr.SplitKeyRange{
		Bound: request.Bound,
	})
	if err != nil {
		return nil, err
	}

	return &protos.ModifyReply{}, nil
}

func (c CoordinatorService) ListKeyRange(ctx context.Context, request *protos.ListKeyRangeRequest) (*protos.KeyRangeReply, error) {
	// TODO:
	tracelog.InfoLogger.Printf("Coordinator Service %v %T %#v", c.impl, c.impl, c.impl.(*qdbCoordinator).db)

	if c.impl == nil {
		return &protos.KeyRangeReply{}, nil
	}

	//krsqb, err := c.impl.(*qdbCoordinator).db.ListKeyRange(ctx)
	//if err != nil {
	//	return nil, err
	//}

	krsqb, err := c.impl.ListKeyRange(ctx)
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

var _ protos.KeyRangeServiceServer = CoordinatorService{}

func NewKeyRangeService(impl coordinator.Coordinator) protos.KeyRangeServiceServer {
	return &CoordinatorService{
		impl: impl,
	}
}
