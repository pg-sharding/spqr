package provider

import (
	"bytes"
	"context"
	"fmt"
	"github.com/pg-sharding/spqr/pkg/spqrlog"

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
	keyRangeID, err := c.KeyRangeIDByBounds(ctx, request.GetKeyRange())
	if err != nil {
		return nil, err
	}

	_, err = c.impl.Lock(ctx, keyRangeID)
	return &protos.ModifyReply{}, err
}

func (c CoordinatorService) UnlockKeyRange(ctx context.Context, request *protos.UnlockKeyRangeRequest) (*protos.ModifyReply, error) {
	keyRangeID, err := c.KeyRangeIDByBounds(ctx, request.GetKeyRange())
	if err != nil {
		return nil, err
	}

	err = c.impl.Unlock(ctx, keyRangeID)
	return &protos.ModifyReply{}, err
}

func (c CoordinatorService) KeyRangeIDByBounds(ctx context.Context, keyRange *protos.KeyRange) (string, error) {
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

	return "", fmt.Errorf("key range not found")
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

func (c CoordinatorService) ListKeyRange(ctx context.Context, _ *protos.ListKeyRangeRequest) (*protos.KeyRangeReply, error) {
	if c.impl == nil {
		return &protos.KeyRangeReply{}, nil
	}

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

func (c CoordinatorService) MoveKeyRange(ctx context.Context, request *protos.MoveKeyRangeRequest) (*protos.ModifyReply, error) {
	keyRangeID, err := c.KeyRangeIDByBounds(ctx, request.GetKeyRange())
	if err != nil {
		return nil, err
	}

	updKeyRange := &kr.KeyRange{
		LowerBound: []byte(request.GetKeyRange().GetLowerBound()),
		UpperBound: []byte(request.GetKeyRange().GetUpperBound()),
		ShardID:    request.GetToShardId(),
		ID:         keyRangeID,
	}

	if err := c.impl.MoveKeyRange(ctx, updKeyRange); err != nil {
		return nil, err
	}

	return &protos.ModifyReply{}, nil
}

func (c CoordinatorService) MergeKeyRange(ctx context.Context, request *protos.MergeKeyRangeRequest) (*protos.ModifyReply, error) {
	krsqb, err := c.impl.ListKeyRange(ctx)
	if err != nil {
		return nil, err
	}

	bound := request.GetBound()
	uniteKeyRange := &kr.UniteKeyRange{}

	for _, krqb := range krsqb {
		if bytes.Equal(krqb.LowerBound, bound) {
			uniteKeyRange.KeyRangeIDRight = krqb.ID

			if uniteKeyRange.KeyRangeIDLeft != "" {
				break
			}
			continue
		}

		if bytes.Equal(krqb.UpperBound, bound) {
			uniteKeyRange.KeyRangeIDLeft = krqb.ID

			if uniteKeyRange.KeyRangeIDRight != "" {
				break
			}
			continue
		}
	}

	spqrlog.Logger.Printf(spqrlog.DEBUG3, "unite keyrange %#v", uniteKeyRange)

	if uniteKeyRange.KeyRangeIDLeft == "" || uniteKeyRange.KeyRangeIDRight == "" {
		spqrlog.Logger.Printf(spqrlog.DEBUG3, "no found key ranges to merge by border %v", bound)
		return &protos.ModifyReply{}, nil
	}

	if err := c.impl.Unite(ctx, uniteKeyRange); err != nil {
		return nil, fmt.Errorf("failed to unite key ranges: %w", err)
	}

	return &protos.ModifyReply{}, nil
}

var _ protos.KeyRangeServiceServer = CoordinatorService{}

func NewKeyRangeService(impl coordinator.Coordinator) protos.KeyRangeServiceServer {
	return &CoordinatorService{
		impl: impl,
	}
}
