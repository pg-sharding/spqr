package provider

import (
	"context"

	"github.com/pg-sharding/spqr/coordinator"
	shards "github.com/pg-sharding/spqr/router/protos"
)

type KeyRangeService struct {
	shards.UnimplementedKeyRangeServiceServer

	impl coordinator.Coordinator
}

func (c KeyRangeService) ListKeyRange(ctx context.Context, request *shards.ListKeyRangeRequest) (*shards.KeyRangeReply, error) {

	krs := []*shards.KeyRange{
		{
			Krid:    "1",
			ShardId: "2",
		},
	}

	return &shards.KeyRangeReply{
		KeyRanges: krs,
	}, nil
}

func (c KeyRangeService) LockKeyRange(ctx context.Context, request *shards.LockKeyRangeRequest) (*shards.KeyRangeReply, error) {
	err := c.impl.Lock(request.Krid)
	return nil, err
}

func (c KeyRangeService) UnlockKeyRange(ctx context.Context, request *shards.UnlockKeyRangeRequest) (*shards.KeyRangeReply, error) {
	err := c.impl.UnLock(request.Krid)
	return nil, err
}

func (c KeyRangeService) SplitKeyRange(ctx context.Context, request *shards.SplitKeyRangeRequest) (*shards.KeyRangeReply, error) {
	panic("implement me")
}

var _ shards.KeyRangeServiceServer = KeyRangeService{}

func NewKeyRangeService(impl coordinator.Coordinator) shards.KeyRangeServiceServer {
	return &KeyRangeService{
		impl: impl,
	}
}
