package keyrangeservice

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
	panic("implement me")
}

func (c KeyRangeService) LockKeyRange(ctx context.Context, request *shards.LockKeyRangeRequest) (*shards.KeyRangeReply, error) {
	err := c.impl.Lock(request.Krid)
	return nil, err
}

func (c KeyRangeService) UnlockKeyRange(ctx context.Context, request *shards.UnlockKeyRangeRequest) (*shards.KeyRangeReply, error) {
	panic("implement me")
}

func (c KeyRangeService) SplitKeyRange(ctx context.Context, request *shards.SplitKeyRangeRequest) (*shards.KeyRangeReply, error) {
	panic("implement me")
}

var _ shards.KeyRangeServiceServer = KeyRangeService{}
