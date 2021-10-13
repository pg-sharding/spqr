package coordinator

import (
	"context"

	shards "github.com/pg-sharding/spqr/router/protos"
)

type CoordinatorKeyRangeService struct {
	shards.UnimplementedKeyRangeServiceServer
}

func (c CoordinatorKeyRangeService) ListKeyRange(ctx context.Context, request *shards.ListKeyRangeRequest) (*shards.KeyRangeReply, error) {
	panic("implement me")
}

func (c CoordinatorKeyRangeService) LockKeyRange(ctx context.Context, request *shards.LockKeyRangeRequest) (*shards.KeyRangeReply, error) {
	panic("implement me")
}

func (c CoordinatorKeyRangeService) UnlockKeyRange(ctx context.Context, request *shards.UnlockKeyRangeRequest) (*shards.KeyRangeReply, error) {
	panic("implement me")
}

func (c CoordinatorKeyRangeService) SplitKeyRange(ctx context.Context, request *shards.SplitKeyRangeRequest) (*shards.KeyRangeReply, error) {
	panic("implement me")
}

var _ shards.KeyRangeServiceServer = CoordinatorKeyRangeService{}
