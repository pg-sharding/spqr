package coordinator

import (
	"context"

	shards "github.com/pg-sharding/spqr/router/protos"
)

type Coordinator struct {
	shards.UnimplementedKeyRangeServiceServer
}

func (c Coordinator) ListKeyRange(ctx context.Context, request *shards.ListKeyRangeRequest) (*shards.KeyRangeReply, error) {
	panic("implement me")
}

func (c Coordinator) LockKeyRange(ctx context.Context, request *shards.LockKeyRangeRequest) (*shards.KeyRangeReply, error) {
	panic("implement me")
}

func (c Coordinator) UnlockKeyRange(ctx context.Context, request *shards.UnlockKeyRangeRequest) (*shards.KeyRangeReply, error) {
	panic("implement me")
}

func (c Coordinator) SplitKeyRange(ctx context.Context, request *shards.SplitKeyRangeRequest) (*shards.KeyRangeReply, error) {
	panic("implement me")
}

var _ shards.KeyRangeServiceServer = Coordinator{}
