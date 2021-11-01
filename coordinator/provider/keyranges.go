package provider

import (
	"context"
	"github.com/pg-sharding/spqr/pkg/models/shrule"

	"github.com/pg-sharding/spqr/coordinator"
	protos "github.com/pg-sharding/spqr/router/protos"
)

type KeyRangeService struct {
	protos.UnimplementedKeyRangeServiceServer

	impl coordinator.Coordinator
}

func (c KeyRangeService) LockKeyRange(ctx context.Context, request *protos.LockKeyRangeRequest) (*protos.LockKeyRangeReply, error) {
	_, err := c.impl.Lock(request.Krid)
	return nil, err
}

func (c KeyRangeService) UnlockKeyRange(ctx context.Context, request *protos.UnlockKeyRangeRequest) (*protos.UnlockKeyRangeReply, error) {
	err := c.impl.UnLock(request.Krid)
	return nil, err
}
func (c KeyRangeService) SplitKeyRange(ctx context.Context, request *protos.SplitKeyRangeRequest) (*protos.SplitKeyRangeReply, error) {
	panic("implement me")
}

func (c KeyRangeService) AddShardingColumn(ctx context.Context, request *protos.AddShardingColumnRequest) (*protos.AddShardingColumnReply, error) {
	err := c.impl.AddShardingRule(shrule.NewShardingRule(request.Colname))

	if err != nil {
		return nil, err
	}

	return &protos.AddShardingColumnReply{}, nil
}

func (c KeyRangeService) AddLocalTable(ctx context.Context, request *protos.AddLocalTableRequest) (*protos.AddLocalTableReply, error) {
	panic("implement me")
}

func (c KeyRangeService) ListKeyRange(ctx context.Context, request *protos.ListKeyRangeRequest) (*protos.KeyRangeReply, error) {

	krs := []*protos.KeyRange{
		{
			Krid:    "1",
			ShardId: "2",
		},
	}

	return &protos.KeyRangeReply{
		KeyRanges: krs,
	}, nil
}

var _ protos.KeyRangeServiceServer = KeyRangeService{}

func NewKeyRangeService(impl coordinator.Coordinator) protos.KeyRangeServiceServer {
	return &KeyRangeService{
		impl: impl,
	}
}
