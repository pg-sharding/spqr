package shhttp

import (
	"context"

	shards "github.com/pg-sharding/spqr/router/protos"
)

type ShardServer struct {
	shards.UnimplementedShardServiceServer
}

func (s *ShardServer) AddShard(ctx context.Context, request *shards.AddShardRequest) (*shards.AddShardReply, error) {
	panic("implement me")
}

var _ shards.ShardServiceServer = &ShardServer{}

func (*ShardServer) ListShards(context.Context, *shards.ShardRequest) (*shards.ShardReply, error) {
	return &shards.ShardReply{
		Shards: nil,
	}, nil
}

func (*ShardServer) GetShardInfo(_ context.Context, _ *shards.ShardRequest) (*shards.ShardInfoReply, error) {
	return &shards.ShardInfoReply{
		ShardInfo: nil,
	}, nil
}
