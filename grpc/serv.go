package shhttp

import (
	"context"

	shards "github.com/pg-sharding/spqr/router/protos"
)

type Spqrserver struct {
	shards.UnimplementedShardServiceServer
}

func (s *Spqrserver) AddShard(ctx context.Context, request *shards.AddShardRequest) (*shards.AddShardReply, error) {
	panic("implement me")
}

var _ shards.ShardServiceServer = &Spqrserver{}

func (*Spqrserver) ListShards(context.Context, *shards.ShardRequest) (*shards.ShardReply, error) {
	return &shards.ShardReply{
		Shards: nil,
	}, nil
}
