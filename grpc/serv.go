package shhttp

import (
	"context"

	shards "github.com/pg-sharding/spqr/router/protos"
	"google.golang.org/grpc"
)

type Spqrserver struct {
	shards.UnimplementedShardServiceServer
}

var _ shards.ShardServiceServer = &Spqrserver{}

func (*Spqrserver) ListShards(context.Context, *shards.ShardRequest) (*shards.ShardReply, error) {
	return &shards.ShardReply{
		Shards: nil,
	}, nil
}

func Register(server *grpc.Server) {
	shards.RegisterShardServiceServer(server, &Spqrserver{})
}
