package shhttp

import (
	"context"

	shards "github.com/pg-sharding/spqr/protos"
	"google.golang.org/grpc"
)

type Spqrserver struct {
	shards.UnimplementedShardServiceServer
}

var _ shards.ShardServiceServer = &Spqrserver{}

func (*Spqrserver) ListShards(context.Context, *shards.ShardRequest) (*shards.ShardReply, error) {
	return &shards.ShardReply{
		Shards: []string{
			"loh1",
			"loh2",
		},
	}, nil
}

func Register(server *grpc.Server) {
	shards.RegisterShardServiceServer(server, &Spqrserver{})
}


