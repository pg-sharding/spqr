package shhttp

import (
	"context"
	"fmt"

	shards "github.com/shgo/genproto/protos"
	"google.golang.org/grpc"
)

type Shgoserver struct {
	shards.UnimplementedShardServiceServer
}

var _ shards.ShardServiceServer = &Shgoserver{}

func (*Shgoserver) ListShards(context.Context, *shards.ShardRequest) (*shards.ShardReply, error) {
	fmt.Print("repl")
	return &shards.ShardReply{
		Shards: []string{
			"loh1",
			"loh2",
		},
	}, nil
}

func Register(server *grpc.Server) {
	shards.RegisterShardServiceServer(server, &Shgoserver{})
}
