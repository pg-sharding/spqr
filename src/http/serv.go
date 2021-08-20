package shhttp

import (
	"context"
	"fmt"

	shards "github.com/spqr/genproto/protos"
	"google.golang.org/grpc"
)

type Spqrserver struct {
	shards.UnimplementedShardServiceServer
}

var _ shards.ShardServiceServer = &Spqrserver{}

func (*Spqrserver) ListShards(context.Context, *shards.ShardRequest) (*shards.ShardReply, error) {
	fmt.Print("repl")
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

type HttpConf struct {
	Addr string `json:"http_addr" toml:"http_addr" yaml:"http_addr"`
}
