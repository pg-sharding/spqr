package shhttp

import (
	"context"
	shards "github.com/shgo/genproto/protos"
	"google.golang.org/grpc"
)

type Shgoserver struct {
	shards.UnimplementedShardSerivceServer
}

var _ shards.ShardSerivceServer = &Shgoserver{}
//
//func NewShgoServer() *shgoserver {
//	//server := grpc.NewServer()
//	//shardsServ := shards.NewShardSerivceClient(server)
//}

func (* Shgoserver) ListShards(context.Context, *shards.ShardRequest) (*shards.ShardReply, error) {
	return &shards.ShardReply{
		Shards: []string{
			"fd",
		},
	}, nil
}

func Register(server * grpc.Server) {
	shards.RegisterShardSerivceServer(server, &Shgoserver{})
}