package shhttp

import (
	shards "github.com/pg-sharding/spqr/pkg/protos"
	"google.golang.org/grpc"
)

func Register(server *grpc.Server) {
	//shards.RegisterQueryServiceServer(server, &{})

	shards.RegisterShardServiceServer(server, &ShardServer{})
}
