package world

import (
	"net"

	shhttp "github.com/pg-sharding/spqr/grpc"
	"github.com/pg-sharding/spqr/pkg/config"
	shards "github.com/pg-sharding/spqr/router/protos"
	"github.com/wal-g/tracelog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

type World struct {
	shards.UnimplementedKeyRangeServiceServer
}

var (
	_ shards.KeyRangeServiceServer = World{}
)

func NewWorld() *World {
	return &World{}
}

func (w *World) Run() error {
	serv := grpc.NewServer()
	shhttp.Register(serv)
	reflection.Register(serv)
	worldShard := getWorldShard(config.RouterConfig().ShardMapping)
	if worldShard == nil {
		return nil
	}
	address := worldShard.Hosts[0]
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return err
	}
	tracelog.InfoLogger.Printf("world listening on %s", address)

	return serv.Serve(listener)
}

func getWorldShard(shardMapping map[string]*config.Shard) *config.Shard {
	for _, shard := range shardMapping {
		if shard.Type == config.WorldShard {
			return shard
		}
	}
	return nil
}
