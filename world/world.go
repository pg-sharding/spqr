package world

import (
	"net"

	shhttp "github.com/pg-sharding/spqr/grpc"
	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/spqrlog"
	shards "github.com/pg-sharding/spqr/pkg/protos"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

type World struct {
	shards.UnimplementedKeyRangeServiceServer
	rcfg *config.Router
}

var (
	_ shards.KeyRangeServiceServer = World{}
)

func NewWorld(rcfg *config.Router) *World {
	return &World{
		rcfg: rcfg,
	}
}

func (w *World) Run() error {
	serv := grpc.NewServer()
	shhttp.Register(serv)
	reflection.Register(serv)

	worldShard := getWorldShard(w.rcfg.ShardMapping)
	if worldShard == nil {
		return nil
	}
	address := worldShard.Hosts[0]
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return err
	}

	spqrlog.Logger.Printf(spqrlog.INFO, "world listening on %s", address)

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
