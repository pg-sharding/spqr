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
	httpAddr := config.RouterConfig().WorldShardAddress
	listener, err := net.Listen("tcp", httpAddr)
	if err != nil {
		return err
	}
	tracelog.InfoLogger.Printf("world listening on %s", httpAddr)

	return serv.Serve(listener)
}
