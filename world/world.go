package world

import (
	context "context"
	"net"

	shhttp "github.com/pg-sharding/spqr/grpc"
	"github.com/pg-sharding/spqr/pkg/config"
	shards "github.com/pg-sharding/spqr/router/protos"
	"github.com/wal-g/tracelog"
	"golang.org/x/xerrors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

type World struct {
	shards.UnimplementedKeyRangeServiceServer
}

func (w World) LockKeyRange(ctx context.Context, request *shards.LockKeyRangeRequest) (*shards.LockKeyRangeReply, error) {
	panic("implement me")
}

func (w World) UnlockKeyRange(ctx context.Context, request *shards.UnlockKeyRangeRequest) (*shards.UnlockKeyRangeReply, error) {
	panic("implement me")
}

func (w World) SplitKeyRange(ctx context.Context, request *shards.SplitKeyRangeRequest) (*shards.SplitKeyRangeReply, error) {
	panic("implement me")
}

func (w World) AddShardingColumn(ctx context.Context, request *shards.AddShardingColumnRequest) (*shards.AddShardingColumnReply, error) {
	panic("implement me")
}

func (w World) AddLocalTable(ctx context.Context, request *shards.AddLocalTableRequest) (*shards.AddLocalTableReply, error) {
	panic("implement me")
}

func (w World) ListKeyRange(ctx context.Context, request *shards.ListKeyRangeRequest) (*shards.KeyRangeReply, error) {
	return nil, xerrors.New("not implemented for World")
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
	httpAddr := config.RouterConfig().WorldHttpAddr
	listener, err := net.Listen("tcp", httpAddr)
	if err != nil {
		return err
	}
	tracelog.InfoLogger.Printf("world listening on %s", httpAddr)

	return serv.Serve(listener)
}
