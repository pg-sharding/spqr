package world

import (
	context "context"
	shhttp "github.com/pg-sharding/spqr/grpc"
	"github.com/pg-sharding/spqr/pkg/config"
	shards "github.com/pg-sharding/spqr/router/protos"
	"github.com/wal-g/tracelog"
	"golang.org/x/xerrors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
	"net"
)

type World struct {
	shards.UnimplementedKeyRangeServiceServer
}

func (w World) ListKeyRange(ctx context.Context, request *shards.ListKeyRangeRequest) (*shards.KeyRangeReply, error) {
	return nil, xerrors.New("not implemented for World")
}

func (w World) LockKeyRange(ctx context.Context, request *shards.LockKeyRangeRequest) (*shards.KeyRangeReply, error) {
	panic("implement me")
}

func (w World) UnlockKeyRange(ctx context.Context, request *shards.UnlockKeyRangeRequest) (*shards.KeyRangeReply, error) {
	panic("implement me")
}

func (w World) SplitKeyRange(ctx context.Context, request *shards.SplitKeyRangeRequest) (*shards.KeyRangeReply, error) {
	panic("implement me")
}

var _ shards.KeyRangeServiceServer = World{}

func NewWorld() *World {
	return &World{

	}
}

func (w *World) Run() error {
	serv := grpc.NewServer()
	shhttp.Register(serv)
	reflection.Register(serv)
	httpAddr := config.Get().WorldHttpAddr
	listener, err := net.Listen("tcp", httpAddr)
	if err != nil {
		return err
	}
	tracelog.InfoLogger.Printf("world listening on %s", httpAddr)

	return serv.Serve(listener)
}