package grpcqrouter

import (
	"context"

	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/router/pkg/qrouter"
	protos "github.com/pg-sharding/spqr/router/protos"
	"google.golang.org/grpc/reflection"
)

type LocalQrouterServer struct {
	protos.UnimplementedKeyRangeServiceServer
	qr qrouter.Qrouter
}

func (l LocalQrouterServer) AddKeyRange(ctx context.Context, request *protos.AddKeyRangeRequest) (*protos.AddKeyRangeReply, error) {

	err := l.qr.AddKeyRange(kr.KeyRangeFromProto(request.KeyRange))
	if err != nil {
		return nil, err
	}

	return &protos.AddKeyRangeReply{}, nil
}

func (l LocalQrouterServer) ListKeyRange(ctx context.Context, request *protos.ListKeyRangeRequest) (*protos.KeyRangeReply, error) {
	krs := []*protos.KeyRange{}

	for _, kr := range l.qr.KeyRanges() {
		krs = append(krs, kr.ToProto())
	}

	resp := &protos.KeyRangeReply{
		KeyRanges: krs,
	}

	return resp, nil
}

func (l LocalQrouterServer) LockKeyRange(ctx context.Context, request *protos.LockKeyRangeRequest) (*protos.LockKeyRangeReply, error) {
	panic("implement me")
}

func (l LocalQrouterServer) UnlockKeyRange(ctx context.Context, request *protos.UnlockKeyRangeRequest) (*protos.UnlockKeyRangeReply, error) {
	panic("implement me")
}

func (l LocalQrouterServer) SplitKeyRange(ctx context.Context, request *protos.SplitKeyRangeRequest) (*protos.SplitKeyRangeReply, error) {
	panic("implement me")
}

func (l LocalQrouterServer) AddShardingColumn(ctx context.Context, request *protos.AddShardingColumnRequest) (*protos.AddShardingColumnReply, error) {
	panic("implement me")
}

func (l LocalQrouterServer) AddLocalTable(ctx context.Context, request *protos.AddLocalTableRequest) (*protos.AddLocalTableReply, error) {
	err := l.qr.AddLocalTable(request.Tname)

	if err != nil {
		return nil, err
	}

	return &protos.AddLocalTableReply{}, nil
}

func Register(server reflection.GRPCServer, qrouter qrouter.Qrouter) {

	reflection.Register(server)

	lqr := LocalQrouterServer{
		qr: qrouter,
	}

	protos.RegisterKeyRangeServiceServer(server, lqr)
}

var _ protos.KeyRangeServiceServer = LocalQrouterServer{}
