package router

import (
	context "context"

	proto "github.com/pg-sharding/spqr/router/protos"
	"github.com/pg-sharding/spqr/router/router"
	"github.com/pg-sharding/spqr/router/router/console"
	"github.com/pg-sharding/spqr/router/router/qrouter"
	"github.com/pg-sharding/spqr/router/router/rrouter"
	"google.golang.org/grpc"
)

type RouterConn struct {
	Console console.Console
}

func NewSpqrConn(c console.Console) * RouterConn {
	return &RouterConn{
		Console: c,
	}
}

func (s RouterConn) Process(ctx context.Context, in *proto.QueryExecuteRequest, opts ...grpc.CallOption) (*proto.QueryExecuteResponse, error) {
	_ = s.Console.ProcessQuery(in.Query, rrouter.NewFakeClient())

	return &proto.QueryExecuteResponse{}, nil
}

var _ proto.RouterClient = RouterConn{}

type KeyRangeService struct {
	proto.UnimplementedKeyRangeServiceServer

	impl  router.Router
	qimpl qrouter.Qrouter
}

func (k KeyRangeService) ListKeyRange(ctx context.Context, in *proto.ListKeyRangeRequest, opts ...grpc.CallOption) (*proto.KeyRangeReply, error) {
	var krs []*proto.KeyRange
	for _, el := range k.qimpl.KeyRanges() {
		krs = append(krs, el.ToProto())
	}
	return &proto.KeyRangeReply{
		KeyRanges: krs,
	}, nil
}

func (k KeyRangeService) LockKeyRange(ctx context.Context, in *proto.LockKeyRangeRequest, opts ...grpc.CallOption) (*proto.KeyRangeReply, error) {
	_ = k.qimpl.Lock(in.Krid)

	return nil, nil
}

func (k KeyRangeService) UnlockKeyRange(ctx context.Context, in *proto.UnlockKeyRangeRequest, opts ...grpc.CallOption) (*proto.KeyRangeReply, error) {
	panic("implement me")
}

func (k KeyRangeService) SplitKeyRange(ctx context.Context, in *proto.SplitKeyRangeRequest, opts ...grpc.CallOption) (*proto.KeyRangeReply, error) {
	panic("implement me")
}

var _ proto.KeyRangeServiceClient = KeyRangeService{}
