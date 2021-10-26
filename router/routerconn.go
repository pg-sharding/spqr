package router

import (
	context "context"

	"github.com/pg-sharding/spqr/router/pkg/console"
	"github.com/pg-sharding/spqr/router/pkg/rrouter"
	proto "github.com/pg-sharding/spqr/router/protos"
)

type RouterConn struct {
	proto.UnimplementedQueryServiceServer

	Console console.Console
}

func (s RouterConn) Process(ctx context.Context, request *proto.QueryExecuteRequest) (*proto.QueryExecuteResponse, error) {
	_ = s.Console.ProcessQuery(request.Query, rrouter.NewFakeClient())

	return &proto.QueryExecuteResponse{}, nil
}

func NewSpqrConn(c console.Console) *RouterConn {
	return &RouterConn{
		Console: c,
	}
}

var _ proto.QueryServiceServer = RouterConn{}
