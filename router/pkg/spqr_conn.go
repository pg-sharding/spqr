package pkg

import (
	context "context"

	"github.com/pg-sharding/spqr/router/pkg/console"
	"github.com/pg-sharding/spqr/router/pkg/rrouter"
	proto "github.com/pg-sharding/spqr/router/protos"
	"google.golang.org/grpc"
)

type RouterConn struct {
	ConsoleDB console.Console
}

func newSpqrConn() {

}

func (s RouterConn) Process(ctx context.Context, in *proto.QueryExecuteRequest, opts ...grpc.CallOption) (*proto.QueryExecuteResponse, error) {
	s.ConsoleDB.ProcessQuery(in.Query, rrouter.NewFakeClient())

	return &proto.QueryExecuteResponse{}, nil
}

var _ proto.RouterClient = RouterConn{}
