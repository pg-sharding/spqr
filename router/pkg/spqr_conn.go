package pkg

import (
	context "context"

	proto "github.com/pg-sharding/spqr/router/protos"
	"google.golang.org/grpc"
)

type spqrConn struct {
}

func newSpqrConn() {

}

func (s spqrConn) Process(ctx context.Context, in *proto.QueryExecuteRequest, opts ...grpc.CallOption) (*proto.QueryExecuteResponse, error) {
	panic("implement me")
}

var _ proto.RouterClient = spqrConn{}
