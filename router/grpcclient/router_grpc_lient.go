package grpcclient

import (
	"context"

	"github.com/pg-sharding/spqr/router/pkg/client"
	"github.com/pg-sharding/spqr/router/pkg/console"
	proto "github.com/pg-sharding/spqr/router/protos"
	"google.golang.org/grpc"
)

type RouterQClient struct {
	proto.UnimplementedQueryServiceServer

	Console console.Console
}

func Dial(addr string) (*grpc.ClientConn, error) {
	return grpc.Dial(addr, grpc.WithInsecure())
}

func (s RouterQClient) Process(ctx context.Context, request *proto.QueryExecuteRequest) (*proto.QueryExecuteResponse, error) {
	_ = s.Console.ProcessQuery(ctx, request.Query, client.NewFakeClient())

	return &proto.QueryExecuteResponse{}, nil
}

func NewSpqrConn(c console.Console) *RouterQClient {
	return &RouterQClient{
		Console: c,
	}
}

var _ proto.QueryServiceServer = RouterQClient{}
