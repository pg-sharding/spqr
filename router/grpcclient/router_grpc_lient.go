package grpcclient

import (
	"google.golang.org/grpc"
)

type rgrpclient struct {
}

func Dial(addr string) (*grpc.ClientConn, error) {
	return grpc.Dial(addr, grpc.WithInsecure())
}
