package provider

import (
	"context"

	"github.com/pg-sharding/spqr/coordinator"
	proto "github.com/pg-sharding/spqr/pkg/protos"
	"google.golang.org/protobuf/types/known/emptypb"
)

type TwoPhaseTxMetaServer struct {
	proto.UnimplementedMetaTransactionServiceServer

	impl coordinator.Coordinator
}

func NewTwoPhaseTxMetaServer(impl coordinator.Coordinator) *TwoPhaseTxMetaServer {
	return &TwoPhaseTxMetaServer{
		impl: impl,
	}
}

var _ proto.MetaTransactionServiceServer = &TwoPhaseTxMetaServer{}

// GetTwoPhaseTxMetaStorage implements [proto.TwoPhaseTxMetaServiceServer].
func (l *TwoPhaseTxMetaServer) GetTwoPhaseTxMetaStorage(ctx context.Context, _ *emptypb.Empty) (*proto.TwoPhaseTxMetaStorageReply, error) {
	storage, err := l.impl.GetTwoPhaseTxMetaStorage(ctx)
	if err != nil {
		return nil, err
	}
	return &proto.TwoPhaseTxMetaStorageReply{Storage: storage}, nil
}

// SetTwoPhaseTxMetaStorage implements [proto.TwoPhaseTxMetaServiceServer].
func (l *TwoPhaseTxMetaServer) SetTwoPhaseTxMetaStorage(ctx context.Context, req *proto.SetTwoPhaseTxMetaStorageRequest) (*emptypb.Empty, error) {
	return nil, l.impl.SetTwoPhaseTxMetaStorage(ctx, req.Storage)
}
