package provider

import (
	"context"

	"github.com/pg-sharding/spqr/coordinator"
	mtran "github.com/pg-sharding/spqr/pkg/models/transaction"
	proto "github.com/pg-sharding/spqr/pkg/protos"
	"google.golang.org/protobuf/types/known/emptypb"
)

type MetaTransactionServer struct {
	proto.UnimplementedMetaTransactionServiceServer

	impl coordinator.Coordinator
}

func NewMetaTransactionServer(impl coordinator.Coordinator) *MetaTransactionServer {
	return &MetaTransactionServer{
		impl: impl,
	}
}

var _ proto.MetaTransactionServiceServer = &MetaTransactionServer{}

func (mts *MetaTransactionServer) ExecNoTran(ctx context.Context, request *proto.ExecNoTranRequest) (*emptypb.Empty, error) {
	return nil, mts.impl.ExecNoTran(ctx, mtran.NewMetaTransactionChunk(request.MetaCmdList))
}

func (mts *MetaTransactionServer) CommitTran(ctx context.Context, request *proto.MetaTransactionRequest) (*emptypb.Empty, error) {
	var metaTran *mtran.MetaTransaction
	metaTran, err := mtran.TransactionFromProtoRequest(request)
	if err != nil {
		return nil, err
	}
	return nil, mts.impl.CommitTran(ctx, metaTran)
}

func (mts *MetaTransactionServer) BeginTran(ctx context.Context, _ *emptypb.Empty) (*proto.MetaTransactionReply, error) {
	if tran, err := mts.impl.BeginTran(ctx); err != nil {
		return nil, err
	} else {
		return &proto.MetaTransactionReply{TransactionId: tran.TransactionId.String()}, nil
	}
}
