package provider

import (
	"context"

	"github.com/pg-sharding/spqr/coordinator"
	mtran "github.com/pg-sharding/spqr/pkg/models/transaction"
	proto "github.com/pg-sharding/spqr/pkg/protos"
	qdb "github.com/pg-sharding/spqr/qdb"
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

func toQdbStatementList(cmdList []*proto.QdbTransactionCmd) ([]qdb.QdbStatement, error) {
	stmts := make([]qdb.QdbStatement, 0, len(cmdList))
	for _, cmd := range cmdList {
		if qdbCmd, err := qdb.QdbStmtFromProto(cmd); err != nil {
			return nil, err
		} else {
			stmts = append(stmts, *qdbCmd)
		}
	}
	return stmts, nil
}

func (mts *MetaTransactionServer) ExecNoTran(ctx context.Context, request *proto.ExecNoTranRequest) (*emptypb.Empty, error) {
	stmts, err := toQdbStatementList(request.CmdList)
	if err != nil {
		return nil, err
	}
	tranChunk, err := mtran.NewMetaTransactionChunk(request.MetaCmdList, stmts)
	if err != nil {
		return nil, err
	}
	return nil, mts.impl.ExecNoTran(ctx, tranChunk)
}

func (mts *MetaTransactionServer) ExecTran(ctx context.Context, request *proto.MetaTransactionRequest) (*emptypb.Empty, error) {
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
