package meta_transaction

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	proto "github.com/pg-sharding/spqr/pkg/protos"
	"github.com/pg-sharding/spqr/qdb"
)

type TransactionMgr interface {
	// Execute chunk of commands as atomic operation
	// TODO: remove after transaction system will be implemented at all
	ExecNoTran(ctx context.Context, chunk *MetaTransactionChunk) error
	// Execute chunk of commands as atomic operation in transaction
	CommitTran(ctx context.Context, transaction *MetaTransaction) error
	// Begins transaction in qdb
	BeginTran(ctx context.Context) (*MetaTransaction, error)
}

type MetaTransaction struct {
	TransactionId uuid.UUID
	Operations    *MetaTransactionChunk
}

func NewMetaTransaction(qdbTransaction qdb.QdbTransaction) *MetaTransaction {
	return &MetaTransaction{TransactionId: qdbTransaction.Id()}
}

func innerFromProto(TransactionId string, CmdList []*proto.QdbTransactionCmd, MetaCmdList []*proto.MetaTransactionGossipCommand) (*MetaTransaction, error) {
	if tranId, err := uuid.Parse(TransactionId); err != nil {
		return nil, err
	} else {
		qdbCmdList := make([]qdb.QdbStatement, len(CmdList))
		for index, stmtProto := range CmdList {
			if stmtQdb, err := qdb.QdbStmtFromProto(stmtProto); err != nil {
				return nil, err
			} else {
				qdbCmdList[index] = *stmtQdb
			}
		}
		chunk, err := NewMetaTransactionChunk(MetaCmdList, qdbCmdList)
		if err != nil {
			return nil, err
		}
		return &MetaTransaction{TransactionId: tranId,
			Operations: chunk,
		}, nil
	}
}

func BeginTranFromProto(tran *proto.MetaTransactionReply) (*MetaTransaction, error) {
	if len(tran.CmdList) > 0 || len(tran.MetaCmdList) > 0 {
		return nil, fmt.Errorf("Begin from proto non empty transaction")
	}
	if idTran, err := uuid.Parse(tran.TransactionId); err != nil {
		return nil, fmt.Errorf("invalid transaction id=%s", tran.TransactionId)
	} else {
		return &MetaTransaction{TransactionId: idTran}, nil
	}
}

func TransactionFromProto(tran *proto.MetaTransactionReply) (*MetaTransaction, error) {
	return innerFromProto(tran.TransactionId, tran.CmdList, tran.MetaCmdList)
}

func TransactionFromProtoRequest(tran *proto.MetaTransactionRequest) (*MetaTransaction, error) {
	return innerFromProto(tran.TransactionId, tran.CmdList, tran.MetaCmdList)
}

func ToQdbTransaction(tran *MetaTransaction) *qdb.QdbTransaction {
	return qdb.NewTransactionWithCmd(tran.TransactionId, tran.Operations.QdbStatements)
}

type MetaTransactionChunk struct {
	// There are commands for router to change meta in router in atomic operation.
	// Coordinator generates its.
	GossipRequests []*proto.MetaTransactionGossipCommand
	// There are qdb commands which need execute in atomic qdb operation.
	// Qdb instance on active instance coordinator generates it
	QdbStatements []qdb.QdbStatement
}

// Any change in this enum must change GetGossipRequestType function
const (
	GR_UNKNOWN = iota
	GR_CreateDistributionRequest
	GR_CreateKeyRange
)

func NewMetaTransactionChunk(gossipRequests []*proto.MetaTransactionGossipCommand,
	qdbStatements []qdb.QdbStatement) (*MetaTransactionChunk, error) {
	if len(qdbStatements) == 0 {
		return nil, fmt.Errorf("transaction chunk must have a qdb statement (create chunk)")
	} else {
		return &MetaTransactionChunk{
			GossipRequests: gossipRequests,
			QdbStatements:  qdbStatements,
		}, nil
	}
}

func (tc *MetaTransactionChunk) Append(gossipRequests []*proto.MetaTransactionGossipCommand,
	qdbStatements []qdb.QdbStatement) error {
	if len(qdbStatements) == 0 {
		return fmt.Errorf("transaction chunk must have a qdb statement (case 1)")
	} else {
		for _, req := range gossipRequests {
			if gossipType := GetGossipRequestType(req); gossipType == GR_UNKNOWN {
				return fmt.Errorf("invalid meta gossip command request")
			}
		}
		tc.GossipRequests = append(tc.GossipRequests, gossipRequests...)
		tc.QdbStatements = append(tc.QdbStatements, qdbStatements...)
	}
	return nil
}

func NewTransaction() (*MetaTransaction, error) {
	if qdbTran, err := qdb.NewTransaction(); err != nil {
		return nil, err
	} else {
		return &MetaTransaction{
			TransactionId: qdbTran.Id(),
			Operations:    &MetaTransactionChunk{},
		}, nil
	}
}

func ToNoGossipTransaction(transaction *MetaTransaction) *MetaTransaction {
	return &MetaTransaction{
		TransactionId: transaction.TransactionId,
		Operations: &MetaTransactionChunk{
			GossipRequests: make([]*proto.MetaTransactionGossipCommand, 0),
			QdbStatements:  transaction.Operations.QdbStatements,
		},
	}
}

// Checks algebraic type MetaTransactionGossipCommand and returns the command type
// or GR_UNKNOWN if check failed
//
// Parameters:
// - (request *proto.MetaTransactionGossipCommand): generic command
//
// Returns:
// - type of command
func GetGossipRequestType(request *proto.MetaTransactionGossipCommand) int {
	result := GR_UNKNOWN
	if request.CreateDistribution != nil {
		result = GR_CreateDistributionRequest
	}
	if request.CreateKeyRange != nil {
		if result != GR_UNKNOWN {
			return GR_UNKNOWN
		} else {
			result = GR_CreateKeyRange
		}
	}
	return result
}
