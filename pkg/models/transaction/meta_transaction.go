package meta_transaction

import (
	"context"
	"fmt"
	"reflect"

	"github.com/google/uuid"
	proto "github.com/pg-sharding/spqr/pkg/protos"
	"github.com/pg-sharding/spqr/qdb"
	googleProto "google.golang.org/protobuf/proto"
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
	TransactionID uuid.UUID
	Operations    *MetaTransactionChunk
}

func NewMetaTransaction(qdbTransaction qdb.QdbTransaction) *MetaTransaction {
	return &MetaTransaction{TransactionID: qdbTransaction.ID()}
}

func innerFromProto(TransactionID string, MetaCmdList []*proto.MetaTransactionGossipCommand) (*MetaTransaction, error) {
	if tranId, err := uuid.Parse(TransactionID); err != nil {
		return nil, err
	} else {
		return &MetaTransaction{TransactionID: tranId,
			Operations: NewMetaTransactionChunk(MetaCmdList),
		}, nil
	}
}

func BeginTranFromProto(tran *proto.MetaTransactionReply) (*MetaTransaction, error) {
	idTran, err := uuid.Parse(tran.TransactionID)
	if err != nil {
		return nil, fmt.Errorf("invalid transaction id=%s", tran.TransactionID)
	}
	return &MetaTransaction{TransactionID: idTran}, nil
}

func TransactionFromProto(tran *proto.MetaTransactionReply) (*MetaTransaction, error) {
	return innerFromProto(tran.TransactionID, tran.MetaCmdList)
}

func TransactionFromProtoRequest(tran *proto.MetaTransactionRequest) (*MetaTransaction, error) {
	return innerFromProto(tran.TransactionID, tran.MetaCmdList)
}

type MetaTransactionChunk struct {
	// There are commands for router to change meta in router in atomic operation.
	// Coordinator generates its.
	GossipRequests []*proto.MetaTransactionGossipCommand
}

// Any change in this enum must change GetGossipRequestType function
const (
	GrError = iota - 1
	GrUnknown
	GrCreateDistributionRequest
	GrCreateKeyRange
	GrDropKeyRange
	GrUpdateKeyRange
	GrCreateSequence
)

func NewMetaTransactionChunk(gossipRequests []*proto.MetaTransactionGossipCommand) *MetaTransactionChunk {
	return &MetaTransactionChunk{
		GossipRequests: gossipRequests,
	}
}

func NewEmptyMetaTransactionChunk() *MetaTransactionChunk {
	return NewMetaTransactionChunk([]*proto.MetaTransactionGossipCommand{})
}

func (tc *MetaTransactionChunk) Append(gossipRequests []*proto.MetaTransactionGossipCommand) error {
	for _, req := range gossipRequests {
		if _, ok := GetGossipRequestType(req); !ok {
			return fmt.Errorf("invalid meta gossip command request")
		}
	}
	tc.GossipRequests = append(tc.GossipRequests, gossipRequests...)
	return nil
}

func NewTransaction() (*MetaTransaction, error) {
	if qdbTran, err := qdb.NewTransaction(); err != nil {
		return nil, err
	} else {
		return &MetaTransaction{
			TransactionID: qdbTran.ID(),
			Operations:    &MetaTransactionChunk{},
		}, nil
	}
}

func checkCommandPart(part googleProto.Message, current int, target int) int {
	if current == GrError {
		return current
	}
	if part == nil {
		return current
	}
	v := reflect.ValueOf(part)
	if v.Kind() == reflect.Ptr && !v.IsNil() {
		if current != GrUnknown {
			return GrError
		} else {
			return target
		}
	}
	return current
}

// Checks algebraic type MetaTransactionGossipCommand and returns the command type
// or GrUnknown, GrError if check failed
//
// Parameters:
// - (request *proto.MetaTransactionGossipCommand): generic command
//
// Returns:
// - type of command
// - type is recognized
func GetGossipRequestType(request *proto.MetaTransactionGossipCommand) (int, bool) {
	result := GrUnknown
	if request.CreateDistribution != nil {
		result = GrCreateDistributionRequest
	}
	result = checkCommandPart(request.CreateKeyRange, result, GrCreateKeyRange)
	result = checkCommandPart(request.DropKeyRange, result, GrDropKeyRange)
	result = checkCommandPart(request.UpdateKeyRange, result, GrUpdateKeyRange)
	result = checkCommandPart(request.CreateSequence, result, GrCreateSequence)
	return result, result != GrUnknown && result != GrError
}
