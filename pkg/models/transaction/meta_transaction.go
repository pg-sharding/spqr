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
	TransactionId uuid.UUID
	Operations    *MetaTransactionChunk
}

func NewMetaTransaction(qdbTransaction qdb.QdbTransaction) *MetaTransaction {
	return &MetaTransaction{TransactionId: qdbTransaction.Id()}
}

func innerFromProto(TransactionId string, MetaCmdList []*proto.MetaTransactionGossipCommand) (*MetaTransaction, error) {
	if tranId, err := uuid.Parse(TransactionId); err != nil {
		return nil, err
	} else {
		return &MetaTransaction{TransactionId: tranId,
			Operations: NewMetaTransactionChunk(MetaCmdList),
		}, nil
	}
}

func BeginTranFromProto(tran *proto.MetaTransactionReply) (*MetaTransaction, error) {
	idTran, err := uuid.Parse(tran.TransactionId)
	if err != nil {
		return nil, fmt.Errorf("invalid transaction id=%s", tran.TransactionId)
	}
	return &MetaTransaction{TransactionId: idTran}, nil
}

func TransactionFromProto(tran *proto.MetaTransactionReply) (*MetaTransaction, error) {
	return innerFromProto(tran.TransactionId, tran.MetaCmdList)
}

func TransactionFromProtoRequest(tran *proto.MetaTransactionRequest) (*MetaTransaction, error) {
	return innerFromProto(tran.TransactionId, tran.MetaCmdList)
}

type MetaTransactionChunk struct {
	// There are commands for router to change meta in router in atomic operation.
	// Coordinator generates its.
	GossipRequests []*proto.MetaTransactionGossipCommand
}

// Any change in this enum must change GetGossipRequestType function
const (
	GR_ERROR = iota - 1
	GR_UNKNOWN
	GR_CreateDistributionRequest
	GR_CreateKeyRange
	GR_DropKeyRange
	GR_DropKeyRangeAll
	GR_LockKeyRange
	GR_UnLockKeyRange
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
		if _, recognized := GetGossipRequestType(req); !recognized {
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
			TransactionId: qdbTran.Id(),
			Operations:    &MetaTransactionChunk{},
		}, nil
	}
}

func checkCommandPart(part googleProto.Message, current int, target int) int {
	if current == GR_ERROR {
		return current
	}
	if part == nil {
		return current
	}
	v := reflect.ValueOf(part)
	if v.Kind() == reflect.Ptr && !v.IsNil() {
		if current != GR_UNKNOWN {
			return GR_ERROR
		} else {
			return target
		}
	}
	return current
}

// Checks algebraic type MetaTransactionGossipCommand and returns the command type
// or GR_UNKNOWN, GR_ERROR if check failed
//
// Parameters:
// - (request *proto.MetaTransactionGossipCommand): generic command
//
// Returns:
// - type of command
// - type is recognized
func GetGossipRequestType(request *proto.MetaTransactionGossipCommand) (int, bool) {
	result := GR_UNKNOWN
	if request.CreateDistribution != nil {
		result = GR_CreateDistributionRequest
	}
	result = checkCommandPart(request.CreateKeyRange, result, GR_CreateKeyRange)
	result = checkCommandPart(request.DropKeyRange, result, GR_DropKeyRange)
	return result, result != GR_UNKNOWN && result != GR_ERROR
}
