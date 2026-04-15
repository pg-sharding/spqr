package meta_transaction

import (
	"testing"

	"github.com/google/uuid"
	proto "github.com/pg-sharding/spqr/pkg/protos"
	"github.com/stretchr/testify/assert"
)

func TestTransactionFromProto(t *testing.T) {
	is := assert.New(t)
	t.Run("happy path", func(_ *testing.T) {
		distribution := &proto.Distribution{
			Id:          "ds1",
			ColumnTypes: []string{"integer"},
		}
		protoTran := &proto.MetaTransactionReply{
			TransactionID: "6ca41a0b-8446-4098-abcf-d9802bea3447",
			MetaCmdList: []*proto.MetaTransactionGossipCommand{
				{
					CreateDistribution: &proto.CreateDistributionGossip{
						Distributions: []*proto.Distribution{distribution},
					},
				},
			},
		}
		transactionID, err := uuid.Parse("6ca41a0b-8446-4098-abcf-d9802bea3447")
		is.NoError(err)
		chunk := NewMetaTransactionChunk([]*proto.MetaTransactionGossipCommand{
			{
				CreateDistribution: &proto.CreateDistributionGossip{
					Distributions: []*proto.Distribution{distribution},
				},
			},
		},
		)
		is.NoError(err)
		expected := MetaTransaction{
			TransactionID: transactionID,
			Operations:    chunk,
		}

		actual, err := TransactionFromProto(protoTran)
		is.NoError(err)
		is.Equal(expected, *actual)
	})

}

func TestCheckCommandPart(t *testing.T) {
	t.Run("pass through ERROR", func(t *testing.T) {
		is := assert.New(t)
		actual := checkCommandPart(nil, GrError, GrCreateKeyRange)
		is.Equal(actual, GrError)
		data := &proto.CreateDistributionGossip{}
		actual = checkCommandPart(data, GrError, GrUnknown)
		is.Equal(actual, GrError)
		var data1 *proto.CreateDistributionGossip
		actual = checkCommandPart(data1, GrError, GrCreateKeyRange)
		is.Equal(actual, GrError)
	})
	t.Run("nil passed no changes", func(t *testing.T) {
		is := assert.New(t)
		actual := checkCommandPart(nil, GrCreateKeyRange, GrCreateKeyRange)
		is.Equal(actual, GrCreateKeyRange)
		actual = checkCommandPart(nil, GrUnknown, GrUnknown)
		is.Equal(actual, GrUnknown)
	})
	t.Run("empty data passed no changes", func(t *testing.T) {
		is := assert.New(t)
		var data *proto.CreateDistributionGossip
		actual := checkCommandPart(data, GrCreateDistributionRequest, GrCreateDistributionRequest)
		is.Equal(actual, GrCreateDistributionRequest)
		actual = checkCommandPart(data, GrUnknown, GrUnknown)
		is.Equal(actual, GrUnknown)
	})
	t.Run("NO EMPTY data passed GrUnknown current", func(t *testing.T) {
		is := assert.New(t)
		data := &proto.CreateDistributionGossip{}
		actual := checkCommandPart(data, GrUnknown, GrCreateDistributionRequest)
		is.Equal(actual, GrCreateDistributionRequest)
	})
	t.Run("NO EMPTY data passed current IS NOT GrUnknown", func(t *testing.T) {
		is := assert.New(t)
		data := &proto.CreateDistributionGossip{}
		actual := checkCommandPart(data, GrDropKeyRange, GrCreateDistributionRequest)
		is.Equal(actual, GrError)
	})
}

func TestGetGossipRequestType(t *testing.T) {
	is := assert.New(t)
	t.Run("happy path proto.CreateDistributionGossip", func(_ *testing.T) {
		cmd := &proto.MetaTransactionGossipCommand{
			CreateDistribution: &proto.CreateDistributionGossip{},
		}
		actual, ok := GetGossipRequestType(cmd)
		is.True(ok)
		is.Equal(GrCreateDistributionRequest, actual)
	})
	t.Run("happy path proto.CreateDistributionGossip", func(_ *testing.T) {
		cmd := &proto.MetaTransactionGossipCommand{
			CreateKeyRange: &proto.CreateKeyRangeGossip{},
		}
		actual, ok := GetGossipRequestType(cmd)
		is.True(ok)
		is.Equal(GrCreateKeyRange, actual)
	})

	t.Run("failed algebraic type parsing", func(t *testing.T) {
		cmd := &proto.MetaTransactionGossipCommand{
			CreateKeyRange:     &proto.CreateKeyRangeGossip{},
			CreateDistribution: &proto.CreateDistributionGossip{},
		}
		actual, ok := GetGossipRequestType(cmd)
		is.False(ok)
		is.Equal(GrError, actual)
	})

	t.Run("happy path proto.DropKeyRangeGossip", func(t *testing.T) {
		cmd := &proto.MetaTransactionGossipCommand{
			DropKeyRange: &proto.DropKeyRangeGossip{},
		}
		actual, ok := GetGossipRequestType(cmd)
		is.True(ok)
		is.Equal(GrDropKeyRange, actual)
	})
	t.Run("failed algebraic type parsing, case 2", func(t *testing.T) {
		cmd := &proto.MetaTransactionGossipCommand{
			CreateKeyRange: &proto.CreateKeyRangeGossip{},
			DropKeyRange:   &proto.DropKeyRangeGossip{},
		}
		actual, ok := GetGossipRequestType(cmd)
		is.False(ok)
		is.Equal(GrError, actual)
	})
	t.Run("failed algebraic type parsing, case 2", func(t *testing.T) {
		cmd := &proto.MetaTransactionGossipCommand{}
		actual, ok := GetGossipRequestType(cmd)
		is.False(ok)
		is.Equal(GrUnknown, actual)
	})
	t.Run("failed algebraic type parsing, case: NOT NILL, NIL, NOT NILL", func(t *testing.T) {
		cmd := &proto.MetaTransactionGossipCommand{
			CreateDistribution: &proto.CreateDistributionGossip{},
			DropKeyRange:       &proto.DropKeyRangeGossip{},
		}
		actual, ok := GetGossipRequestType(cmd)
		is.False(ok)
		is.Equal(GrError, actual)
	})

	t.Run("happy path proto.CreateSequenceGossip", func(t *testing.T) {
		cmd := &proto.MetaTransactionGossipCommand{
			CreateSequence: &proto.CreateSequenceGossip{},
		}
		actual, ok := GetGossipRequestType(cmd)
		is.True(ok)
		is.Equal(GrCreateSequence, actual)
	})
	t.Run("fail proto.CreateSequenceGossip", func(t *testing.T) {
		cmd := &proto.MetaTransactionGossipCommand{
			CreateSequence: &proto.CreateSequenceGossip{},
			CreateKeyRange: &proto.CreateKeyRangeGossip{},
		}
		actual, ok := GetGossipRequestType(cmd)
		is.False(ok)
		is.Equal(GrError, actual)
	})
}
