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
			TransactionId: "6ca41a0b-8446-4098-abcf-d9802bea3447",
			MetaCmdList: []*proto.MetaTransactionGossipCommand{
				{
					CreateDistribution: &proto.CreateDistributionGossip{
						Distributions: []*proto.Distribution{distribution},
					},
				},
			},
		}
		transactionId, err := uuid.Parse("6ca41a0b-8446-4098-abcf-d9802bea3447")
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
			TransactionId: transactionId,
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
		actual := checkCommandPart(nil, GRError, GRCreateKeyRange)
		is.Equal(actual, GRError)
		data := &proto.CreateDistributionGossip{}
		actual = checkCommandPart(data, GRError, GRUnknown)
		is.Equal(actual, GRError)
		var data1 *proto.CreateDistributionGossip
		actual = checkCommandPart(data1, GRError, GRCreateKeyRange)
		is.Equal(actual, GRError)
	})
	t.Run("nil passed no changes", func(t *testing.T) {
		is := assert.New(t)
		actual := checkCommandPart(nil, GRCreateKeyRange, GRCreateKeyRange)
		is.Equal(actual, GRCreateKeyRange)
		actual = checkCommandPart(nil, GRUnknown, GRUnknown)
		is.Equal(actual, GRUnknown)
	})
	t.Run("empty data passed no changes", func(t *testing.T) {
		is := assert.New(t)
		var data *proto.CreateDistributionGossip
		actual := checkCommandPart(data, GRCreateDistributionRequest, GRCreateDistributionRequest)
		is.Equal(actual, GRCreateDistributionRequest)
		actual = checkCommandPart(data, GRUnknown, GRUnknown)
		is.Equal(actual, GRUnknown)
	})
	t.Run("NO EMPTY data passed GRUnknown current", func(t *testing.T) {
		is := assert.New(t)
		data := &proto.CreateDistributionGossip{}
		actual := checkCommandPart(data, GRUnknown, GRCreateDistributionRequest)
		is.Equal(actual, GRCreateDistributionRequest)
	})
	t.Run("NO EMPTY data passed current IS NOT GRUnknown", func(t *testing.T) {
		is := assert.New(t)
		data := &proto.CreateDistributionGossip{}
		actual := checkCommandPart(data, GRDropKeyRange, GRCreateDistributionRequest)
		is.Equal(actual, GRError)
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
		is.Equal(GRCreateDistributionRequest, actual)
	})
	t.Run("happy path proto.CreateDistributionGossip", func(_ *testing.T) {
		cmd := &proto.MetaTransactionGossipCommand{
			CreateKeyRange: &proto.CreateKeyRangeGossip{},
		}
		actual, ok := GetGossipRequestType(cmd)
		is.True(ok)
		is.Equal(GRCreateKeyRange, actual)
	})

	t.Run("failed algebraic type parsing", func(_ *testing.T) {
		cmd := &proto.MetaTransactionGossipCommand{
			CreateKeyRange:     &proto.CreateKeyRangeGossip{},
			CreateDistribution: &proto.CreateDistributionGossip{},
		}
		actual, ok := GetGossipRequestType(cmd)
		is.False(ok)
		is.Equal(GRError, actual)
	})

	t.Run("happy path proto.DropKeyRangeGossip", func(_ *testing.T) {
		cmd := &proto.MetaTransactionGossipCommand{
			DropKeyRange: &proto.DropKeyRangeGossip{},
		}
		actual, ok := GetGossipRequestType(cmd)
		is.True(ok)
		is.Equal(GRDropKeyRange, actual)
	})
	t.Run("failed algebraic type parsing, case 2", func(_ *testing.T) {
		cmd := &proto.MetaTransactionGossipCommand{
			CreateKeyRange: &proto.CreateKeyRangeGossip{},
			DropKeyRange:   &proto.DropKeyRangeGossip{},
		}
		actual, ok := GetGossipRequestType(cmd)
		is.False(ok)
		is.Equal(GRError, actual)
	})
	t.Run("failed algebraic type parsing, case 2", func(_ *testing.T) {
		cmd := &proto.MetaTransactionGossipCommand{}
		actual, ok := GetGossipRequestType(cmd)
		is.False(ok)
		is.Equal(GRUnknown, actual)
	})
	t.Run("failed algebraic type parsing, case: NOT NILL, NIL, NOT NILL", func(_ *testing.T) {
		cmd := &proto.MetaTransactionGossipCommand{
			CreateDistribution: &proto.CreateDistributionGossip{},
			DropKeyRange:       &proto.DropKeyRangeGossip{},
		}
		actual, ok := GetGossipRequestType(cmd)
		is.False(ok)
		is.Equal(GRError, actual)
	})

	t.Run("happy path proto.CreateSequenceGossip", func(_ *testing.T) {
		cmd := &proto.MetaTransactionGossipCommand{
			CreateSequence: &proto.CreateSequenceGossip{},
		}
		actual, ok := GetGossipRequestType(cmd)
		is.True(ok)
		is.Equal(GRCreateSequence, actual)
	})
	t.Run("fail proto.CreateSequenceGossip", func(_ *testing.T) {
		cmd := &proto.MetaTransactionGossipCommand{
			CreateSequence: &proto.CreateSequenceGossip{},
			CreateKeyRange: &proto.CreateKeyRangeGossip{},
		}
		actual, ok := GetGossipRequestType(cmd)
		is.False(ok)
		is.Equal(GRError, actual)
	})
}
