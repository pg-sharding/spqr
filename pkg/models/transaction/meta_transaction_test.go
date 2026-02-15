package meta_transaction

import (
	"testing"

	"github.com/google/uuid"
	proto "github.com/pg-sharding/spqr/pkg/protos"
	"github.com/stretchr/testify/assert"
)

func TestTransactionFromProto(t *testing.T) {
	is := assert.New(t)
	t.Run("happy path", func(t *testing.T) {
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
		actual := checkCommandPart(nil, GR_ERROR, GR_CreateKeyRange)
		is.Equal(actual, GR_ERROR)
		data := &proto.CreateDistributionGossip{}
		actual = checkCommandPart(data, GR_ERROR, GR_UNKNOWN)
		is.Equal(actual, GR_ERROR)
		var data1 *proto.CreateDistributionGossip
		actual = checkCommandPart(data1, GR_ERROR, GR_CreateKeyRange)
		is.Equal(actual, GR_ERROR)
	})
	t.Run("nil passed no changes", func(t *testing.T) {
		is := assert.New(t)
		actual := checkCommandPart(nil, GR_CreateKeyRange, GR_CreateKeyRange)
		is.Equal(actual, GR_CreateKeyRange)
		actual = checkCommandPart(nil, GR_UNKNOWN, GR_UNKNOWN)
		is.Equal(actual, GR_UNKNOWN)
	})
	t.Run("empty data passed no changes", func(t *testing.T) {
		is := assert.New(t)
		var data *proto.CreateDistributionGossip
		actual := checkCommandPart(data, GR_CreateDistributionRequest, GR_CreateDistributionRequest)
		is.Equal(actual, GR_CreateDistributionRequest)
		actual = checkCommandPart(data, GR_UNKNOWN, GR_UNKNOWN)
		is.Equal(actual, GR_UNKNOWN)
	})
	t.Run("NO EMPTY data passed GR_UNKNOWN current", func(t *testing.T) {
		is := assert.New(t)
		data := &proto.CreateDistributionGossip{}
		actual := checkCommandPart(data, GR_UNKNOWN, GR_CreateDistributionRequest)
		is.Equal(actual, GR_CreateDistributionRequest)
	})
	t.Run("NO EMPTY data passed current IS NOT GR_UNKNOWN", func(t *testing.T) {
		is := assert.New(t)
		data := &proto.CreateDistributionGossip{}
		actual := checkCommandPart(data, GR_DropKeyRange, GR_CreateDistributionRequest)
		is.Equal(actual, GR_ERROR)
	})
}

func TestGetGossipRequestType(t *testing.T) {
	is := assert.New(t)
	t.Run("happy path proto.CreateDistributionGossip", func(t *testing.T) {
		cmd := &proto.MetaTransactionGossipCommand{
			CreateDistribution: &proto.CreateDistributionGossip{},
		}
		actual, recognized := GetGossipRequestType(cmd)
		is.True(recognized)
		is.Equal(GR_CreateDistributionRequest, actual)
	})
	t.Run("happy path proto.CreateDistributionGossip", func(t *testing.T) {
		cmd := &proto.MetaTransactionGossipCommand{
			CreateKeyRange: &proto.CreateKeyRangeGossip{},
		}
		actual, recognized := GetGossipRequestType(cmd)
		is.True(recognized)
		is.Equal(GR_CreateKeyRange, actual)
	})

	t.Run("failed algebraic type parsing", func(t *testing.T) {
		cmd := &proto.MetaTransactionGossipCommand{
			CreateKeyRange:     &proto.CreateKeyRangeGossip{},
			CreateDistribution: &proto.CreateDistributionGossip{},
		}
		actual, recognized := GetGossipRequestType(cmd)
		is.False(recognized)
		is.Equal(GR_ERROR, actual)
	})

	t.Run("happy path proto.DropKeyRangeGossip", func(t *testing.T) {
		cmd := &proto.MetaTransactionGossipCommand{
			DropKeyRange: &proto.DropKeyRangeGossip{},
		}
		actual, recognized := GetGossipRequestType(cmd)
		is.True(recognized)
		is.Equal(GR_DropKeyRange, actual)
	})
	t.Run("failed algebraic type parsing, case 2", func(t *testing.T) {
		cmd := &proto.MetaTransactionGossipCommand{
			CreateKeyRange: &proto.CreateKeyRangeGossip{},
			DropKeyRange:   &proto.DropKeyRangeGossip{},
		}
		actual, recognized := GetGossipRequestType(cmd)
		is.False(recognized)
		is.Equal(GR_ERROR, actual)
	})
	t.Run("failed algebraic type parsing, case 2", func(t *testing.T) {
		cmd := &proto.MetaTransactionGossipCommand{}
		actual, recognized := GetGossipRequestType(cmd)
		is.False(recognized)
		is.Equal(GR_UNKNOWN, actual)
	})
	t.Run("failed algebraic type parsing, case: NOT NILL, NIL, NOT NILL", func(t *testing.T) {
		cmd := &proto.MetaTransactionGossipCommand{
			CreateDistribution: &proto.CreateDistributionGossip{},
			DropKeyRange:       &proto.DropKeyRangeGossip{},
		}
		actual, recognized := GetGossipRequestType(cmd)
		is.False(recognized)
		is.Equal(GR_ERROR, actual)
	})
}
