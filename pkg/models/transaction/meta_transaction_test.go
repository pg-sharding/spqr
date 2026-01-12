package meta_transaction

import (
	"testing"

	"github.com/google/uuid"
	proto "github.com/pg-sharding/spqr/pkg/protos"
	"github.com/pg-sharding/spqr/qdb"
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
			CmdList: []*proto.QdbTransactionCmd{
				{Command: 0, Key: "ds1",
					Value:     `{ "id": "ds1", "col_types": ["integer"], }`,
					Extension: "RelationDistribution"},
			},
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
		chunk, err := NewMetaTransactionChunk([]*proto.MetaTransactionGossipCommand{
			{
				CreateDistribution: &proto.CreateDistributionGossip{
					Distributions: []*proto.Distribution{distribution},
				},
			},
		},
			[]qdb.QdbStatement{{
				CmdType:   0,
				Key:       "ds1",
				Value:     `{ "id": "ds1", "col_types": ["integer"], }`,
				Extension: "RelationDistribution",
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

func TestGetGossipRequestType(t *testing.T) {
	is := assert.New(t)
	t.Run("happy path proto.CreateDistributionGossip", func(t *testing.T) {
		cmd := &proto.MetaTransactionGossipCommand{
			CreateDistribution: &proto.CreateDistributionGossip{},
		}
		actual := GetGossipRequestType(cmd)
		is.Equal(GR_CreateDistributionRequest, actual)
	})
	t.Run("happy path proto.CreateDistributionGossip", func(t *testing.T) {
		cmd := &proto.MetaTransactionGossipCommand{
			CreateKeyRange: &proto.CreateKeyRangeGossip{},
		}
		actual := GetGossipRequestType(cmd)
		is.Equal(GR_CreateKeyRange, actual)
	})

	t.Run("failed algebraic type parsing", func(t *testing.T) {
		cmd := &proto.MetaTransactionGossipCommand{
			CreateKeyRange:     &proto.CreateKeyRangeGossip{},
			CreateDistribution: &proto.CreateDistributionGossip{},
		}
		actual := GetGossipRequestType(cmd)
		is.Equal(GR_UNKNOWN, actual)
	})
}
