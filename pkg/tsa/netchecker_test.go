package tsa_test

import (
	"errors"
	"testing"

	"github.com/jackc/pgx/v5/pgproto3"
	mockshard "github.com/pg-sharding/spqr/pkg/mock/shard"
	"github.com/pg-sharding/spqr/pkg/tsa"
	"github.com/pg-sharding/spqr/pkg/txstatus"
	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"
)

func TestChecker_CheckTSA(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockShard := mockshard.NewMockShard(ctrl)
	mockShard.EXPECT().ID().Return(uint(42)).AnyTimes() // Add expectation for ID method
	checker := tsa.NetChecker{}

	t.Run("RW_test", func(t *testing.T) {
		mockShard.EXPECT().Send(&pgproto3.Query{String: "SHOW transaction_read_only"}).Return(nil)
		mockShard.EXPECT().Receive().Return(&pgproto3.DataRow{
			Values: [][]byte{[]byte("off")},
		}, nil)
		mockShard.EXPECT().Receive().Return(&pgproto3.ReadyForQuery{TxStatus: byte(txstatus.TXIDLE)}, nil)

		result, err := checker.CheckTSA(mockShard)
		assert.NoError(t, err)
		assert.True(t, result.Alive)
		assert.True(t, result.RW)
		assert.Equal(t, "primary", result.Reason)
	})

	t.Run("RO_test", func(t *testing.T) {
		mockShard.EXPECT().Send(&pgproto3.Query{String: "SHOW transaction_read_only"}).Return(nil)
		mockShard.EXPECT().Receive().Return(&pgproto3.DataRow{
			Values: [][]byte{[]byte("on")},
		}, nil)
		mockShard.EXPECT().Receive().Return(&pgproto3.ReadyForQuery{TxStatus: byte(txstatus.TXIDLE)}, nil)

		result, err := checker.CheckTSA(mockShard)
		assert.NoError(t, err)
		assert.True(t, result.Alive)
		assert.False(t, result.RW)
		assert.Equal(t, "replica", result.Reason)
	})

	t.Run("Error_sending_query_test", func(t *testing.T) {
		mockShard.EXPECT().Send(&pgproto3.Query{String: "SHOW transaction_read_only"}).Return(errors.New("send error"))

		result, err := checker.CheckTSA(mockShard)
		assert.Error(t, err)
		assert.False(t, result.Alive)
		assert.False(t, result.RW)
		assert.Equal(t, "failed to send transaction_read_only", result.Reason)
	})

	t.Run("Error_receiving_message_test", func(t *testing.T) {
		mockShard.EXPECT().Send(&pgproto3.Query{String: "SHOW transaction_read_only"}).Return(nil)
		mockShard.EXPECT().Receive().Return(nil, errors.New("receive an error"))

		result, err := checker.CheckTSA(mockShard)
		assert.Error(t, err)
		assert.False(t, result.Alive)
		assert.False(t, result.RW)
		assert.Equal(t, "received an error while receiving the next message", result.Reason)
	})

	t.Run("Unsync_connection", func(t *testing.T) {
		mockShard.EXPECT().Send(&pgproto3.Query{String: "SHOW transaction_read_only"}).Return(nil)
		mockShard.EXPECT().Receive().Return(&pgproto3.ReadyForQuery{TxStatus: byte(txstatus.TXACT)}, nil)

		result, err := checker.CheckTSA(mockShard)
		assert.Error(t, err)
		assert.False(t, result.Alive)
		assert.False(t, result.RW)
		assert.Equal(t, "the connection was unsynced while acquiring it", result.Reason)
	})
	t.Run("Unexpected_DataRow_values", func(t *testing.T) {
		mockShard.EXPECT().Send(&pgproto3.Query{String: "SHOW transaction_read_only"}).Return(nil)
		mockShard.EXPECT().Receive().Return(&pgproto3.DataRow{
			Values: [][]byte{[]byte("maybe")},
		}, nil)

		result, err := checker.CheckTSA(mockShard)
		assert.Error(t, err)
		assert.False(t, result.Alive)
		assert.False(t, result.RW)
		assert.Contains(t, result.Reason, "unexpected datarow received")
	})

	t.Run("Empty_DataRow_values", func(t *testing.T) {
		mockShard.EXPECT().Send(&pgproto3.Query{String: "SHOW transaction_read_only"}).Return(nil)
		mockShard.EXPECT().Receive().Return(&pgproto3.DataRow{
			Values: [][]byte{},
		}, nil)

		result, err := checker.CheckTSA(mockShard)
		assert.Error(t, err)
		assert.False(t, result.Alive)
		assert.False(t, result.RW)
		assert.Contains(t, result.Reason, "unexpected datarow received")
	})
}
