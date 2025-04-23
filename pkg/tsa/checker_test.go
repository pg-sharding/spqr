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
	checker := tsa.Checker{}

	t.Run("RW test", func(t *testing.T) {
		mockShard.EXPECT().Send(&pgproto3.Query{String: "SHOW transaction_read_only"}).Return(nil)
		mockShard.EXPECT().Receive().Return(&pgproto3.DataRow{
			Values: [][]byte{[]byte("off")},
		}, nil)
		mockShard.EXPECT().Receive().Return(&pgproto3.ReadyForQuery{TxStatus: byte(txstatus.TXIDLE)}, nil)

		result, err := checker.CheckTSA(mockShard)
		assert.NoError(t, err)
		assert.True(t, result.RW)
		assert.Equal(t, "is primary", result.Reason)
	})

	t.Run("RO test", func(t *testing.T) {
		mockShard.EXPECT().Send(&pgproto3.Query{String: "SHOW transaction_read_only"}).Return(nil)
		mockShard.EXPECT().Receive().Return(&pgproto3.DataRow{
			Values: [][]byte{[]byte("on")},
		}, nil)
		mockShard.EXPECT().Receive().Return(&pgproto3.ReadyForQuery{TxStatus: byte(txstatus.TXIDLE)}, nil)

		result, err := checker.CheckTSA(mockShard)
		assert.NoError(t, err)
		assert.False(t, result.RW)
		assert.Equal(t, "transaction_read_only is [[111 110]]", result.Reason)
	})

	t.Run("Error sending query test", func(t *testing.T) {
		mockShard.EXPECT().Send(&pgproto3.Query{String: "SHOW transaction_read_only"}).Return(errors.New("send error"))

		result, err := checker.CheckTSA(mockShard)
		assert.Error(t, err)
		assert.False(t, result.RW)
		assert.Equal(t, "error while sending read-write check", result.Reason)
	})

	t.Run("Error receiving message test", func(t *testing.T) {
		mockShard.EXPECT().Send(&pgproto3.Query{String: "SHOW transaction_read_only"}).Return(nil)
		mockShard.EXPECT().Receive().Return(nil, errors.New("receive error"))

		result, err := checker.CheckTSA(mockShard)
		assert.Error(t, err)
		assert.False(t, result.RW)
		assert.Equal(t, "zero datarow received", result.Reason)
	})

	t.Run("Unsync Connection", func(t *testing.T) {
		mockShard.EXPECT().Send(&pgproto3.Query{String: "SHOW transaction_read_only"}).Return(nil)
		mockShard.EXPECT().Receive().Return(&pgproto3.ReadyForQuery{TxStatus: byte(txstatus.TXACT)}, nil)

		result, err := checker.CheckTSA(mockShard)
		assert.Error(t, err)
		assert.False(t, result.RW)
		assert.Equal(t, "zero datarow received", result.Reason)
	})
}
