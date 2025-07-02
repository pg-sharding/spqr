package tsa_test

import (
	"testing"

	"github.com/jackc/pgx/v5/pgproto3"
	mocksh "github.com/pg-sharding/spqr/pkg/mock/shard"
	"github.com/pg-sharding/spqr/pkg/txstatus"
	"github.com/stretchr/testify/assert"

	mockinst "github.com/pg-sharding/spqr/pkg/mock/conn"
	"github.com/pg-sharding/spqr/pkg/tsa"
	"go.uber.org/mock/gomock"
)

// TestTSA_RW is a unit test function that tests the CheckTSA function of the TSA package.
// It verifies that the CheckTSA function correctly checks the transaction read-only status.
// The function sets up a mock DB instance and a mock shard, and expects certain method calls
// on the mock objects. It then calls the CheckTSA function and asserts that the result and error
// values are as expected.
func TestTSA_RW(t *testing.T) {

	assert := assert.New(t)
	ctrl := gomock.NewController(t)

	checker := tsa.NewTSAChecker()
	instance := mockinst.NewMockDBInstance(ctrl)

	sh := mocksh.NewMockShard(ctrl)

	sh.EXPECT().ID().AnyTimes()

	sh.EXPECT().Instance().AnyTimes().Return(instance)
	sh.EXPECT().Send(&pgproto3.Query{String: "SHOW transaction_read_only"}).Times(1)

	sh.EXPECT().Receive().Return(&pgproto3.RowDescription{}, nil)

	sh.EXPECT().Receive().Return(&pgproto3.DataRow{
		Values: [][]byte{
			{
				'o',
				'f',
				'f',
			},
		},
	}, nil)

	sh.EXPECT().Receive().Return(&pgproto3.CommandComplete{}, nil)

	sh.EXPECT().Receive().Return(&pgproto3.ReadyForQuery{
		TxStatus: byte(txstatus.TXIDLE),
	}, nil)

	instance.EXPECT().Hostname().AnyTimes().Return("host1")
	sh.EXPECT().Name().Return("sh1").AnyTimes()

	cr, err := checker.CheckTSA(sh)
	assert.NoError(err)
	assert.True(cr.Alive) // done
	assert.False(cr.RO)   // done
	assert.Equal("primary", cr.Reason)
}

// TestTSA_RO is a unit test function that tests the CheckTSA function of the TSA checker.
// It verifies the behavior of the CheckTSA function when the transaction_read_only setting is 'on'.
// The function sets up the necessary mocks and expectations, and then calls the CheckTSA function.
// It asserts that the returned result is false, indicating that the transaction is not read-only.
func TestTSA_RO(t *testing.T) {

	assert := assert.New(t)
	ctrl := gomock.NewController(t)

	checker := tsa.NewTSAChecker()
	instance := mockinst.NewMockDBInstance(ctrl)

	sh := mocksh.NewMockShard(ctrl)

	sh.EXPECT().ID().AnyTimes()

	sh.EXPECT().Instance().AnyTimes().Return(instance)
	sh.EXPECT().Send(&pgproto3.Query{String: "SHOW transaction_read_only"}).Times(1)

	sh.EXPECT().Receive().Return(&pgproto3.RowDescription{}, nil)

	sh.EXPECT().Receive().Return(&pgproto3.DataRow{
		Values: [][]byte{
			{
				'o',
				'n',
			},
		},
	}, nil)

	sh.EXPECT().Receive().Return(&pgproto3.CommandComplete{}, nil)

	sh.EXPECT().Receive().Return(&pgproto3.ReadyForQuery{
		TxStatus: byte(txstatus.TXIDLE),
	}, nil)

	instance.EXPECT().Hostname().AnyTimes().Return("host1")
	sh.EXPECT().Name().Return("sh1").AnyTimes()

	cr, err := checker.CheckTSA(sh)

	assert.NoError(err)
	assert.True(cr.Alive) // done
	assert.True(cr.RO)    // done
	assert.Equal("replica", cr.Reason)
}

// add test when host is not alive
// add test when more than one seconds passed since last check
