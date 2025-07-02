package tsa_test

import (
	"fmt"
	"testing"
	"time"

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
	assert.True(cr.Alive)
	assert.False(cr.RO)
	assert.Equal("primary", cr.Reason)
}

// TestTSA_HostNotAlive tests the CheckTSA function when the host is not alive.
// It verifies that the CheckTSA function correctly handles the case when the shard
// is not responding or not alive, returning appropriate error and result values.
func TestTSA_HostNotAlive(t *testing.T) {
	assert := assert.New(t)
	ctrl := gomock.NewController(t)

	checker := tsa.NewTSAChecker()
	instance := mockinst.NewMockDBInstance(ctrl)

	sh := mocksh.NewMockShard(ctrl)

	sh.EXPECT().ID().AnyTimes()
	sh.EXPECT().Instance().AnyTimes().Return(instance)
	sh.EXPECT().Send(&pgproto3.Query{String: "SHOW transaction_read_only"}).Times(1)
	sh.EXPECT().Receive().Return(nil, fmt.Errorf("internet is broken"))

	instance.EXPECT().Hostname().AnyTimes().Return("host2")
	sh.EXPECT().Name().Return("sh2").AnyTimes()

	cr, err := checker.CheckTSA(sh)
	assert.Error(err)
	assert.False(cr.Alive)
}

// TestTSA_CacheExpiry tests the CheckTSA function when more than recheck period has passed.
// It verifies that the cached result expires after the recheck period and a new check is performed.
func TestTSA_CacheExpiry(t *testing.T) {
	assert := assert.New(t)
	ctrl := gomock.NewController(t)

	checker := tsa.NewTSACheckerWithDuration(time.Millisecond * 10)
	instance := mockinst.NewMockDBInstance(ctrl)

	sh := mocksh.NewMockShard(ctrl)

	sh.EXPECT().ID().AnyTimes()
	sh.EXPECT().Instance().AnyTimes().Return(instance)

	// First call expectations
	sh.EXPECT().Send(&pgproto3.Query{String: "SHOW transaction_read_only"}).Times(1)
	sh.EXPECT().Receive().Return(&pgproto3.RowDescription{}, nil).Times(1)
	sh.EXPECT().Receive().Return(&pgproto3.DataRow{
		Values: [][]byte{{'o', 'f', 'f'}},
	}, nil).Times(1)
	sh.EXPECT().Receive().Return(&pgproto3.CommandComplete{}, nil).Times(1)
	sh.EXPECT().Receive().Return(&pgproto3.ReadyForQuery{
		TxStatus: byte(txstatus.TXIDLE),
	}, nil).Times(1)

	// Second call expectations
	sh.EXPECT().Send(&pgproto3.Query{String: "SHOW transaction_read_only"}).Times(1)
	sh.EXPECT().Receive().Return(nil, fmt.Errorf("network timeout"))
	// sh.EXPECT().Receive().Return(&pgproto3.RowDescription{}, nil).Times(1)
	// sh.EXPECT().Receive().Return(&pgproto3.DataRow{
	// 	Values: [][]byte{{'o', 'n'}},
	// }, nil).Times(1)
	// sh.EXPECT().Receive().Return(&pgproto3.CommandComplete{}, nil).Times(1)
	// sh.EXPECT().Receive().Return(&pgproto3.ReadyForQuery{
	// 	TxStatus: byte(txstatus.TXIDLE),
	// }, nil).Times(1)

	instance.EXPECT().Hostname().AnyTimes().Return("host3")
	sh.EXPECT().Name().Return("sh3").AnyTimes()

	// First check
	cr1, err1 := checker.CheckTSA(sh)
	assert.NoError(err1)
	assert.True(cr1.Alive)
	assert.False(cr1.RO)

	// Wait for cache to expire
	time.Sleep(time.Second + time.Millisecond)

	// Second check should trigger a new TSA check ()
	cr2, err2 := checker.CheckTSA(sh)
	assert.EqualError(err2, "network timeout")
	assert.False(cr2.Alive)
	assert.False(cr2.RO)
}

// TestTSA_CacheHit tests the CheckTSA function when the cache is still valid.
// It verifies that the cached result is returned without performing a new check.
func TestTSA_CacheHit(t *testing.T) {
	assert := assert.New(t)
	ctrl := gomock.NewController(t)

	checker := tsa.NewTSACheckerWithDuration(time.Second * 10)
	instance := mockinst.NewMockDBInstance(ctrl)

	sh := mocksh.NewMockShard(ctrl)

	sh.EXPECT().ID().AnyTimes()
	sh.EXPECT().Instance().AnyTimes().Return(instance)

	// Only one call expected since second call should hit cache
	sh.EXPECT().Send(&pgproto3.Query{String: "SHOW transaction_read_only"}).Times(1)
	sh.EXPECT().Receive().Return(&pgproto3.RowDescription{}, nil).Times(1)
	sh.EXPECT().Receive().Return(&pgproto3.DataRow{
		Values: [][]byte{{'o', 'n'}},
	}, nil).Times(1)
	sh.EXPECT().Receive().Return(&pgproto3.CommandComplete{}, nil).Times(1)
	sh.EXPECT().Receive().Return(&pgproto3.ReadyForQuery{
		TxStatus: byte(txstatus.TXIDLE),
	}, nil).Times(1)

	instance.EXPECT().Hostname().AnyTimes().Return("host4")
	sh.EXPECT().Name().Return("sh4").AnyTimes()

	// First check
	cr1, err1 := checker.CheckTSA(sh)
	assert.NoError(err1)
	assert.True(cr1.Alive)
	assert.True(cr1.RO)

	// Second check should hit cache
	cr2, err2 := checker.CheckTSA(sh)
	assert.NoError(err2)
	assert.True(cr2.Alive)
	assert.True(cr2.RO)
	assert.Equal(cr1, cr2)
}
