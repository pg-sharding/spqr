package tsa_test

import (
	"testing"

	"github.com/jackc/pgx/v5/pgproto3"
	mocksh "github.com/pg-sharding/spqr/pkg/mock/shard"
	"github.com/pg-sharding/spqr/pkg/txstatus"
	"github.com/stretchr/testify/assert"

	"github.com/golang/mock/gomock"
	mockinst "github.com/pg-sharding/spqr/pkg/mock/conn"
	"github.com/pg-sharding/spqr/pkg/tsa"
)

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

	res, _, err := checker.CheckTSA(sh)

	assert.NoError(err, "")
	assert.Equal(true, res)
}

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

	res, _, err := checker.CheckTSA(sh)

	assert.NoError(err, "")
	assert.Equal(false, res)
}
