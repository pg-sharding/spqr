package pool_test

import (
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/pg-sharding/spqr/pkg/config"
	mockinst "github.com/pg-sharding/spqr/pkg/mock/conn"
	mockpool "github.com/pg-sharding/spqr/pkg/mock/pool"
	mockshard "github.com/pg-sharding/spqr/pkg/mock/shard"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/pkg/pool"
	"github.com/pg-sharding/spqr/pkg/startup"
	"github.com/pg-sharding/spqr/pkg/txstatus"
	"github.com/stretchr/testify/assert"
)

func TestDbPoolOrderCaching(t *testing.T) {
	assert := assert.New(t)

	ctrl := gomock.NewController(t)

	underyling_pool := mockpool.NewMockMultiShardPool(ctrl)

	key := kr.ShardKey{
		Name: "sh1",
	}

	clId := uint(1)

	dbpool := pool.NewDBPoolFromMultiPool(map[string]*config.Shard{
		key.Name: &config.Shard{
			Hosts: []string{
				"h1",
				"h2",
				"h3",
			},
		},
	}, &startup.StartupParams{}, underyling_pool, false, time.Hour)

	ins1 := mockinst.NewMockDBInstance(ctrl)
	ins1.EXPECT().Hostname().AnyTimes().Return("h1")

	ins2 := mockinst.NewMockDBInstance(ctrl)
	ins2.EXPECT().Hostname().AnyTimes().Return("h2")

	ins3 := mockinst.NewMockDBInstance(ctrl)
	ins3.EXPECT().Hostname().AnyTimes().Return("h3")

	h1 := mockshard.NewMockShard(ctrl)
	h1.EXPECT().Instance().AnyTimes().Return(ins1)

	h2 := mockshard.NewMockShard(ctrl)
	h2.EXPECT().Instance().AnyTimes().Return(ins2)

	h3 := mockshard.NewMockShard(ctrl)
	h3.EXPECT().Instance().AnyTimes().Return(ins3)

	h1.EXPECT().ID().AnyTimes().Return(uint(1))

	h2.EXPECT().ID().AnyTimes().Return(uint(2))

	h3.EXPECT().ID().AnyTimes().Return(uint(3))

	hs := []*mockshard.MockShard{
		h1, h2, h3,
	}

	underyling_pool.EXPECT().Connection(clId, key, "h1").Times(1).Return(h1, nil)
	underyling_pool.EXPECT().Connection(clId, key, "h2").Times(1).Return(h2, nil)
	underyling_pool.EXPECT().Connection(clId, key, "h3").Times(1).Return(h3, nil)

	for ind, h := range hs {

		if ind < 2 {
			underyling_pool.EXPECT().Put(h).Return(nil)

			h.EXPECT().Sync().Return(int64(0))

			h.EXPECT().TxStatus().Return(txstatus.TXIDLE)
		}

		h.EXPECT().Send(&pgproto3.Query{String: "SHOW transaction_read_only"}).Times(1)
		h.EXPECT().Receive().Return(&pgproto3.RowDescription{}, nil)
		if ind == 2 {
			h.EXPECT().Receive().DoAndReturn(func() (pgproto3.BackendMessage, error) {

				return &pgproto3.DataRow{
					Values: [][]byte{
						[]byte("off"),
					},
				}, nil
			})
		} else {
			h.EXPECT().Receive().DoAndReturn(func() (pgproto3.BackendMessage, error) {
				return &pgproto3.DataRow{
					Values: [][]byte{
						[]byte("on"),
					},
				}, nil
			})
		}

		h.EXPECT().Receive().Return(&pgproto3.CommandComplete{}, nil)

		h.EXPECT().Receive().Return(&pgproto3.ReadyForQuery{TxStatus: byte(txstatus.TXIDLE)}, nil)
	}

	sh, err := dbpool.Connection(clId, key, config.TargetSessionAttrsRW)

	assert.Equal(sh, h3)

	assert.NoError(err)

	/* next time expect only one call */
	underyling_pool.EXPECT().Connection(clId, key, "h3").Times(1).Return(h3, nil)

	sh, err = dbpool.Connection(clId, key, config.TargetSessionAttrsRW)

	assert.Equal(sh, h3)

	assert.NoError(err)
}

func TestDbPoolReadOnlyOrderDistribution(t *testing.T) {
	assert := assert.New(t)

	ctrl := gomock.NewController(t)

	underyling_pool := mockpool.NewMockMultiShardPool(ctrl)

	key := kr.ShardKey{
		Name: "sh1",
	}

	clId := uint(1)

	dbpool := pool.NewDBPoolFromMultiPool(map[string]*config.Shard{
		key.Name: &config.Shard{
			Hosts: []string{
				"h1",
				"h2",
				"h3",
			},
		},
	}, &startup.StartupParams{}, underyling_pool, false, time.Hour)

	ins1 := mockinst.NewMockDBInstance(ctrl)
	ins1.EXPECT().Hostname().AnyTimes().Return("h1")

	ins2 := mockinst.NewMockDBInstance(ctrl)
	ins2.EXPECT().Hostname().AnyTimes().Return("h2")

	ins3 := mockinst.NewMockDBInstance(ctrl)
	ins3.EXPECT().Hostname().AnyTimes().Return("h3")

	h1 := mockshard.NewMockShard(ctrl)
	h1.EXPECT().Instance().AnyTimes().Return(ins1)

	h2 := mockshard.NewMockShard(ctrl)
	h2.EXPECT().Instance().AnyTimes().Return(ins2)

	h3 := mockshard.NewMockShard(ctrl)
	h3.EXPECT().Instance().AnyTimes().Return(ins3)

	h1.EXPECT().ID().AnyTimes().Return(uint(1))

	h2.EXPECT().ID().AnyTimes().Return(uint(2))

	h3.EXPECT().ID().AnyTimes().Return(uint(3))

	hs := []*mockshard.MockShard{
		h1, h2, h3,
	}

	underyling_pool.EXPECT().Connection(clId, key, "h1").AnyTimes().Return(h1, nil)
	underyling_pool.EXPECT().Connection(clId, key, "h2").AnyTimes().Return(h2, nil)
	underyling_pool.EXPECT().Connection(clId, key, "h3").Times(1).Return(h3, nil)

	for ind, h := range hs {

		if ind < 2 {
			underyling_pool.EXPECT().Put(h).Return(nil)

			h.EXPECT().Sync().Return(int64(0))

			h.EXPECT().TxStatus().Return(txstatus.TXIDLE)
		}

		h.EXPECT().Send(&pgproto3.Query{String: "SHOW transaction_read_only"}).Times(1)
		h.EXPECT().Receive().Return(&pgproto3.RowDescription{}, nil)
		if ind == 2 {
			h.EXPECT().Receive().DoAndReturn(func() (pgproto3.BackendMessage, error) {

				return &pgproto3.DataRow{
					Values: [][]byte{
						[]byte("off"),
					},
				}, nil
			})
		} else {
			h.EXPECT().Receive().DoAndReturn(func() (pgproto3.BackendMessage, error) {
				return &pgproto3.DataRow{
					Values: [][]byte{
						[]byte("on"),
					},
				}, nil
			})
		}

		h.EXPECT().Receive().Return(&pgproto3.CommandComplete{}, nil)

		h.EXPECT().Receive().Return(&pgproto3.ReadyForQuery{TxStatus: byte(txstatus.TXIDLE)}, nil)
	}

	sh, err := dbpool.Connection(clId, key, config.TargetSessionAttrsRW)

	assert.Equal(sh, h3)

	assert.NoError(err)

	underyling_pool.EXPECT().Connection(clId, key, "h3").MaxTimes(1).Return(h3, nil)

	underyling_pool.EXPECT().Put(h3).Return(nil).MaxTimes(1)

	h3.EXPECT().Sync().Return(int64(0)).MaxTimes(1)

	h3.EXPECT().TxStatus().Return(txstatus.TXIDLE).MaxTimes(1)

	repeattimes := 1000

	cnth1 := 0
	cnth2 := 0

	dbpool.SetShuffleHosts(true)

	for i := 0; i < repeattimes; i++ {
		sh, err = dbpool.Connection(clId, key, config.TargetSessionAttrsRO)

		// assert.NotEqual(sh, h3)

		if sh == h1 {
			cnth1++
		} else {
			cnth2++
		}

		assert.NoError(err)
	}

	diff := cnth1 - cnth2
	if diff < 0 {
		diff = -diff
	}

	assert.Less(diff, 90)
	assert.Equal(repeattimes, cnth1+cnth2)
}
