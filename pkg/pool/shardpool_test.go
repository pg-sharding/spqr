package pool_test

import (
	"errors"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/pg-sharding/spqr/pkg/config"
	mockinst "github.com/pg-sharding/spqr/pkg/mock/conn"
	mockshard "github.com/pg-sharding/spqr/pkg/mock/shard"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/pkg/pool"
	"github.com/pg-sharding/spqr/pkg/shard"
	"github.com/pg-sharding/spqr/pkg/txstatus"
	"github.com/stretchr/testify/assert"
)

func TestShardPoolConnectionAcquirePut(t *testing.T) {

	assert := assert.New(t)
	ctrl := gomock.NewController(t)

	ins := mockinst.NewMockDBInstance(ctrl)
	ins.EXPECT().Hostname().AnyTimes().Return("h1")

	shardconn := mockshard.NewMockShard(ctrl)
	shardconn.EXPECT().Instance().AnyTimes().Return(ins)
	shardconn.EXPECT().ID().AnyTimes().Return(uint(1234))
	shardconn.EXPECT().TxStatus().AnyTimes().Return(txstatus.TXIDLE)

	shp := pool.NewShardPool(func(shardKey kr.ShardKey, host string, rule *config.BackendRule) (shard.Shard, error) {
		return shardconn, nil
	}, "h1", &config.BackendRule{
		ConnectionLimit: 1,
	})

	assert.Equal(1, shp.QueueResidualSize())
	assert.Equal(0, shp.IdleConnectionCount())

	conn, err := shp.Connection(10, kr.ShardKey{
		Name: "sh1",
	})

	assert.NoError(err)
	assert.Equal(shardconn, conn)

	assert.Equal(0, shp.IdleConnectionCount())
	assert.Equal(0, shp.QueueResidualSize())

	assert.NoError(shp.Put(shardconn))

	assert.Equal(1, shp.QueueResidualSize())
	assert.Equal(1, shp.IdleConnectionCount())
}

func TestShardPoolConnectionAcquireDisacrd(t *testing.T) {

	assert := assert.New(t)
	ctrl := gomock.NewController(t)

	ins := mockinst.NewMockDBInstance(ctrl)
	ins.EXPECT().Hostname().AnyTimes().Return("h1")

	shardconn := mockshard.NewMockShard(ctrl)
	shardconn.EXPECT().Instance().AnyTimes().Return(ins)
	shardconn.EXPECT().ID().AnyTimes().Return(uint(1234))
	shardconn.EXPECT().TxStatus().AnyTimes().Return(txstatus.TXIDLE)

	shardconn.EXPECT().Close().Times(1)

	shp := pool.NewShardPool(func(shardKey kr.ShardKey, host string, rule *config.BackendRule) (shard.Shard, error) {
		return shardconn, nil
	}, "h1", &config.BackendRule{
		ConnectionLimit: 1,
	})

	assert.Equal(1, shp.QueueResidualSize())
	assert.Equal(0, shp.IdleConnectionCount())

	conn, err := shp.Connection(10, kr.ShardKey{
		Name: "sh1",
	})

	assert.NoError(err)
	assert.Equal(shardconn, conn)

	assert.Equal(0, shp.IdleConnectionCount())
	assert.Equal(0, shp.QueueResidualSize())

	assert.NoError(shp.Discard(shardconn))

	assert.Equal(1, shp.QueueResidualSize())
	assert.Equal(0, shp.IdleConnectionCount())
}

func TestShardPoolAllocFnError(t *testing.T) {

	assert := assert.New(t)
	ctrl := gomock.NewController(t)

	ins := mockinst.NewMockDBInstance(ctrl)
	ins.EXPECT().Hostname().AnyTimes().Return("h1")

	shp := pool.NewShardPool(func(shardKey kr.ShardKey, host string, rule *config.BackendRule) (shard.Shard, error) {
		return nil, errors.New("bad")
	}, "h1", &config.BackendRule{
		ConnectionLimit: 1,
	})

	assert.Equal(1, shp.QueueResidualSize())
	assert.Equal(0, shp.IdleConnectionCount())

	conn, err := shp.Connection(10, kr.ShardKey{
		Name: "sh1",
	})

	assert.Error(err)
	assert.Nil(conn)

	assert.Equal(0, shp.IdleConnectionCount())
	assert.Equal(1, shp.QueueResidualSize())
}
