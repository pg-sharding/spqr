package pool_test

import (
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

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

func TestShardPoolConnectionAcquireDiscard(t *testing.T) {

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

func TestShardPoolConnectionAcquireLimit(t *testing.T) {

	connLimit := 10

	assert := assert.New(t)
	ctrl := gomock.NewController(t)

	conns := make(map[uint]shard.Shard, connLimit)

	used := make(map[uint]bool, connLimit)

	for i := 0; i < connLimit; i++ {
		shardconn := mockshard.NewMockShard(ctrl)

		ins := mockinst.NewMockDBInstance(ctrl)
		ins.EXPECT().Hostname().AnyTimes().Return(fmt.Sprintf("h%d", i))

		shardconn.EXPECT().Instance().AnyTimes().Return(ins)
		shardconn.EXPECT().ID().AnyTimes().Return(uint(1234*100 + i))
		shardconn.EXPECT().TxStatus().AnyTimes().Return(txstatus.TXIDLE)

		conns[shardconn.ID()] = shardconn
		used[shardconn.ID()] = false
	}

	var mu sync.Mutex

	shp := pool.NewShardPool(func(shardKey kr.ShardKey, host string, rule *config.BackendRule) (shard.Shard, error) {
		mu.Lock()
		defer mu.Unlock()

		for _, sh := range conns {
			if !used[sh.ID()] {
				used[sh.ID()] = true
				return sh, nil
			}
		}

		assert.Fail("connection pool overflow")

		return nil, errors.New("bad")
	}, "h1", &config.BackendRule{
		ConnectionLimit:   connLimit,
		ConnectionRetries: 1,
	})

	var wg sync.WaitGroup

	var cntExec atomic.Uint64

	wg.Add(20)

	for id := 0; id < 20; id++ {
		go func() {
			defer wg.Done()

			for it := 0; it < 100; it++ {
				conn, err := shp.Connection(1, kr.ShardKey{
					Name: "1",
				})
				if err != nil {
					// too much connections
					continue
				}

				assert.NotNil(conn)

				// imitate use
				time.Sleep(time.Duration(1+rand.Uint32()%50) * time.Millisecond)

				mu.Lock()
				cntExec.Add(1)

				used[conn.ID()] = false

				_ = shp.Put(conn)

				mu.Unlock()
			}
		}()
	}

	wg.Wait()

	// no more that 25% failure
	assert.Greater(cntExec.Load(), uint64(15*100))
}
