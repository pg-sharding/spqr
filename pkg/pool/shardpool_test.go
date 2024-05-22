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

// TestShardPoolConnectionAcquirePut tests the acquisition and putting of connections in the ShardPool.
// It verifies that the ShardPool correctly acquires a connection and puts it back after use.
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

// TestShardPoolConnectionAcquireDiscard tests the acquisition and discarding of connections in the ShardPool.
// It verifies that the ShardPool correctly acquires and discards connections, and updates the connection counts accordingly.
// This test uses a mock DBInstance and a mock Shard for testing purposes.
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

// TestShardPoolAllocFnError tests the behavior of the ShardPool when the allocation function returns an error.
// It verifies that the ShardPool correctly handles the error and updates its internal state accordingly.
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

// TestShardPoolConnectionAcquireLimit tests the connection acquisition limit of the ShardPool.
// It creates a pool of shard connections and simulates multiple goroutines trying to acquire and release connections.
// The test ensures that the connection acquisition limit is respected and that connections are properly released.
// It uses the assert package for assertions and the gomock package for creating mock objects.
// The test expects the connection limit to be set to 10 and checks that at least 15% of the connection acquisition attempts are successful.
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
