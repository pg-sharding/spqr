package pool_test

import (
	"testing"

	"github.com/pg-sharding/spqr/pkg/pool"
	"github.com/stretchr/testify/assert"
)

// TestPoolViewThreading tests the threading behavior of the PoolView.
func TestPoolViewThreading(t *testing.T) {
	assert := assert.New(t)
	inf := &pool.PoolView{
		Id:            "id",
		DB:            "db",
		Usr:           "usr",
		Host:          "host",
		ConnCount:     1,
		IdleConnCount: 2,
		QueueSize:     3,
	}
	for k := 0; k < 100; k++ {
		go func() {
			statistics := inf.View()
			for i := 0; i < 100; i++ {
				assert.Equal("db", statistics.DB)
				assert.Equal("usr", statistics.Usr)
				assert.Equal("host", statistics.Hostname)
				assert.Equal(1, statistics.UsedConnections)
				assert.Equal(2, statistics.IdleConnections)
				assert.Equal(3, statistics.QueueResidualSize)
			}
		}()
	}
}
