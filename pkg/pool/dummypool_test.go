package pool_test

import (
	"testing"

	"github.com/pg-sharding/spqr/pkg/datashard"
	"github.com/pg-sharding/spqr/pkg/pool"
	"github.com/stretchr/testify/assert"
)

// TestDummyPoolRule tests the Rule method of the DummyPool struct.
func TestDummyPoolRule(t *testing.T) {
	assert := assert.New(t)

	k := pool.DummyPool{
		DB:  "db",
		Usr: "usr",
	}

	br := k.Rule()

	assert.Equal("db", br.DB)
	assert.Equal("usr", br.Usr)
}

// TestDummyPoolHostname is a unit test function that tests the Hostname method of the DummyPool struct.
// It asserts that the Hostname method returns the expected hostname value.
func TestDummyPoolHostname(t *testing.T) {
	assert := assert.New(t)

	k := pool.DummyPool{
		Host: "host",
	}

	assert.Equal("host", k.Hostname())
}

// TestDummyPoolQueueResidualSize tests the QueueResidualSize method of the DummyPool struct.
func TestDummyPoolQueueResidualSize(t *testing.T) {
	assert := assert.New(t)
	k := pool.DummyPool{
		QueueSize: 3,
	}

	assert.Equal(3, k.QueueResidualSize())
}

// TestDummyPoolConnectionCount is a unit test function that tests the connection count methods of the DummyPool struct.
func TestDummyPoolConnectionCount(t *testing.T) {
	assert := assert.New(t)
	k := pool.DummyPool{
		ConnCount:     1,
		IdleConnCount: 2,
	}

	assert.Equal(1, k.UsedConnectionCount())
	assert.Equal(2, k.IdleConnectionCount())
}

// TestDummyPoolControlling is a unit test function that tests the controlling behavior of the DummyPool.
// It verifies that the Put and Discard methods of the DummyPool struct return errors as expected.
func TestDummyPoolControlling(t *testing.T) {
	assert := assert.New(t)
	k := pool.DummyPool{}
	err := k.Put(&datashard.Conn{})
	err1 := k.Discard(&datashard.Conn{})

	assert.Error(err)
	assert.Error(err1)
}

// TestDummyPoolThreading tests the threading behavior of the DummyPool.
func TestDummyPoolThreading(t *testing.T) {
	assert := assert.New(t)
	inf := &pool.DummyPool{
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
			for i := 0; i < 100; i++ {
				assert.Equal("db", inf.Rule().DB)
				assert.Equal("usr", inf.Rule().Usr)
				assert.Equal("host", inf.Hostname())
				assert.Equal(1, inf.UsedConnectionCount())
				assert.Equal(2, inf.IdleConnectionCount())
				assert.Equal(3, inf.QueueResidualSize())
			}
		}()
	}
}
