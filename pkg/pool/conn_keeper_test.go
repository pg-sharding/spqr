package pool_test

import (
	"testing"

	"github.com/pg-sharding/spqr/pkg/datashard"
	"github.com/pg-sharding/spqr/pkg/pool"
	"github.com/stretchr/testify/assert"
)

// TestCoordConnectionKepperRule tests the Rule method of the ConnectionKepperData struct.
func TestCoordConnectionKepperRule(t *testing.T) {
	assert := assert.New(t)

	k := pool.ConnectionKepperData{
		DB:  "db",
		Usr: "usr",
	}

	br := k.Rule()

	assert.Equal("db", br.DB)
	assert.Equal("usr", br.Usr)
}

// TestCoordConnectionKepperList is a unit test function that tests the List method of the ConnectionKepperData struct.
func TestCoordConnectionKepperList(t *testing.T) {
	assert := assert.New(t)

	k := pool.ConnectionKepperData{}

	assert.Nil(k.List())
}

// TestCoordConnectionKepperHostname is a unit test function that tests the Hostname method of the ConnectionKepperData struct.
// It asserts that the Hostname method returns the expected hostname value.
func TestCoordConnectionKepperHostname(t *testing.T) {
	assert := assert.New(t)

	k := pool.ConnectionKepperData{
		Host: "host",
	}

	assert.Equal("host", k.Hostname())
}

// TestCoordConnectionKepperQueueResidualSize tests the QueueResidualSize method of the ConnectionKepperData struct.
func TestCoordConnectionKepperQueueResidualSize(t *testing.T) {
	assert := assert.New(t)
	k := pool.ConnectionKepperData{
		QueueSize: 3,
	}

	assert.Equal(3, k.QueueResidualSize())
}

// TestCoordConnectionKepperConnectionCount is a unit test function that tests the connection count methods of the ConnectionKepperData struct.
func TestCoordConnectionKepperConnectionCount(t *testing.T) {
	assert := assert.New(t)
	k := pool.ConnectionKepperData{
		ConnCount:     1,
		IdleConnCount: 2,
	}

	assert.Equal(1, k.UsedConnectionCount())
	assert.Equal(2, k.IdleConnectionCount())
}

// TestCoordConnectionKepperControlling is a unit test function that tests the controlling behavior of the CoordConnectionKepper.
// It verifies that the Put and Discard methods of the ConnectionKepperData struct return errors as expected.
func TestCoordConnectionKepperControlling(t *testing.T) {
	assert := assert.New(t)
	k := pool.ConnectionKepperData{}
	err := k.Put(&datashard.Conn{})
	err1 := k.Discard(&datashard.Conn{})

	assert.Error(err)
	assert.Error(err1)
}

// TestCoordConnectionKepperThreading tests the threading behavior of the CoordConnectionKepper.
func TestCoordConnectionKepperThreading(t *testing.T) {
	assert := assert.New(t)
	inf := &pool.ConnectionKepperData{
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
