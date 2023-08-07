package provider_test

import (
	"testing"

	"github.com/pg-sharding/spqr/coordinator/provider"
	"github.com/pg-sharding/spqr/pkg/datashard"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	proto "github.com/pg-sharding/spqr/pkg/protos"
	"github.com/pg-sharding/spqr/pkg/shard"
	"github.com/stretchr/testify/assert"
)

func TestIteratorForeach(t *testing.T) {
	assert := assert.New(t)

	iter := provider.CoordPoolIterator{}
	err := iter.ForEach(func(p shard.Shardinfo) error { return nil })

	assert.Error(err)
}

func TestCoordConnectionKepperRule(t *testing.T) {
	assert := assert.New(t)

	k := provider.CoordConnectionKepper{
		DB:  "db",
		Usr: "usr",
	}

	br := k.Rule()

	assert.Equal("db", br.DB)
	assert.Equal("usr", br.Usr)
}

func TestCoordConnectionKepperList(t *testing.T) {
	assert := assert.New(t)

	k := provider.CoordConnectionKepper{}

	assert.Nil(k.List())
}

func TestCoordConnectionKepperHostname(t *testing.T) {
	assert := assert.New(t)

	k := provider.CoordConnectionKepper{
		Host: "host",
	}

	assert.Equal("host", k.Hostname())
}

func TestCoordConnectionKepperQueueResidualSize(t *testing.T) {
	assert := assert.New(t)
	k := provider.CoordConnectionKepper{
		QueueSize: 3,
	}

	assert.Equal(3, k.QueueResidualSize())
}

func TestCoordConnectionKepperConnectionCount(t *testing.T) {
	assert := assert.New(t)
	k := provider.CoordConnectionKepper{
		ConnCount:     1,
		IdleConnCount: 2,
	}

	assert.Equal(1, k.UsedConnectionCount())
	assert.Equal(2, k.IdleConnectionCount())
}

func TestCoordConnectionKepperControlling(t *testing.T) {
	assert := assert.New(t)
	k := provider.CoordConnectionKepper{}
	err := k.Put(&datashard.Conn{})
	err1 := k.Discard(&datashard.Conn{})

	assert.Error(err)
	assert.Error(err1)
}

func TestCoordPoolConn(t *testing.T) {
	assert := assert.New(t)
	p := provider.CoordPool{}

	_, err := p.Connection("klid", kr.ShardKey{})

	assert.Error(err)
}

func TestCoordPoolNew(t *testing.T) {
	assert := assert.New(t)
	inf := &proto.PoolInfo{
		Id:            "id",
		DB:            "db",
		Usr:           "usr",
		Host:          "host",
		ConnCount:     1,
		IdleConnCount: 2,
		QueueSize:     3,
	}
	p := provider.NewCoordPool(inf)

	assert.Equal("db", p.Rule().DB)
	assert.Equal("usr", p.Rule().Usr)
	assert.Equal("host", p.Hostname())
	assert.Equal(1, p.UsedConnectionCount())
	assert.Equal(2, p.IdleConnectionCount())
	assert.Equal(3, p.QueueResidualSize())
}

func TestCoordPoolThreading(t *testing.T) {
	assert := assert.New(t)
	inf := &proto.PoolInfo{
		Id:            "id",
		DB:            "db",
		Usr:           "usr",
		Host:          "host",
		ConnCount:     1,
		IdleConnCount: 2,
		QueueSize:     3,
	}
	p := provider.NewCoordPool(inf)
	for k := 0; k < 100; k++ {
		go func() {
			for i := 0; i < 100; i++ {
				assert.Equal("db", p.Rule().DB)
				assert.Equal("usr", p.Rule().Usr)
				assert.Equal("host", p.Hostname())
				assert.Equal(1, p.UsedConnectionCount())
				assert.Equal(2, p.IdleConnectionCount())
				assert.Equal(3, p.QueueResidualSize())
			}
		}()
	}
}
