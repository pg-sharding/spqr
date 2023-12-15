package provider_test

import (
	"testing"

	"github.com/pg-sharding/spqr/coordinator/provider"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	proto "github.com/pg-sharding/spqr/pkg/protos"
	"github.com/pg-sharding/spqr/pkg/shard"
	"github.com/stretchr/testify/assert"
)

func TestIteratorForeach(t *testing.T) {
	assert := assert.New(t)
	p := provider.CoordPool{}

	err := p.ForEach(func(p shard.Shardinfo) error { return nil })

	assert.Error(err)
}

func TestCoordPoolConn(t *testing.T) {
	assert := assert.New(t)
	p := provider.CoordPool{}

	_, err := p.Connection(0x12345678, kr.ShardKey{})

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
