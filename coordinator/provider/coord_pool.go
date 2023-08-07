package provider

import (
	"fmt"
	"sync"

	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	protos "github.com/pg-sharding/spqr/pkg/protos"
	"github.com/pg-sharding/spqr/pkg/shard"
)

type CoordPoolImpl struct {
	CoordConnectionKepperImpl
	CoordPoolIterator
}

func NewCoordPool(info *protos.PoolInfo) *CoordPoolImpl {
	return &CoordPoolImpl{
		CoordConnectionKepperImpl: CoordConnectionKepperImpl{
			Id:            info.Id,
			DB:            info.DB,
			Usr:           info.Usr,
			Host:          info.Host,
			ConnCount:     info.ConnCount,
			IdleConnCount: info.IdleConnCount,
			QueueSize:     info.QueueSize,
			m:             sync.RWMutex{},
		},
		CoordPoolIterator: CoordPoolIterator{},
	}
}

func (r *CoordPoolImpl) Connection(clid string, shardKey kr.ShardKey) (shard.Shard, error) {
	return nil, fmt.Errorf("unimplemented")
}

type CoordConnectionKepperImpl struct {
	Id            string
	DB            string
	Usr           string
	Host          string
	ConnCount     int64
	IdleConnCount int64
	QueueSize     int64

	m sync.RWMutex
}

func (r *CoordConnectionKepperImpl) Put(host shard.Shard) error {
	return fmt.Errorf("unimplemented")
}

func (r *CoordConnectionKepperImpl) Discard(sh shard.Shard) error {
	return fmt.Errorf("unimplemented")
}

func (r *CoordConnectionKepperImpl) UsedConnectionCount() int {
	r.m.Lock()
	defer r.m.Unlock()

	return int(r.ConnCount)
}

func (r *CoordConnectionKepperImpl) IdleConnectionCount() int {
	r.m.Lock()
	defer r.m.Unlock()

	return int(r.IdleConnCount)
}

func (r *CoordConnectionKepperImpl) QueueResidualSize() int {
	r.m.Lock()
	defer r.m.Unlock()

	return int(r.QueueSize)
}

func (r *CoordConnectionKepperImpl) Hostname() string {
	r.m.Lock()
	defer r.m.Unlock()

	return r.Host
}

func (r *CoordConnectionKepperImpl) List() []shard.Shard {
	return nil
}

func (r *CoordConnectionKepperImpl) Rule() *config.BackendRule {
	r.m.Lock()
	defer r.m.Unlock()

	return &config.BackendRule{
		DB:  r.DB,
		Usr: r.Usr,
	}
}

type CoordPoolIterator struct {
}

func (r *CoordPoolIterator) ForEach(cb func(p shard.Shardinfo) error) error {
	return fmt.Errorf("unimplemented")
}
