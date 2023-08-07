package provider

import (
	"fmt"
	"sync"

	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	protos "github.com/pg-sharding/spqr/pkg/protos"
	"github.com/pg-sharding/spqr/pkg/shard"
)

type CoordPool struct {
	CoordConnectionKepper
	CoordPoolIterator
}

func NewCoordPool(info *protos.PoolInfo) *CoordPool {
	return &CoordPool{
		CoordConnectionKepper: CoordConnectionKepper{
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

func (r *CoordPool) Connection(clid string, shardKey kr.ShardKey) (shard.Shard, error) {
	return nil, fmt.Errorf("unimplemented")
}

type CoordConnectionKepper struct {
	Id            string
	DB            string
	Usr           string
	Host          string
	ConnCount     int64
	IdleConnCount int64
	QueueSize     int64

	m sync.RWMutex
}

func (r *CoordConnectionKepper) Put(host shard.Shard) error {
	return fmt.Errorf("unimplemented")
}

func (r *CoordConnectionKepper) Discard(sh shard.Shard) error {
	return fmt.Errorf("unimplemented")
}

func (r *CoordConnectionKepper) UsedConnectionCount() int {
	r.m.Lock()
	defer r.m.Unlock()

	return int(r.ConnCount)
}

func (r *CoordConnectionKepper) IdleConnectionCount() int {
	r.m.Lock()
	defer r.m.Unlock()

	return int(r.IdleConnCount)
}

func (r *CoordConnectionKepper) QueueResidualSize() int {
	r.m.Lock()
	defer r.m.Unlock()

	return int(r.QueueSize)
}

func (r *CoordConnectionKepper) Hostname() string {
	r.m.Lock()
	defer r.m.Unlock()

	return r.Host
}

func (r *CoordConnectionKepper) List() []shard.Shard {
	return nil
}

func (r *CoordConnectionKepper) Rule() *config.BackendRule {
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
