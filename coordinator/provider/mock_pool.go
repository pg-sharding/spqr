package provider

import (
	"fmt"

	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	protos "github.com/pg-sharding/spqr/pkg/protos"
	"github.com/pg-sharding/spqr/pkg/shard"
)

type CoordMockPool struct {
	CoordMockConnectionKepper
	CoordMockPoolIterator
}

func NewMockPool(info *protos.PoolInfo) *CoordMockPool {
	return &CoordMockPool{
		CoordMockConnectionKepper: CoordMockConnectionKepper{PoolInfo: *info},
		CoordMockPoolIterator:     CoordMockPoolIterator{},
	}
}

func (r *CoordMockConnectionKepper) Put(host shard.Shard) error {
	return fmt.Errorf("mock")
}

type CoordMockConnectionKepper struct {
	protos.PoolInfo
}

func (r *CoordMockPool) Connection(clid string, shardKey kr.ShardKey) (shard.Shard, error) {
	return nil, fmt.Errorf("mock")
}

func (r *CoordMockConnectionKepper) Discard(sh shard.Shard) error {
	return fmt.Errorf("mock")
}

func (r *CoordMockConnectionKepper) UsedConnectionCount() int {
	return int(r.ConnCount)
}

func (r *CoordMockConnectionKepper) IdleConnectionCount() int {
	return int(r.IdleConnCount)
}

func (r *CoordMockConnectionKepper) QueueResidualSize() int {
	return int(r.QueueSize)
}

func (r *CoordMockConnectionKepper) Hostname() string {
	return r.Host
}

func (r *CoordMockConnectionKepper) List() []shard.Shard {
	return nil
}

func (r *CoordMockConnectionKepper) Rule() *config.BackendRule {
	return &config.BackendRule{
		DB:  r.DB,
		Usr: r.Usr,
	}
}

type CoordMockPoolIterator struct {
}

func (r *CoordMockPoolIterator) ForEach(cb func(p shard.Shardinfo) error) error {
	return fmt.Errorf("mock")
}
