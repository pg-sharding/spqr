package provider

import (
	"fmt"

	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/pkg/pool"
	protos "github.com/pg-sharding/spqr/pkg/protos"
	"github.com/pg-sharding/spqr/pkg/shard"
)

type CoordPool struct {
	pool.ConnectionKepperData
}

func NewCoordPool(info *protos.PoolInfo) *CoordPool {
	return &CoordPool{
		ConnectionKepperData: *pool.NewConnectionKepperData(info),
	}
}

// TODO : unit tests
// TODO : implement
func (r *CoordPool) Connection(clid uint, shardKey kr.ShardKey) (shard.Shard, error) {
	return nil, fmt.Errorf("unimplemented")
}

// TODO : unit tests
// TODO : implement
func (r *CoordPool) ForEach(cb func(p shard.Shardinfo) error) error {
	return fmt.Errorf("unimplemented")
}
