package pool

import (
	"sync"

	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/pkg/models/spqrerror"
	protos "github.com/pg-sharding/spqr/pkg/protos"
	"github.com/pg-sharding/spqr/pkg/shard"
)

type DummyPool struct {
	Id            string
	DB            string
	Usr           string
	Host          string
	Router        string
	ConnCount     int64
	IdleConnCount int64
	QueueSize     int64

	m sync.RWMutex
}

var _ Pool = &DummyPool{}

// NewDummyPool creates a new instance of DummyPool based on the provided PoolInfo.
// It initializes the fields of DummyPool with the corresponding values from the PoolInfo.
//
// Parameters:
//   - info: The PoolInfo instance to create the DummyPool from.
//
// Returns:
//   - *DummyPool: The created DummyPool.
func NewDummyPool(info *protos.PoolInfo) *DummyPool {
	return &DummyPool{
		Id:            info.Id,
		DB:            info.DB,
		Usr:           info.Usr,
		Router:        info.RouterName,
		Host:          info.Host,
		ConnCount:     info.ConnCount,
		IdleConnCount: info.IdleConnCount,
		QueueSize:     info.QueueSize,
		m:             sync.RWMutex{},
	}
}

// Put adds a host to the DummyPool.
// It returns an error if the operation is not implemented.
//
// Parameters:
//   - host: The shard to add to the DummyPool.
//
// Returns:
//   - error: An error if the operation is not implemented.
func (r *DummyPool) Put(host shard.Shard) error {
	return spqrerror.New(spqrerror.SPQR_NOT_IMPLEMENTED, "DummyPool.Put not implemented")
}

// Discard discards the given shard from the connection keeper.
// It returns an error if the operation is not implemented.
//
// Parameters:
//   - sh: The shard to discard from the DummyPool.
//
// Returns:
//   - error: An error if the operation is not implemented.
func (r *DummyPool) Discard(sh shard.Shard) error {
	return spqrerror.New(spqrerror.SPQR_NOT_IMPLEMENTED, "DummyPool.Discard not implemented")
}

// TODO : unit tests
// TODO : implement
func (r *DummyPool) Connection(clid uint, shardKey kr.ShardKey) (shard.Shard, error) {
	return nil, spqrerror.New(spqrerror.SPQR_NOT_IMPLEMENTED, "DummyPool.Connection method unimplemented")
}

// TODO : unit tests
// TODO : implement
func (r *DummyPool) ForEach(cb func(p shard.Shardinfo) error) error {
	return spqrerror.New(spqrerror.SPQR_NOT_IMPLEMENTED, "DummyPool.ForEach method unimplemented")
}

func (r *DummyPool) View() Statistics {
	r.m.Lock()
	defer r.m.Unlock()

	return Statistics{
		DB:                r.DB,
		Usr:               r.Usr,
		Hostname:          r.Host,
		RouterName:        r.Router,
		UsedConnections:   int(r.ConnCount),
		IdleConnections:   int(r.IdleConnCount),
		QueueResidualSize: int(r.QueueSize),
	}
}
