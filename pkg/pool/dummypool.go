package pool

import (
	"sync"

	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/pkg/models/spqrerror"
	"github.com/pg-sharding/spqr/pkg/config"
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

// UsedConnectionCount returns the number of used connections in the ConnectionKeeperData.
// It returns the number of used connections in the ConnectionKeeperData.
//
// Returns:
//   - int: The number of used connections in the ConnectionKeeperData.
func (r *DummyPool) UsedConnectionCount() int {
	r.m.Lock()
	defer r.m.Unlock()

	return int(r.ConnCount)
}

// IdleConnectionCount returns the number of idle connections in the connection keeper.
// It returns the number of idle connections in the ConnectionKeeperData.
//
// Returns:
//   - int: The number of idle connections in the ConnectionKeeperData.
func (r *DummyPool) IdleConnectionCount() int {
	r.m.Lock()
	defer r.m.Unlock()

	return int(r.IdleConnCount)
}

// QueueResidualSize returns the residual size of the queue.
// It returns the number of elements in the queue.
//
// Returns:
//   - int: The number of elements in the queue.
func (r *DummyPool) QueueResidualSize() int {
	r.m.Lock()
	defer r.m.Unlock()

	return int(r.QueueSize)
}

// Hostname returns the hostname associated with the DummyPool.
//
// Returns:
//   - string: The hostname associated with the DummyPool.
func (r *DummyPool) Hostname() string {
	r.m.Lock()
	defer r.m.Unlock()

	return r.Host
}

// RouterName returns the name of the router associated with the ConnectionKeeperData.
//
// Returns:
//   - string: The name of the router associated with the DummyPool.
func (r *DummyPool) RouterName() string {
	r.m.Lock()
	defer r.m.Unlock()
	return r.Router
}

// Rule returns a new instance of BackendRule based on the current DummyPool.
// It copies the DB and Usr fields from the DummyPool and returns the new BackendRule.
// The returned BackendRule is not a reference to the original DummyPool fields.
// It is a new instance with the same values.
//
// Returns:
//   - *config.BackendRule: The BackendRule created from the DummyPool.
func (r *DummyPool) Rule() *config.BackendRule {
	r.m.Lock()
	defer r.m.Unlock()

	return &config.BackendRule{
		DB:  r.DB,
		Usr: r.Usr,
	}
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
