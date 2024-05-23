package pool

import (
	"sync"

	"github.com/pg-sharding/spqr/pkg/models/spqrerror"

	"github.com/pg-sharding/spqr/pkg/config"
	protos "github.com/pg-sharding/spqr/pkg/protos"
	"github.com/pg-sharding/spqr/pkg/shard"
)

type ConnectionKepperData struct {
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

var _ ConnectionKepper = &ConnectionKepperData{}

// NewConnectionKepperData creates a new instance of ConnectionKepperData based on the provided PoolInfo.
// It initializes the fields of ConnectionKepperData with the corresponding values from the PoolInfo.
//
// Parameters:
//   - info: The PoolInfo instance to create the ConnectionKepperData from.
//
// Returns:
//   - *ConnectionKepperData: The created ConnectionKepperData.
func NewConnectionKepperData(info *protos.PoolInfo) *ConnectionKepperData {
	return &ConnectionKepperData{
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

// Put adds a host to the ConnectionKepperData.
// It returns an error if the operation is not implemented.
//
// Parameters:
//   - host: The shard to add to the ConnectionKepperData.
//
// Returns:
//   - error: An error if the operation is not implemented.
func (r *ConnectionKepperData) Put(host shard.Shard) error {
	return spqrerror.New(spqrerror.SPQR_NOT_IMPLEMENTED, "ConnectionKepperData Put not implemented")
}

// Discard discards the given shard from the connection keeper.
// It returns an error if the operation is not implemented.
//
// Parameters:
//   - sh: The shard to discard from the ConnectionKepperData.
//
// Returns:
//   - error: An error if the operation is not implemented.
func (r *ConnectionKepperData) Discard(sh shard.Shard) error {
	return spqrerror.New(spqrerror.SPQR_NOT_IMPLEMENTED, "ConnectionKepperData Discard not implemented")
}

// UsedConnectionCount returns the number of used connections in the ConnectionKeeperData.
// It returns the number of used connections in the ConnectionKeeperData.
//
// Returns:
//   - int: The number of used connections in the ConnectionKeeperData.
func (r *ConnectionKepperData) UsedConnectionCount() int {
	r.m.Lock()
	defer r.m.Unlock()

	return int(r.ConnCount)
}

// IdleConnectionCount returns the number of idle connections in the connection keeper.
// It returns the number of idle connections in the ConnectionKeeperData.
//
// Returns:
//   - int: The number of idle connections in the ConnectionKeeperData.
func (r *ConnectionKepperData) IdleConnectionCount() int {
	r.m.Lock()
	defer r.m.Unlock()

	return int(r.IdleConnCount)
}

// QueueResidualSize returns the residual size of the queue.
// It returns the number of elements in the queue.
//
// Returns:
//   - int: The number of elements in the queue.
func (r *ConnectionKepperData) QueueResidualSize() int {
	r.m.Lock()
	defer r.m.Unlock()

	return int(r.QueueSize)
}

// Hostname returns the hostname associated with the ConnectionKepperData.
//
// Returns:
//   - string: The hostname associated with the ConnectionKepperData.
func (r *ConnectionKepperData) Hostname() string {
	r.m.Lock()
	defer r.m.Unlock()

	return r.Host
}

// RouterName returns the name of the router associated with the ConnectionKeeperData.
//
// Returns:
//   - string: The name of the router associated with the ConnectionKepperData.
func (r *ConnectionKepperData) RouterName() string {
	r.m.Lock()
	defer r.m.Unlock()
	return r.Router
}

// List returns a slice of shard.Shard objects.
// It is used to retrieve all the shards stored in the connection keeper.
// Returns an empty slice if no shards are found.
//
// Returns:
//   - []shard.Shard: The list of shards stored in the ConnectionKepperData.
func (r *ConnectionKepperData) List() []shard.Shard {
	return nil
}

// Rule returns a new instance of BackendRule based on the current ConnectionKepperData.
// It copies the DB and Usr fields from the ConnectionKepperData and returns the new BackendRule.
// The returned BackendRule is not a reference to the original ConnectionKepperData fields.
// It is a new instance with the same values.
//
// Returns:
//   - *config.BackendRule: The BackendRule created from the ConnectionKepperData.
func (r *ConnectionKepperData) Rule() *config.BackendRule {
	r.m.Lock()
	defer r.m.Unlock()

	return &config.BackendRule{
		DB:  r.DB,
		Usr: r.Usr,
	}
}
