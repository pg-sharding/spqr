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

func (r *ConnectionKepperData) Put(host shard.Shard) error {
	return spqrerror.New(spqrerror.SPQR_NOT_IMPLEMENTED, "ConnectionKepperData Put not implemented")
}

func (r *ConnectionKepperData) Discard(sh shard.Shard) error {
	return spqrerror.New(spqrerror.SPQR_NOT_IMPLEMENTED, "ConnectionKepperData Discard not implemented")
}

func (r *ConnectionKepperData) UsedConnectionCount() int {
	r.m.Lock()
	defer r.m.Unlock()

	return int(r.ConnCount)
}

func (r *ConnectionKepperData) IdleConnectionCount() int {
	r.m.Lock()
	defer r.m.Unlock()

	return int(r.IdleConnCount)
}

func (r *ConnectionKepperData) QueueResidualSize() int {
	r.m.Lock()
	defer r.m.Unlock()

	return int(r.QueueSize)
}

func (r *ConnectionKepperData) Hostname() string {
	r.m.Lock()
	defer r.m.Unlock()

	return r.Host
}

func (r *ConnectionKepperData) RouterName() string {
	r.m.Lock()
	defer r.m.Unlock()
	return r.Router
}

func (r *ConnectionKepperData) List() []shard.Shard {
	return nil
}

func (r *ConnectionKepperData) Rule() *config.BackendRule {
	r.m.Lock()
	defer r.m.Unlock()

	return &config.BackendRule{
		DB:  r.DB,
		Usr: r.Usr,
	}
}
