package pool

import (
	"sync"

	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/pkg/models/spqrerror"
	protos "github.com/pg-sharding/spqr/pkg/protos"
	"github.com/pg-sharding/spqr/pkg/shard"
)

type PoolView struct {
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

var _ Pool = &PoolView{}

// NewPoolView creates a new instance of PoolView based on the provided PoolInfo.
// The PoolView is a dummy implementation of the Pool interface.
func NewPoolView(info *protos.PoolInfo) *PoolView {
	return &PoolView{
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

func (r *PoolView) Put(host shard.ShardHostInstance) error {
	return spqrerror.New(spqrerror.SPQR_NOT_IMPLEMENTED, "PoolView.Put not implemented")
}

func (r *PoolView) Discard(sh shard.ShardHostInstance) error {
	return spqrerror.New(spqrerror.SPQR_NOT_IMPLEMENTED, "PoolView.Discard not implemented")
}

func (r *PoolView) Connection(clid uint, shardKey kr.ShardKey) (shard.ShardHostInstance, error) {
	return nil, spqrerror.New(spqrerror.SPQR_NOT_IMPLEMENTED, "PoolView.Connection method not implemented")
}

func (r *PoolView) ForEach(cb func(p shard.ShardHostCtl) error) error {
	return spqrerror.New(spqrerror.SPQR_NOT_IMPLEMENTED, "PoolView.ForEach method not implemented")
}

func (r *PoolView) View() Statistics {
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
