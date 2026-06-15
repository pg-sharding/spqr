package pool

import (
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
	DiscardCount  int64
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
		DiscardCount:  0,
	}
}

func (r *PoolView) Put(_ shard.ShardHostInstance) error {
	return spqrerror.New(spqrerror.SPQR_NOT_IMPLEMENTED, "PoolView.Put not implemented")
}

func (r *PoolView) Discard(_ shard.ShardHostInstance) error {
	return spqrerror.New(spqrerror.SPQR_NOT_IMPLEMENTED, "PoolView.Discard not implemented")
}

func (r *PoolView) Connection(_ uint, _ kr.ShardKey) (shard.ShardHostInstance, error) {
	return nil, spqrerror.New(spqrerror.SPQR_NOT_IMPLEMENTED, "PoolView.Connection method not implemented")
}

func (r *PoolView) ForEach(_ func(p shard.ShardHostCtl) error) error {
	return spqrerror.New(spqrerror.SPQR_NOT_IMPLEMENTED, "PoolView.ForEach method not implemented")
}

func (r *PoolView) View() Statistics {

	return Statistics{
		DB:                r.DB,
		Usr:               r.Usr,
		Hostname:          r.Host,
		RouterName:        r.Router,
		UsedConnections:   r.ConnCount,
		IdleConnections:   r.IdleConnCount,
		QueueResidualSize: r.QueueSize,
		DiscardCount:      r.DiscardCount,
	}
}
