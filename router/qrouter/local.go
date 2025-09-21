package qrouter

import (
	"context"
	"fmt"

	"sync/atomic"

	"github.com/pg-sharding/lyx/lyx"
	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/meta"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/pkg/models/topology"
	"github.com/pg-sharding/spqr/pkg/session"
	"github.com/pg-sharding/spqr/pkg/spqrlog"
	"github.com/pg-sharding/spqr/router/cache"
	"github.com/pg-sharding/spqr/router/plan"
)

type LocalQrouter struct {
	ds    *topology.DataShard
	ready *atomic.Bool
}

func (qr *LocalQrouter) Ready() bool {
	return qr.ready.Load()
}

func (qr *LocalQrouter) SetReady(ready bool) {
	qr.ready.Store(ready)
}

// WorldShardsRoutes implements QueryRouter.
func (l *LocalQrouter) WorldShardsRoutes() []kr.ShardKey {
	return []kr.ShardKey{}
}

var _ QueryRouter = &LocalQrouter{}

func NewLocalQrouter(shardMapping map[string]*config.Shard) (*LocalQrouter, error) {
	if len(shardMapping) != 1 {
		errmsg := "local router support only single-datashard routing"
		err := fmt.Errorf(errmsg)
		spqrlog.Zero.Error().Err(err).Msg("")
		return nil, err
	}

	l := &LocalQrouter{
		ready: &atomic.Bool{},
	}

	var name string
	var cfg *config.Shard

	for k, v := range shardMapping {
		name = k
		cfg = v
	}

	l.ds = &topology.DataShard{
		ID:  name,
		Cfg: cfg,
	}

	return l, nil
}

func (l *LocalQrouter) Initialize() bool {
	return true
}

func (l *LocalQrouter) Initialized() bool {
	return true
}

// TODO : unit tests
func (l *LocalQrouter) AddDataShard(_ context.Context, ds *topology.DataShard) error {
	spqrlog.Zero.Debug().Str("shard", ds.ID).Msg("adding data shard")
	l.ds = ds
	return nil
}

// TODO : unit tests
func (l *LocalQrouter) PlanQuery(_ context.Context, _ lyx.Node, _ session.SessionParamsHolder) (plan.Plan, error) {
	return &plan.ShardDispatchPlan{
		ExecTarget: kr.ShardKey{
			Name: l.ds.ID,
		},
	}, nil
}

func (l *LocalQrouter) SchemaCache() *cache.SchemaCache {
	return nil
}

// TODO : unit tests
func (l *LocalQrouter) DataShardsRoutes() []kr.ShardKey {
	return []kr.ShardKey{
		{
			Name: l.ds.ID,
			RO:   false,
		},
	}
}

func (l *LocalQrouter) Mgr() meta.EntityMgr {
	return nil
}

func (l *LocalQrouter) SetQuery(_ *string) {}
func (l *LocalQrouter) Query() *string     { return nil }
