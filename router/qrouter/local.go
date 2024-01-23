package qrouter

import (
	"context"

	"github.com/juju/errors"
	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/models/datashards"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/pkg/session"
	"github.com/pg-sharding/spqr/pkg/spqrlog"
	"github.com/pg-sharding/spqr/router/routingstate"

	"github.com/pg-sharding/lyx/lyx"
)

type LocalQrouter struct {
	QueryRouter
	ds *datashards.DataShard
}

var _ QueryRouter = &LocalQrouter{}

func NewLocalQrouter(shardMapping map[string]*config.Shard) (*LocalQrouter, error) {
	if len(shardMapping) != 1 {
		errmsg := "local router support only single-datashard routing"
		err := errors.New(errmsg)
		spqrlog.Zero.Error().Err(err).Msg("")
		return nil, err
	}

	l := &LocalQrouter{}

	var name string
	var cfg *config.Shard

	for k, v := range shardMapping {
		name = k
		cfg = v
	}

	l.ds = &datashards.DataShard{
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
func (l *LocalQrouter) AddDataShard(_ context.Context, ds *datashards.DataShard) error {
	spqrlog.Zero.Debug().Str("shard", ds.ID).Msg("adding data shard")
	l.ds = ds
	return nil
}

// TODO : unit tests
func (l *LocalQrouter) Route(_ context.Context, _ lyx.Node, _ session.SessionParamsHolder) (routingstate.RoutingState, error) {
	return routingstate.ShardMatchState{
		Route: &routingstate.DataShardRoute{
			Shkey: kr.ShardKey{
				Name: l.ds.ID,
			},
		},
	}, nil
}

func (l *LocalQrouter) ListKeyRanges(ctx context.Context) ([]*kr.KeyRange, error) {
	return nil, nil
}
