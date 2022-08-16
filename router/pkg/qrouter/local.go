package qrouter

import (
	"context"

	"github.com/jackc/pgproto3/v2"
	"github.com/juju/errors"
	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/models/datashards"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/pkg/spqrlog"
	"github.com/pg-sharding/spqr/router/pkg/parser"
)

type LocalQrouter struct {
	QueryRouter
	ds     *datashards.DataShard
	parser parser.QParser
}

var _ QueryRouter = &LocalQrouter{}

func NewLocalQrouter(shardMapping map[string]*config.Shard) (*LocalQrouter, error) {
	if len(shardMapping) != 1 {
		errmsg := "local router support only single-datashard routing"
		err := errors.New(errmsg)
		spqrlog.Logger.PrintError(err)
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

func (l *LocalQrouter) AddDataShard(_ context.Context, ds *datashards.DataShard) error {
	spqrlog.Logger.Printf(spqrlog.DEBUG5, "adding node %s", ds.ID)
	l.ds = ds
	return nil
}

func (l *LocalQrouter) Route(_ context.Context) (RoutingState, error) {
	return ShardMatchState{
		Routes: []*DataShardRoute{
			{
				Shkey: kr.ShardKey{
					Name: l.ds.ID,
				},
			},
		},
	}, nil
}

func (l *LocalQrouter) Parse(q *pgproto3.Query) (parser.ParseState, error) {
	if stmt, err := l.parser.Parse(q); err == nil {
		return stmt, nil
	}
	return parser.ParseStateQuery{}, nil
}
