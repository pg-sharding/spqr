package qrouter

import (
	"context"
	"github.com/blastrain/vitess-sqlparser/sqlparser"
	"github.com/juju/errors"
	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/models/datashards"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/wal-g/tracelog"
)

type LocalQrouter struct {
	QueryRouter
	ds *datashards.DataShard
}

var _ QueryRouter = &LocalQrouter{}

func NewLocalQrouter(rules config.RulesCfg) (*LocalQrouter, error) {
	if len(rules.ShardMapping) != 1 {
		errmsg := "local router support only single-datashard routing"
		tracelog.ErrorLogger.Printf(errmsg)
		return nil, errors.New(errmsg)
	}

	l := &LocalQrouter{}

	var name string
	var cfg *config.ShardCfg

	for k, v := range rules.ShardMapping {
		name = k
		cfg = v
	}

	l.ds = &datashards.DataShard{
		ID:  name,
		Cfg: cfg,
	}

	return l, nil
}

func (qr *LocalQrouter) AddDataShard(ctx context.Context, ds *datashards.DataShard) error {
	tracelog.InfoLogger.Printf("adding node %s", ds.ID)
	qr.ds = ds
	return nil
}

func (l *LocalQrouter) IsRouterCommand(statement sqlparser.Statement) bool {
	return false
}

func (l *LocalQrouter) Route(q string) (RoutingState, error) {
	//
	//parsedStmt, err := sqlparser.Parse(q)
	//if err != nil {
	//	return nil, ParseError
	//}
	//
	//if l.IsRouterCommand(parsedStmt) {
	//	return SkipRoutingState{}, nil
	//}

	return ShardMatchState{
		Routes: []*ShardRoute{
			{
				Shkey: kr.ShardKey{
					Name: l.ds.ID,
				},
			},
		},
	}, nil
}
