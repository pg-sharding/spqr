package qrouter

import (
	"context"
	"fmt"
	"github.com/jackc/pgproto3/v2"
	"github.com/pg-sharding/spqr/pkg/spqrlog"
	"math/rand"
	"sync"

	"github.com/blastrain/vitess-sqlparser/sqlparser"

	"golang.org/x/xerrors"

	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/models/datashards"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/pkg/models/shrule"
	"github.com/pg-sharding/spqr/qdb"
	"github.com/pg-sharding/spqr/qdb/mem"
	"github.com/pg-sharding/spqr/router/pkg/parser"
)

type ProxyQrouter struct {
	QueryRouter
	mu sync.Mutex

	Rules []*shrule.ShardingRule

	ColumnMapping map[string]struct{}
	LocalTables   map[string]struct{}

	// shards
	DataShardCfgs  map[string]*config.ShardCfg
	WorldShardCfgs map[string]*config.ShardCfg

	qdb qdb.QrouterDB

	parser parser.QParser
}

func (qr *ProxyQrouter) ListDataShards(ctx context.Context) []*datashards.DataShard {
	qr.mu.Lock()
	qr.mu.Unlock()

	var ret []*datashards.DataShard
	for id, cfg := range qr.DataShardCfgs {
		ret = append(ret, datashards.NewDataShard(id, cfg))
	}
	return ret
}

func (qr *ProxyQrouter) ListShardingRules(ctx context.Context) ([]*shrule.ShardingRule, error) {
	qr.mu.Lock()
	qr.mu.Unlock()
	return qr.Rules, nil
}

func (qr *ProxyQrouter) AddWorldShard(name string, cfg *config.ShardCfg) error {
	qr.mu.Lock()
	qr.mu.Unlock()

	spqrlog.Logger.Printf(spqrlog.LOG, "adding world datashard %s", name)
	qr.WorldShardCfgs[name] = cfg

	return nil
}

func (qr *ProxyQrouter) DataShardsRoutes() []*ShardRoute {
	qr.mu.Lock()
	qr.mu.Unlock()

	var ret []*ShardRoute

	for name := range qr.DataShardCfgs {
		ret = append(ret, &ShardRoute{
			Shkey: kr.ShardKey{
				Name: name,
				RW:   true,
			},
		})
	}

	return ret
}

func (qr *ProxyQrouter) WorldShardsRoutes() []*ShardRoute {
	qr.mu.Lock()
	qr.mu.Unlock()

	var ret []*ShardRoute

	for name := range qr.WorldShardCfgs {
		ret = append(ret, &ShardRoute{
			Shkey: kr.ShardKey{
				Name: name,
				RW:   true,
			},
		})
	}

	// a sort of round robin

	rand.Shuffle(len(ret), func(i, j int) {
		ret[i], ret[j] = ret[j], ret[i]
	})
	return ret
}

func (qr *ProxyQrouter) WorldShards() []string {
	qr.mu.Lock()
	qr.mu.Unlock()

	panic("implement me")
}

var _ QueryRouter = &ProxyQrouter{}

func NewProxyRouter(rules config.RulesCfg) (*ProxyQrouter, error) {
	db, err := mem.NewQrouterDBMem()
	if err != nil {
		return nil, err
	}

	proxy := &ProxyQrouter{
		ColumnMapping:  map[string]struct{}{},
		DataShardCfgs:  map[string]*config.ShardCfg{},
		WorldShardCfgs: map[string]*config.ShardCfg{},
		qdb:            db,
		Rules:          []*shrule.ShardingRule{},
	}

	for name, shardCfg := range rules.ShardMapping {
		switch shardCfg.ShType {
		case config.WorldShard:
		case config.DataShard:
			fallthrough // default is datashard
		default:
			if err := proxy.AddDataShard(context.TODO(), &datashards.DataShard{
				ID:  name,
				Cfg: shardCfg,
			}); err != nil {
				return nil, err
			}
		}
	}
	return proxy, nil
}

func (qr *ProxyQrouter) Parse(q *pgproto3.Query) (parser.ParseState, error) {
	return qr.parser.Parse(q)
}

func (qr *ProxyQrouter) Unite(ctx context.Context, req *kr.UniteKeyRange) error {
	qr.mu.Lock()
	qr.mu.Unlock()

	var krRight *qdb.KeyRange
	var krleft *qdb.KeyRange
	var err error

	if krleft, err = qr.qdb.Lock(ctx, req.KeyRangeIDLeft); err != nil {
		return err
	}
	defer func(qdb qdb.QrouterDB, ctx context.Context, keyRangeID string) {
		err := qdb.UnLock(ctx, keyRangeID)
		if err != nil {
			spqrlog.Logger.PrintError(err)
			return
		}
	}(qr.qdb, ctx, req.KeyRangeIDLeft)

	if krleft, err = qr.qdb.Lock(ctx, req.KeyRangeIDRight); err != nil {
		return err
	}
	defer func(qdb qdb.QrouterDB, ctx context.Context, keyRangeID string) {
		err := qdb.UnLock(ctx, keyRangeID)
		if err != nil {
			spqrlog.Logger.PrintError(err)
			return
		}
	}(qr.qdb, ctx, req.KeyRangeIDRight)

	if err = qr.qdb.DropKeyRange(ctx, krleft); err != nil {
		return err
	}

	krRight.LowerBound = krleft.LowerBound

	return qr.qdb.UpdateKeyRange(ctx, krRight)
}

func (qr *ProxyQrouter) Split(ctx context.Context, req *kr.SplitKeyRange) error {
	qr.mu.Lock()
	qr.mu.Unlock()

	var krOld *qdb.KeyRange
	var err error

	if krOld, err = qr.qdb.Lock(ctx, req.SourceID); err != nil {
		return err
	}
	defer qr.qdb.UnLock(ctx, req.SourceID)

	krNew := kr.KeyRangeFromDB(
		&qdb.KeyRange{
			LowerBound: req.Bound,
			UpperBound: krOld.UpperBound,
			KeyRangeID: req.SourceID,
		},
	)

	_ = qr.qdb.AddKeyRange(ctx, krNew.ToSQL())
	krOld.UpperBound = req.Bound
	_ = qr.qdb.UpdateKeyRange(ctx, krOld)

	return nil
}

func (qr *ProxyQrouter) Lock(ctx context.Context, krid string) (*kr.KeyRange, error) {
	qr.mu.Lock()
	qr.mu.Unlock()

	keyRangeDB, err := qr.qdb.Lock(ctx, krid)
	if err != nil {
		return nil, err
	}

	return kr.KeyRangeFromDB(keyRangeDB), nil
}

func (qr *ProxyQrouter) Unlock(ctx context.Context, krid string) error {
	return qr.qdb.UnLock(ctx, krid)
}

func (qr *ProxyQrouter) AddDataShard(ctx context.Context, ds *datashards.DataShard) error {
	spqrlog.Logger.Printf(spqrlog.LOG, "adding node %s", ds.ID)
	qr.DataShardCfgs[ds.ID] = ds.Cfg
	return nil
}

func (qr *ProxyQrouter) Shards() []string {
	var ret []string

	for name := range qr.DataShardCfgs {
		ret = append(ret, name)
	}

	return ret
}

func (qr *ProxyQrouter) ListKeyRanges(ctx context.Context) ([]*kr.KeyRange, error) {
	qr.mu.Lock()
	qr.mu.Unlock()

	var ret []*kr.KeyRange

	if krs, err := qr.qdb.ListKeyRanges(ctx); err != nil {
		return nil, err
	} else {
		for _, keyRange := range krs {
			ret = append(ret, kr.KeyRangeFromDB(keyRange))
		}
	}

	return ret, nil
}

func (qr *ProxyQrouter) AddShardingRule(ctx context.Context, rule *shrule.ShardingRule) error {
	qr.mu.Lock()
	qr.mu.Unlock()

	if len(rule.Columns()) != 1 {
		return xerrors.New("only single column sharding rules are supported for now")
	}

	if _, ok := qr.ColumnMapping[rule.Columns()[0]]; ok {
		return fmt.Errorf("sharding column already exist")
	}

	qr.ColumnMapping[rule.Columns()[0]] = struct{}{}
	return nil
}

func (qr *ProxyQrouter) AddKeyRange(ctx context.Context, kr *kr.KeyRange) error {
	return qr.qdb.AddKeyRange(ctx, kr.ToSQL())
}

func (qr *ProxyQrouter) routeByIndx(i []byte) *kr.KeyRange {
	krs, _ := qr.qdb.ListKeyRanges(context.TODO())

	for _, keyRange := range krs {
		spqrlog.Logger.Printf(spqrlog.DEBUG2, "comparing %v with key range %v %v", i, keyRange.LowerBound, keyRange.UpperBound)
		if kr.CmpRanges(keyRange.LowerBound, i) && kr.CmpRanges(i, keyRange.UpperBound) {
			return kr.KeyRangeFromDB(keyRange)
		}
	}

	return &kr.KeyRange{
		ShardID: NOSHARD,
	}
}

func (qr *ProxyQrouter) matchShkey(expr sqlparser.Expr) bool {
	switch texpr := expr.(type) {
	case sqlparser.ValTuple:
		for _, val := range texpr {
			if qr.matchShkey(val) {
				return true
			}
		}
	case *sqlparser.ColName:
		_, ok := qr.ColumnMapping[texpr.Name.String()]
		return ok
	default:
	}

	return false
}

func (qr *ProxyQrouter) routeByExpr(expr sqlparser.Expr) *ShardRoute {

	switch texpr := expr.(type) {
	case *sqlparser.AndExpr:
		lft := qr.routeByExpr(texpr.Left)
		if lft.Shkey.Name == NOSHARD {
			return qr.routeByExpr(texpr.Right)
		}
		return lft
	case *sqlparser.ComparisonExpr:

		if qr.matchShkey(texpr.Left) {
			shindx := qr.routeByExpr(texpr.Right)
			return shindx
		}
		return qr.routeByExpr(texpr.Left)
	case *sqlparser.SQLVal:
		keyRange := qr.routeByIndx(texpr.Val)
		//rw := qr.qdb.Check(keyRange.ToSQL())

		return &ShardRoute{
			Shkey: kr.ShardKey{
				Name: keyRange.ShardID,
				RW:   false,
			},
			Matchedkr: keyRange,
		}
	default:
	}

	return &ShardRoute{
		Shkey: kr.ShardKey{
			Name: NOSHARD,
		},
	}
}

func (qr *ProxyQrouter) isLocalTbl(from sqlparser.TableExprs) bool {
	for _, texpr := range from {
		switch tbltype := texpr.(type) {
		case *sqlparser.ParenTableExpr:
		case *sqlparser.JoinTableExpr:
		case *sqlparser.AliasedTableExpr:

			switch tname := tbltype.Expr.(type) {
			case sqlparser.TableName:
				if _, ok := qr.LocalTables[tname.Name.String()]; ok {
					return true
				}
			case *sqlparser.Subquery:
			default:
			}
		}
	}

	return false
}

func (qr *ProxyQrouter) matchShards(qstmt sqlparser.Statement) []*ShardRoute {
	switch stmt := qstmt.(type) {
	case *sqlparser.Select:
		if qr.isLocalTbl(stmt.From) {
			return nil
		}
		if stmt.Where != nil {
			shroute := qr.routeByExpr(stmt.Where.Expr)
			if shroute.Shkey.Name == NOSHARD {
				return nil
			}
			return []*ShardRoute{shroute}
		}
		return nil

	case *sqlparser.Insert:
		for i, c := range stmt.Columns {
			if _, ok := qr.ColumnMapping[c.String()]; ok {
				switch vals := stmt.Rows.(type) {
				case sqlparser.Values:
					valTyp := vals[0]
					shroute := qr.routeByExpr(valTyp[i])
					if shroute.Shkey.Name == NOSHARD {
						return nil
					}
					return []*ShardRoute{shroute}
				}
			}
		}
	case *sqlparser.Update:
		if stmt.Where != nil {
			shroute := qr.routeByExpr(stmt.Where.Expr)
			if shroute.Shkey.Name == NOSHARD {
				return nil
			}
			return []*ShardRoute{shroute}
		}
		return nil
	case *sqlparser.TruncateTable, *sqlparser.CreateTable:
		// route ddl to every datashard
		shrds := qr.Shards()
		var ret []*ShardRoute
		for _, sh := range shrds {
			ret = append(ret,
				&ShardRoute{
					Shkey: kr.ShardKey{
						Name: sh,
						RW:   true,
					},
				})
		}

		return ret
	}

	return nil
}

var ParseError = xerrors.New("parsing stmt error")

func (qr *ProxyQrouter) Route() (RoutingState, error) {
	parsedStmt := qr.parser.Stmt()
	switch parsedStmt.(type) {
	case *sqlparser.DDL:
		return ShardMatchState{
			Routes: qr.DataShardsRoutes(),
		}, nil
	default:
		routes := qr.matchShards(parsedStmt)

		if routes == nil {
			return SkipRoutingState{}, nil
		}

		return ShardMatchState{
			Routes: routes,
		}, nil
	}
}
