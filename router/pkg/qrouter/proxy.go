package qrouter

import (
	"context"
	"math/rand"

	"github.com/blastrain/vitess-sqlparser/sqlparser"
	"github.com/wal-g/tracelog"
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
	ColumnMapping map[string]struct{}
	LocalTables   map[string]struct{}

	// shards
	DataShardCfgs  map[string]*config.ShardCfg
	WorldShardCfgs map[string]*config.ShardCfg

	qdb qdb.QrouterDB
}

func (qr *ProxyQrouter) ListDataShards(ctx context.Context) []*datashards.DataShard {
	var ret []*datashards.DataShard
	for id, cfg := range qr.DataShardCfgs {
		ret = append(ret, datashards.NewDataShard(id, cfg))
	}
	return ret
}

func (qr *ProxyQrouter) AddWorldShard(name string, cfg *config.ShardCfg) error {

	tracelog.InfoLogger.Printf("adding world datashard %s", name)
	qr.WorldShardCfgs[name] = cfg

	return nil
}

func (qr *ProxyQrouter) DataShardsRoutes() []*ShardRoute {
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
	var ret []string

	for name := range qr.WorldShardCfgs {
		ret = append(ret, name)
	}

	return ret
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

func (qr *ProxyQrouter) Subscribe(krid string, krst *qdb.KeyRangeStatus, noitfyio chan<- interface{}) error {
	//return qr.qdb.Watch(krid, krst, noitfyio)
	return nil
}

func (qr *ProxyQrouter) Unite(ctx context.Context, req *kr.UniteKeyRange) error {
	var krRight *qdb.KeyRange
	var krleft *qdb.KeyRange
	var err error

	if krleft, err = qr.qdb.Lock(ctx, req.KeyRangeIDLeft); err != nil {
		return err
	}
	defer func(qdb qdb.QrouterDB, ctx context.Context, keyRangeID string) {
		err := qdb.UnLock(ctx, keyRangeID)
		if err != nil {
			tracelog.ErrorLogger.PrintError(err)
		}
	}(qr.qdb, ctx, req.KeyRangeIDLeft)

	// TODO: krRight seems to be empty.
	if krleft, err = qr.qdb.Lock(ctx, req.KeyRangeIDRight); err != nil {
		return err
	}
	defer func(qdb qdb.QrouterDB, ctx context.Context, keyRangeID string) {
		err := qdb.UnLock(ctx, keyRangeID)
		if err != nil {
			tracelog.ErrorLogger.PrintError(err)
		}
	}(qr.qdb, ctx, req.KeyRangeIDRight)

	if err = qr.qdb.DropKeyRange(ctx, krleft); err != nil {
		return err
	}

	krRight.LowerBound = krleft.LowerBound

	return qr.qdb.UpdateKeyRange(ctx, krRight)
}

func (qr *ProxyQrouter) Split(ctx context.Context, req *kr.SplitKeyRange) error {
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
	tracelog.InfoLogger.Printf("adding node %s", ds.ID)
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

func (qr *ProxyQrouter) ListKeyRange(ctx context.Context) ([]*kr.KeyRange, error) {
	var ret []*kr.KeyRange
	if krs, err := qr.qdb.ListKeyRange(ctx); err != nil {
		return nil, err
	} else {
		for _, keyRange := range krs {
			ret = append(ret, kr.KeyRangeFromDB(keyRange))
		}
	}

	return ret, nil
}

func (qr *ProxyQrouter) AddShardingRule(ctx context.Context, rule *shrule.ShardingRule) error {
	if len(rule.Columns()) != 1 {
		return xerrors.New("only single column sharding rules are supported for now")
	}

	qr.ColumnMapping[rule.Columns()[0]] = struct{}{}
	return nil
}

func (qr *ProxyQrouter) ListShardingRules(_ context.Context) ([]*shrule.ShardingRule, error) {
	rules := make([]*shrule.ShardingRule, 0, len(qr.ColumnMapping))

	for rule := range qr.ColumnMapping {
		rules = append(rules, shrule.NewShardingRule([]string{rule}))
	}

	return rules, nil
}

func (qr *ProxyQrouter) AddLocalTable(tname string) error {
	qr.LocalTables[tname] = struct{}{}
	return nil
}

func (qr *ProxyQrouter) AddKeyRange(ctx context.Context, kr *kr.KeyRange) error {
	return qr.qdb.AddKeyRange(ctx, kr.ToSQL())
}

func (qr *ProxyQrouter) MoveKeyRange(ctx context.Context, kr *kr.KeyRange) error {
	return qr.qdb.UpdateKeyRange(ctx, kr.ToSQL())
}

func (qr *ProxyQrouter) routeByIndx(i []byte) *kr.KeyRange {

	krs, _ := qr.qdb.ListKeyRange(context.TODO())

	for _, keyRange := range krs {
		tracelog.InfoLogger.Printf("comparing %v with key range %v %v", i, keyRange.LowerBound, keyRange.UpperBound)
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
			tracelog.InfoLogger.Printf("go right")
			shindx := qr.routeByExpr(texpr.Right)
			return shindx
		}
		tracelog.InfoLogger.Printf("go left")
		return qr.routeByExpr(texpr.Left)
	case *sqlparser.SQLVal:

		tracelog.InfoLogger.Printf("parsed val %d", texpr.Val)
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

	tracelog.InfoLogger.Printf("parsed qtype %T", qstmt)

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
		tracelog.InfoLogger.Printf("ddl routing expands to every datashard")
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

func (qr *ProxyQrouter) Route(parser parser.QParser) (RoutingState, error) {
	parsedStmt := parser.Stmt()
	tracelog.InfoLogger.Printf("stmt type %T", parsedStmt)

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
