package qrouter

import (
	"context"
	"fmt"
	"github.com/jackc/pgproto3/v2"
	"github.com/pg-sharding/spqr/pkg/spqrlog"
	pgquery "github.com/pganalyze/pg_query_go/v2"
	"math/rand"
	"sync"

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

func (qr *ProxyQrouter) DataShardsRoutes() []*DataShardRoute {
	qr.mu.Lock()
	defer qr.mu.Unlock()

	var ret []*DataShardRoute

	for name := range qr.DataShardCfgs {
		ret = append(ret, &DataShardRoute{
			Shkey: kr.ShardKey{
				Name: name,
				RW:   true,
			},
		})
	}

	return ret
}

func (qr *ProxyQrouter) WorldShardsRoutes() []*DataShardRoute {
	qr.mu.Lock()
	defer qr.mu.Unlock()

	var ret []*DataShardRoute

	for name := range qr.WorldShardCfgs {
		ret = append(ret, &DataShardRoute{
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
	defer qr.mu.Unlock()

	var krRight *qdb.KeyRange
	var krleft *qdb.KeyRange
	var err error

	if krleft, err = qr.qdb.Lock(ctx, req.KeyRangeIDLeft); err != nil {
		return err
	}
	defer func(qdb qdb.QrouterDB, ctx context.Context, keyRangeID string) {
		err := qdb.Unlock(ctx, keyRangeID)
		if err != nil {
			spqrlog.Logger.PrintError(err)
			return
		}
	}(qr.qdb, ctx, req.KeyRangeIDLeft)

	if krleft, err = qr.qdb.Lock(ctx, req.KeyRangeIDRight); err != nil {
		return err
	}
	defer func(qdb qdb.QrouterDB, ctx context.Context, keyRangeID string) {
		err := qdb.Unlock(ctx, keyRangeID)
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
	defer qr.mu.Unlock()

	var krOld *qdb.KeyRange
	var err error

	if krOld, err = qr.qdb.Lock(ctx, req.SourceID); err != nil {
		return err
	}
	defer func(qdb qdb.QrouterDB, ctx context.Context, krid string) {
		err := qdb.Unlock(ctx, krid)
		if err != nil {
			spqrlog.Logger.PrintError(err)
		}
	}(qr.qdb, ctx, req.SourceID)

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
	defer qr.mu.Unlock()

	keyRangeDB, err := qr.qdb.Lock(ctx, krid)
	if err != nil {
		return nil, err
	}

	return kr.KeyRangeFromDB(keyRangeDB), nil
}

func (qr *ProxyQrouter) Unlock(ctx context.Context, krid string) error {
	return qr.qdb.Unlock(ctx, krid)
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
	defer qr.mu.Unlock()

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

	return qr.qdb.AddShardingRule(ctx, rule)
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

var tooComplexQuery = fmt.Errorf("too complex query to parse")

func (qr *ProxyQrouter) DeparseExprCol(expr *pgquery.Node) ([]string, error) {
	var colnames []string

	spqrlog.Logger.Printf(spqrlog.DEBUG5, "deparsing column name %T", expr.Node)
	switch texpr := expr.Node.(type) {
	case *pgquery.Node_ColumnRef:
		for _, node := range texpr.ColumnRef.Fields {
			spqrlog.Logger.Printf(spqrlog.DEBUG4, "columnref field %T", node.Node)

			switch colname := node.Node.(type) {
			case *pgquery.Node_String_:
				colnames = append(colnames, colname.String_.Str)
			default:
				return nil, tooComplexQuery
			}
		}
	default:
		return nil, tooComplexQuery
	}

	if len(colnames) != 1 {
		return nil, tooComplexQuery
	}

	return colnames, nil
}

func (qr *ProxyQrouter) deparseKeyWithRangesInternal(ctx context.Context, key string) (*DataShardRoute, error) {
	spqrlog.Logger.Printf(spqrlog.DEBUG1, "checking key %s", key)

	krs, err := qr.qdb.ListKeyRanges(ctx)

	if err != nil {
		return nil, err
	}

	for _, krkey := range krs {
		if kr.CmpRanges(krkey.LowerBound, []byte(key)) && kr.CmpRanges([]byte(key), krkey.UpperBound) {
			if err := qr.qdb.Share(krkey); err != nil {
				return nil, err
			}

			return &DataShardRoute{
				Shkey: kr.ShardKey{Name: krkey.ShardID},
			}, nil
		}
	}

	return nil, tooComplexQuery
}

func getbytes(val *pgquery.Node) (string, error) {
	switch valt := val.Node.(type) {
	case *pgquery.Node_Integer:
		return fmt.Sprintf("%d", valt.Integer.Ival), nil
	case *pgquery.Node_String_:
		return valt.String_.Str, nil
	default:
		return "", tooComplexQuery
	}
}

func (qr *ProxyQrouter) DeparseKeyWithRanges(ctx context.Context, expr *pgquery.Node) (*DataShardRoute, error) {
	spqrlog.Logger.Printf(spqrlog.DEBUG5, "deparsing key ranges %T", expr.Node)

	switch texpr := expr.Node.(type) {
	case *pgquery.Node_AConst:
		val, err := getbytes(texpr.AConst.Val)
		if err != nil {
			return nil, err
		}
		return qr.deparseKeyWithRangesInternal(ctx, val)
	case *pgquery.Node_List:
		if len(texpr.List.Items) == 0 {
			return nil, tooComplexQuery
		}
		return qr.DeparseKeyWithRanges(ctx, texpr.List.Items[0])
	default:
		return nil, tooComplexQuery
	}
}

func (qr *ProxyQrouter) routeByExpr(ctx context.Context, expr *pgquery.Node) (*DataShardRoute, error) {

	spqrlog.Logger.Printf(spqrlog.DEBUG5, "deparsed stmt type %T", expr.Node)
	switch texpr := expr.Node.(type) {
	case *pgquery.Node_AExpr:
		if texpr.AExpr.Kind != pgquery.A_Expr_Kind_AEXPR_OP {
			return nil, tooComplexQuery
		}

		colnames, err := qr.DeparseExprCol(texpr.AExpr.Lexpr)
		if err != nil {
			return nil, err
		}

		spqrlog.Logger.Printf(spqrlog.DEBUG5, "deparsed columns references %+v", colnames)

		if !qr.qdb.CheckShardingRule(ctx, colnames) {
			return nil, tooComplexQuery
		}

		krs, err := qr.DeparseKeyWithRanges(ctx, texpr.AExpr.Rexpr)
		if err != nil {
			return nil, err
		}
		return krs, nil
	default:
		return nil, tooComplexQuery
	}
}

func (qr *ProxyQrouter) DeparseSelectStmt(ctx context.Context, node *pgquery.Node) (ShardRoute, error) {
	//val, err := getbytes(res.ResTarget.Val)

	switch q := node.Node.(type) {
	case *pgquery.Node_SelectStmt:
		if len(q.SelectStmt.ValuesLists) != 1 {
			return nil, tooComplexQuery
		}
		valNode := q.SelectStmt.ValuesLists[0]
		spqrlog.Logger.Printf(spqrlog.DEBUG5, "val node is %T", valNode.Node)
		return qr.DeparseKeyWithRanges(ctx, valNode)
	}

	return nil, tooComplexQuery
}

func (qr *ProxyQrouter) matchShards(ctx context.Context, qstmt *pgquery.RawStmt) (ShardRoute, error) {
	spqrlog.Logger.Printf(spqrlog.DEBUG5, "mathcing qstmt %T", qstmt.Stmt.Node)
	switch stmt := qstmt.Stmt.Node.(type) {
	case *pgquery.Node_SelectStmt:
		clause := stmt.SelectStmt.WhereClause
		if clause == nil {
			return &MultiMatchRoute{}, nil
		}
		shroute, err := qr.routeByExpr(ctx, clause)
		if err != nil {
			return nil, err
		}
		if shroute.Shkey.Name == NOSHARD {
			return nil, err
		}
		return shroute, nil
	case *pgquery.Node_InsertStmt:
		for _, c := range stmt.InsertStmt.Cols {
			spqrlog.Logger.Printf(spqrlog.DEBUG5, "col tp is %T", c.Node)
			switch res := c.Node.(type) {
			case *pgquery.Node_ResTarget:

				spqrlog.Logger.Printf(spqrlog.DEBUG1, "checking insert colname %v, %T", res.ResTarget.Name, stmt.InsertStmt.SelectStmt.Node)
				if !qr.qdb.CheckShardingRule(ctx, []string{res.ResTarget.Name}) {
					continue
				}

				return qr.DeparseSelectStmt(ctx, stmt.InsertStmt.SelectStmt)
			default:
				return nil, tooComplexQuery
			}
		}

	case *pgquery.Node_UpdateStmt:
		clause := stmt.UpdateStmt.WhereClause
		if clause == nil {
			return &MultiMatchRoute{}, nil
		}

		shroute, err := qr.routeByExpr(ctx, clause)
		if err != nil {
			return nil, err
		}
		if shroute.Shkey.Name == NOSHARD {
			return nil, tooComplexQuery
		}
		return shroute, nil
	case *pgquery.Node_DeleteStmt:
		clause := stmt.DeleteStmt.WhereClause
		if clause == nil {
			return &MultiMatchRoute{}, nil
		}

		shroute, err := qr.routeByExpr(ctx, clause)
		if err != nil {
			return nil, err
		}
		if shroute.Shkey.Name == NOSHARD {
			return nil, tooComplexQuery
		}
		return shroute, nil
	}

	return nil, tooComplexQuery
}

var ParseError = xerrors.New("parsing stmt error")

func (qr *ProxyQrouter) Route(ctx context.Context) (RoutingState, error) {
	parsedStmt, err := qr.parser.Stmt()

	if err != nil {
		return nil, err
	}

	if len(parsedStmt.Stmts) > 1 {
		return nil, fmt.Errorf("too complex query to route")
	}

	stmt := parsedStmt.Stmts[0]

	switch stmt.Stmt.Node.(type) {
	case *pgquery.Node_CreateStmt, *pgquery.Node_AlterTableStmt, *pgquery.Node_DropStmt, *pgquery.Node_TruncateStmt:
		// support simple ddl
		return ShardMatchState{
			Routes: qr.DataShardsRoutes(),
		}, nil
	default:

		routes, err := qr.matchShards(ctx, stmt)
		if err != nil {
			return nil, err
		}
		spqrlog.Logger.Printf(spqrlog.DEBUG1, "parsed shard %+v", routes)
		switch v := routes.(type) {
		case *DataShardRoute:
			return ShardMatchState{
				Routes: []*DataShardRoute{v},
			}, nil
		case *MultiMatchRoute:
			return MultiMatchState{}, nil
		}

		return SkipRoutingState{}, nil
	}
}
