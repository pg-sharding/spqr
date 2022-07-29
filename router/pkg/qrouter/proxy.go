package qrouter

import (
	"context"
	"fmt"
	"math/rand"
	"sync"

	"github.com/pg-sharding/spqr/pkg/models/routers"

	"github.com/pg-sharding/spqr/qdb/ops"

	"github.com/jackc/pgproto3/v2"
	pgquery "github.com/pganalyze/pg_query_go/v2"

	"github.com/pg-sharding/spqr/pkg/spqrlog"

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
	DataShardCfgs  map[string]*config.Shard
	WorldShardCfgs map[string]*config.Shard

	qdb qdb.QrouterDB

	parser parser.QParser
}

func (qr *ProxyQrouter) ListDataShards(ctx context.Context) []*datashards.DataShard {
	qr.mu.Lock()
	defer qr.mu.Unlock()

	var ret []*datashards.DataShard
	for id, cfg := range qr.DataShardCfgs {
		ret = append(ret, datashards.NewDataShard(id, cfg))
	}
	return ret
}

func (qr *ProxyQrouter) ListShards(ctx context.Context) ([]*datashards.DataShard, error) {
	resp, err := qr.qdb.ListShards(ctx)
	if err != nil {
		return nil, err
	}
	var retShards []*datashards.DataShard

	for _, sh := range resp {
		retShards = append(retShards, &datashards.DataShard{
			ID: sh.ID,
			Cfg: &config.Shard{
				Hosts: sh.Hosts,
			},
		})
	}
	return retShards, nil
}

func (qr *ProxyQrouter) AddWorldShard(ctx context.Context, ds *datashards.DataShard) error {
	qr.mu.Lock()
	defer qr.mu.Unlock()

	spqrlog.Logger.Printf(spqrlog.LOG, "adding world datashard %s", ds.ID)
	qr.WorldShardCfgs[ds.ID] = ds.Cfg

	return nil
}

func (qr *ProxyQrouter) DropKeyRange(ctx context.Context, id string) error {
	qr.mu.Lock()
	defer qr.mu.Unlock()

	spqrlog.Logger.Printf(spqrlog.LOG, "dropping key range %s", id)
	return qr.qdb.DropKeyRange(ctx, id)
}

func (qr *ProxyQrouter) DropKeyRangeAll(ctx context.Context) ([]*kr.KeyRange, error) {
	qr.mu.Lock()
	defer qr.mu.Unlock()

	spqrlog.Logger.Printf(spqrlog.LOG, "dropping all key range")
	resp, err := qr.qdb.DropKeyRangeAll(ctx)
	var krid []*kr.KeyRange
	for _, krcurr := range resp {
		krid = append(krid, kr.KeyRangeFromDB(krcurr))
	}
	return krid, err
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
	defer qr.mu.Unlock()

	var ret []string

	for name := range qr.WorldShardCfgs {
		ret = append(ret, name)
	}

	return ret
}

var _ QueryRouter = &ProxyQrouter{}

func NewProxyRouter(shardMapping map[string]*config.Shard) (*ProxyQrouter, error) {
	db, err := mem.NewQrouterDBMem()
	if err != nil {
		return nil, err
	}

	proxy := &ProxyQrouter{
		DataShardCfgs:  map[string]*config.Shard{},
		WorldShardCfgs: map[string]*config.Shard{},
		qdb:            db,
	}

	for name, shardCfg := range shardMapping {
		switch shardCfg.Type {
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

func (qr *ProxyQrouter) Move(ctx context.Context, req *kr.MoveKeyRange) error {
	var krmv *qdb.KeyRange
	var err error
	if krmv, err = qr.qdb.CheckLocked(ctx, req.Krid); err != nil {
		return err
	}

	krmv.ShardID = req.ShardId
	return ops.ModifyKeyRangeWithChecks(ctx, qr.qdb, krmv)
}

func (qr *ProxyQrouter) Unite(ctx context.Context, req *kr.UniteKeyRange) error {
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

	// TODO: krRight seems to be empty.
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

	if err = qr.qdb.DropKeyRange(ctx, krleft.KeyRangeID); err != nil {
		return err
	}

	krRight.LowerBound = krleft.LowerBound

	return ops.ModifyKeyRangeWithChecks(ctx, qr.qdb, krRight)
}

func (qr *ProxyQrouter) Split(ctx context.Context, req *kr.SplitKeyRange) error {
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

	if err := ops.AddKeyRangeWithChecks(ctx, qr.qdb, krNew.ToSQL()); err != nil {
		return err
	}
	krOld.UpperBound = req.Bound
	_ = qr.qdb.UpdateKeyRange(ctx, krOld)

	return nil
}

func (qr *ProxyQrouter) LockKeyRange(ctx context.Context, krid string) (*kr.KeyRange, error) {
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

	return qr.qdb.AddShard(ctx, &qdb.Shard{
		ID:    ds.ID,
		Hosts: ds.Cfg.Hosts,
	})
}

func (qr *ProxyQrouter) Shards() []string {
	var ret []string

	for name := range qr.DataShardCfgs {
		ret = append(ret, name)
	}

	return ret
}

func (qr *ProxyQrouter) ListKeyRanges(ctx context.Context) ([]*kr.KeyRange, error) {
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

func (qr *ProxyQrouter) ListRouters(ctx context.Context) ([]*routers.Router, error) {
	return []*routers.Router{{
		Id: "local",
	}}, nil
}

func (qr *ProxyQrouter) AddShardingRule(ctx context.Context, rule *shrule.ShardingRule) error {
	if len(rule.Columns()) != 1 {
		return xerrors.New("only single column sharding rules are supported for now")
	}

	return qr.qdb.AddShardingRule(ctx, &qdb.ShardingRule{
		Id:       rule.ID(),
		Colnames: rule.Columns(),
	})
}

func (qr *ProxyQrouter) ListShardingRules(ctx context.Context) ([]*shrule.ShardingRule, error) {
	rules, err := qr.qdb.ListShardingRules(ctx)
	if err != nil {
		return nil, err
	}
	var resp []*shrule.ShardingRule
	for _, v := range rules {
		resp = append(resp, shrule.ShardingRuleFromDB(v))
	}

	return resp, nil
}

func (qr *ProxyQrouter) DropShardingRule(ctx context.Context, id string) error {
	return qr.qdb.DropShardingRule(ctx, id)
}

func (qr *ProxyQrouter) AddKeyRange(ctx context.Context, kr *kr.KeyRange) error {
	return ops.AddKeyRangeWithChecks(ctx, qr.qdb, kr.ToSQL())
}

func (qr *ProxyQrouter) MoveKeyRange(ctx context.Context, kr *kr.KeyRange) error {
	return ops.ModifyKeyRangeWithChecks(ctx, qr.qdb, kr.ToSQL())
}

func (qr *ProxyQrouter) routeByIndx(i []byte) *kr.KeyRange {
	krs, _ := qr.qdb.ListKeyRanges(context.TODO())

	for _, keyRange := range krs {
		spqrlog.Logger.Printf(spqrlog.DEBUG2, "comparing %v with key range %v %v", i, keyRange.LowerBound, keyRange.UpperBound)
		if kr.CmpRangesLess(keyRange.LowerBound, i) && kr.CmpRangesLess(i, keyRange.UpperBound) {
			return kr.KeyRangeFromDB(keyRange)
		}
	}

	return &kr.KeyRange{
		ShardID: NOSHARD,
	}
}

var ComplexQuery = fmt.Errorf("too complex query to parse")
var ShardingKeysMissing = fmt.Errorf("shardiung keys are missing in query")
var CrossShardQueryUnsupported = fmt.Errorf("cross shard query unsupported")

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
				return nil, ComplexQuery
			}
		}
	default:
		return nil, ComplexQuery
	}

	if len(colnames) != 1 {
		return nil, ComplexQuery
	}

	return colnames, nil
}

func (qr *ProxyQrouter) deparseKeyWithRangesInternal(ctx context.Context, key string) (*DataShardRoute, error) {
	spqrlog.Logger.Printf(spqrlog.DEBUG1, "checking key %s", key)

	krs, err := qr.qdb.ListKeyRanges(ctx)

	if err != nil {
		return nil, err
	}

	spqrlog.Logger.Printf(spqrlog.DEBUG5, "checking with %d key ranges", len(krs))

	for _, krkey := range krs {
		if kr.CmpRangesLess(krkey.LowerBound, []byte(key)) && kr.CmpRangesLess([]byte(key), krkey.UpperBound) {
			if err := qr.qdb.Share(krkey); err != nil {
				return nil, err
			}

			return &DataShardRoute{
				Shkey:     kr.ShardKey{Name: krkey.ShardID},
				Matchedkr: kr.KeyRangeFromDB(krkey),
			}, nil
		}
	}

	return nil, ComplexQuery
}

func getbytes(val *pgquery.Node) (string, error) {
	switch valt := val.Node.(type) {
	case *pgquery.Node_Integer:
		return fmt.Sprintf("%d", valt.Integer.Ival), nil
	case *pgquery.Node_String_:
		return valt.String_.Str, nil
	default:
		return "", ComplexQuery
	}
}

func (qr *ProxyQrouter) RouteKeyWithRanges(ctx context.Context, colindx int, expr *pgquery.Node) (*DataShardRoute, error) {
	spqrlog.Logger.Printf(spqrlog.DEBUG5, "deparsing key ranges %T", expr.Node)

	switch texpr := expr.Node.(type) {
	case *pgquery.Node_RowExpr:
		spqrlog.Logger.Printf(spqrlog.DEBUG5, "looking for row expr with columns %+v", texpr.RowExpr.Args[colindx])

		switch valexpr := texpr.RowExpr.Args[colindx].Node.(type) {
		case *pgquery.Node_AConst:
			val, err := getbytes(valexpr.AConst.Val)
			if err != nil {
				return nil, err
			}
			return qr.deparseKeyWithRangesInternal(ctx, val)
		default:
			return nil, ComplexQuery
		}
	case *pgquery.Node_AConst:
		val, err := getbytes(texpr.AConst.Val)
		if err != nil {
			return nil, err
		}
		return qr.deparseKeyWithRangesInternal(ctx, val)
	case *pgquery.Node_List:
		if len(texpr.List.Items) == 0 {
			return nil, ComplexQuery
		}
		return qr.RouteKeyWithRanges(ctx, colindx, texpr.List.Items[colindx])
	default:
		return nil, ComplexQuery
	}
}

func (qr *ProxyQrouter) routeByClause(ctx context.Context, expr *pgquery.Node) (*DataShardRoute, error) {
	spqrlog.Logger.Printf(spqrlog.DEBUG5, "deparsed stmt type %T", expr.Node)

	switch texpr := expr.Node.(type) {
	case *pgquery.Node_BoolExpr:
		spqrlog.Logger.Printf(spqrlog.DEBUG2, "bool expr routing")
		var route *DataShardRoute
		var err error
		for i, inExpr := range texpr.BoolExpr.Args {
			if i == 0 {
				route, err = qr.routeByClause(ctx, inExpr)
				if err != nil {
					return nil, err
				}
			} else {
				inRoute, err := qr.routeByClause(ctx, inExpr)
				if err != nil {
					return nil, err
				}
				if inRoute.Matchedkr.ShardID != route.Matchedkr.ShardID {
					return nil, CrossShardQueryUnsupported
				}
			}
		}

		return route, nil

	case *pgquery.Node_AExpr:
		if texpr.AExpr.Kind != pgquery.A_Expr_Kind_AEXPR_OP {
			return nil, ComplexQuery
		}

		colnames, err := qr.DeparseExprCol(texpr.AExpr.Lexpr)
		if err != nil {
			return nil, err
		}

		spqrlog.Logger.Printf(spqrlog.DEBUG5, "deparsed columns references %+v", colnames)

		if err := ops.CheckShardingRule(ctx, qr.qdb, colnames); err == nil {
			return nil, ShardingKeysMissing
		}

		route, err := qr.RouteKeyWithRanges(ctx, -1, texpr.AExpr.Rexpr)
		if err != nil {
			return nil, err
		}
		return route, nil
	default:
		return nil, ComplexQuery
	}
}

func (qr *ProxyQrouter) DeparseSelectStmt(ctx context.Context, colindx int, node *pgquery.Node) (ShardRoute, error) {

	spqrlog.Logger.Printf(spqrlog.DEBUG5, "val node is %T", node.Node)
	switch q := node.Node.(type) {
	case *pgquery.Node_SelectStmt:
		spqrlog.Logger.Printf(spqrlog.DEBUG5, "select stmt values list len is %d", len(q.SelectStmt.ValuesLists))
		if len(q.SelectStmt.ValuesLists) == 0 {
			return nil, ComplexQuery
		}
		// route using first tuple from `VALUES` clause
		valNode := q.SelectStmt.ValuesLists[0]
		return qr.RouteKeyWithRanges(ctx, colindx, valNode)
	}

	return nil, ComplexQuery
}

func (qr *ProxyQrouter) matchShards(ctx context.Context, qstmt *pgquery.RawStmt) (ShardRoute, error) {
	spqrlog.Logger.Printf(spqrlog.DEBUG5, "mathcing qstmt %T", qstmt.Stmt.Node)
	switch stmt := qstmt.Stmt.Node.(type) {
	case *pgquery.Node_SelectStmt:
		clause := stmt.SelectStmt.WhereClause
		if clause == nil {
			return &MultiMatchRoute{}, nil
		}
		shroute, err := qr.routeByClause(ctx, clause)
		if err != nil {
			return nil, err
		}
		if shroute.Shkey.Name == NOSHARD {
			return nil, err
		}
		return shroute, nil
	case *pgquery.Node_InsertStmt:
		for cindx, c := range stmt.InsertStmt.Cols {
			if sr, err := func() (ShardRoute, error) {
				spqrlog.Logger.Printf(spqrlog.DEBUG5, "col tp is %T", c.Node)
				switch res := c.Node.(type) {
				case *pgquery.Node_ResTarget:
					spqrlog.Logger.Printf(spqrlog.DEBUG1, "checking insert colname %v, %T", res.ResTarget.Name, stmt.InsertStmt.SelectStmt.Node)
					if err := ops.CheckShardingRule(ctx, qr.qdb, []string{res.ResTarget.Name}); err == nil {
						return nil, ShardingKeysMissing
					}
					return qr.DeparseSelectStmt(ctx, cindx, stmt.InsertStmt.SelectStmt)
				default:
					return nil, ShardingKeysMissing
				}
			}(); err != nil {
				continue
			} else {
				return sr, nil
			}
		}
		return nil, ShardingKeysMissing

	case *pgquery.Node_UpdateStmt:
		clause := stmt.UpdateStmt.WhereClause
		if clause == nil {
			return &MultiMatchRoute{}, nil
		}

		shroute, err := qr.routeByClause(ctx, clause)
		if err != nil {
			return nil, err
		}
		if shroute.Shkey.Name == NOSHARD {
			return nil, CrossShardQueryUnsupported
		}
		return shroute, nil
	case *pgquery.Node_DeleteStmt:
		clause := stmt.DeleteStmt.WhereClause
		if clause == nil {
			return &MultiMatchRoute{}, nil
		}

		shroute, err := qr.routeByClause(ctx, clause)
		if err != nil {
			return nil, err
		}
		if shroute.Shkey.Name == NOSHARD {
			return nil, CrossShardQueryUnsupported
		}
		return shroute, nil
	case *pgquery.Node_CopyStmt:
		if !stmt.CopyStmt.IsFrom {
			// COPY TO STOUT

		}
		spqrlog.Logger.Printf(spqrlog.DEBUG3, "copy query was: %s", qstmt.Stmt.String())
		clause := stmt.CopyStmt.WhereClause
		if clause == nil {
			// will not work
			return &MultiMatchRoute{}, nil
		}

		shroute, err := qr.routeByClause(ctx, clause)
		if err != nil {
			return nil, err
		}
		if shroute.Shkey.Name == NOSHARD {
			return nil, CrossShardQueryUnsupported
		}
		return shroute, nil
	}

	return nil, ComplexQuery
}

var ParseError = xerrors.New("parsing stmt error")

func (qr *ProxyQrouter) CheckTableShardingColumns(ctx context.Context, node *pgquery.Node_CreateStmt) error {

	for _, elt := range node.CreateStmt.TableElts {
		switch eltTar := elt.Node.(type) {
		case *pgquery.Node_ColumnDef:
			// TODO: multi-column sharding rules checks
			if err := ops.CheckShardingRule(ctx, qr.qdb, []string{eltTar.ColumnDef.Colname}); err == ops.RuleIntersec {
				return nil
			}
		default:
			spqrlog.Logger.Printf(spqrlog.DEBUG3, "current table elt type is %T %v", elt, elt)
		}
	}

	return ShardingKeysMissing
}

func (qr *ProxyQrouter) Route(ctx context.Context) (RoutingState, error) {
	parsedStmt, err := qr.parser.Stmt()

	if err != nil {
		return nil, err
	}

	if len(parsedStmt.Stmts) > 1 {
		return nil, ComplexQuery
	}

	stmt := parsedStmt.Stmts[0]

	switch node := stmt.Stmt.Node.(type) {
	case *pgquery.Node_VariableSetStmt:
		return MultiMatchState{}, nil
	case *pgquery.Node_CreateStmt: // XXX: need alter table which renames sharding column to non-sharding column check
		/*
		* Disallow to create table which does not contain any sharding column
		 */
		if err := qr.CheckTableShardingColumns(ctx, node); err != nil {
			return nil, err
		}
		return MultiMatchState{}, nil
	case *pgquery.Node_AlterTableStmt, *pgquery.Node_DropStmt, *pgquery.Node_TruncateStmt:
		// support simple ddl commands, route them to every chard
		// this is not fully ACID (not atomic at least)
		return MultiMatchState{}, nil
	case *pgquery.Node_DropdbStmt, *pgquery.Node_DropRoleStmt:
		// forbid under separate setting
		return MultiMatchState{}, nil
	case *pgquery.Node_CreateRoleStmt, *pgquery.Node_CreatedbStmt:
		// forbid under separate setting
		return MultiMatchState{}, nil
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
