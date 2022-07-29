package qrouter

import (
	"context"
	"fmt"

	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/pkg/spqrlog"
	"github.com/pg-sharding/spqr/qdb/ops"
	pgquery "github.com/pganalyze/pg_query_go/v2"
)

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

var ParseError = fmt.Errorf("parsing stmt error")

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

var _ QueryRouter = &ProxyQrouter{}
