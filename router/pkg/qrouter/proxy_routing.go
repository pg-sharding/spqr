package qrouter

import (
	"context"
	"fmt"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/pkg/spqrlog"
	"github.com/pg-sharding/spqr/qdb"
	"github.com/pg-sharding/spqr/qdb/ops"
	pgquery "github.com/pganalyze/pg_query_go/v2"
)

type RoutingMetadataContext struct {
	// this maps table names to its query-defined restrictions
	// All columns in query should be considered in context of its table,
	// to distinguish composite join/select queries routing schemas
	//
	// For example,
	// SELECT * FROM a join b WHERE a.c1 = <val> and b.c2 = <val>
	// and
	// SELECT * FROM a join b WHERE a.c1 = <val> and a.c2 = <val>
	// can be routed with different rules
	rels map[string][]string

	// last matched routing rule or nul
	routingRule *qdb.ShardingRule
	offsets     []int

	// needed to parse
	// SELECT * FROM t1 a where a.i = 1
	// rarg:{range_var:{relname:"t2" inh:true relpersistence:"p" alias:{aliasname:"b"}
	tableAliases map[string]string
	// TODO: include client ops and metadata here
}

func NewRoutingMetadataContext() *RoutingMetadataContext {
	return &RoutingMetadataContext{
		rels:         map[string][]string{},
		tableAliases: map[string]string{},
	}
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
var SkipColumn = fmt.Errorf("skip column for routing")
var ShardingKeysMissing = fmt.Errorf("shardiung keys are missing in query")
var CrossShardQueryUnsupported = fmt.Errorf("cross shard query unsupported")

// DeparseExprShardingEntries deparses sharding column entries(column names or aliased column names)
// e.g {fields:{string:{str:"a"}} fields:{string:{str:"i"}} for `WHERE a.i = 1`
func (qr *ProxyQrouter) DeparseExprShardingEntries(expr *pgquery.Node, meta *RoutingMetadataContext) (string, error) {
	var colnames []string

	spqrlog.Logger.Printf(spqrlog.DEBUG5, "deparsing column name %T", expr.Node)
	switch texpr := expr.Node.(type) {
	case *pgquery.Node_ColumnRef:
		for _, node := range texpr.ColumnRef.Fields {
			spqrlog.Logger.Printf(spqrlog.DEBUG4, "columnref field %v", node.Node)

			switch colname := node.Node.(type) {
			case *pgquery.Node_String_:
				colnames = append(colnames, colname.String_.Str)
			default:
				return "", ComplexQuery
			}
		}
	default:
		return "", ComplexQuery
	}

	switch len(colnames) {
	case 1:
		// pure table column ref
		return colnames[0], nil
	case 2:
		//
		meta.rels[meta.tableAliases[colnames[0]]] = append(meta.rels[meta.tableAliases[colnames[0]]],
			colnames[1])

		return colnames[1], nil
	default:
		return "", ComplexQuery
	}
}

func (qr *ProxyQrouter) deparseKeyWithRangesInternal(ctx context.Context, key string) (*DataShardRoute, error) {
	spqrlog.Logger.Printf(spqrlog.DEBUG1, "checking key %s", key)

	krs, err := qr.qdb.ListKeyRanges(ctx)

	if err != nil {
		return nil, err
	}

	spqrlog.Logger.Printf(spqrlog.DEBUG5, "checking key %s with %d key ranges", key, len(krs))

	for _, krkey := range krs {
		if kr.CmpRangesLess(krkey.LowerBound, []byte(key)) && kr.CmpRangesLess([]byte(key), krkey.UpperBound) {
			if err := qr.qdb.ShareKeyRange(krkey.KeyRangeID); err != nil {
				return nil, err
			}

			return &DataShardRoute{
				Shkey:     kr.ShardKey{Name: krkey.ShardID},
				Matchedkr: kr.KeyRangeFromDB(krkey),
			}, nil
		}
	}

	spqrlog.Logger.Printf(spqrlog.DEBUG2, "failed to match key with ranges")

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

func (qr *ProxyQrouter) RouteKeyWithRanges(ctx context.Context, expr *pgquery.Node, meta *RoutingMetadataContext) (*DataShardRoute, error) {
	spqrlog.Logger.Printf(spqrlog.DEBUG5, "deparsing key ranges %T", expr.Node)

	switch texpr := expr.Node.(type) {
	case *pgquery.Node_RowExpr:
		spqrlog.Logger.Printf(spqrlog.DEBUG5, "looking for row expr with columns %+v",
			texpr.RowExpr.Args[meta.offsets[0]])

		switch valexpr := texpr.RowExpr.Args[meta.offsets[0]].Node.(type) {
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
		if len(meta.offsets) == 0 {
			// TBD: check between routing case properly
			return qr.RouteKeyWithRanges(ctx, texpr.List.Items[0], meta)
		}
		return qr.RouteKeyWithRanges(ctx, texpr.List.Items[meta.offsets[0]], meta)
	case *pgquery.Node_ColumnRef:
		return nil, SkipColumn
	default:
		return nil, ComplexQuery
	}
}

func (qr *ProxyQrouter) routeByClause(ctx context.Context, expr *pgquery.Node, meta *RoutingMetadataContext) (*DataShardRoute, error) {
	spqrlog.Logger.Printf(spqrlog.DEBUG5, "deparsed stmt type %T", expr.Node)

	switch texpr := expr.Node.(type) {
	case *pgquery.Node_BoolExpr:
		spqrlog.Logger.Printf(spqrlog.DEBUG2, "boolean expr routing")

		var route *DataShardRoute = nil
		var err error
		for _, inExpr := range texpr.BoolExpr.Args {
			if route == nil {
				route, err = qr.routeByClause(ctx, inExpr, meta)
				if err != nil {
					// failed to parse some references
					// ignore
					continue
				}
			} else {
				inRoute, err := qr.routeByClause(ctx, inExpr, meta)
				if err != nil {
					// failed to parse some references
					// ignore
					continue
				}
				if inRoute.Matchedkr.ShardID != route.Matchedkr.ShardID {
					return nil, CrossShardQueryUnsupported
				}
			}
		}

		if route == nil {
			return nil, ComplexQuery
		}

		return route, nil

	case *pgquery.Node_AExpr:
		if !(texpr.AExpr.Kind == pgquery.A_Expr_Kind_AEXPR_OP || texpr.AExpr.Kind == pgquery.A_Expr_Kind_AEXPR_BETWEEN) {
			return nil, ComplexQuery
		}

		colname, err := qr.DeparseExprShardingEntries(texpr.AExpr.Lexpr, meta)
		if err != nil {
			return nil, err
		}

		spqrlog.Logger.Printf(spqrlog.DEBUG5, "deparsed columns references %+v", colname)
		// TBD: postpone routing from here to root of parsing tree
		if _, err := ops.MatchShardingRule(ctx, qr.qdb, "", []string{colname}); err == nil {
			return nil, ShardingKeysMissing
		}

		route, err := qr.RouteKeyWithRanges(ctx, texpr.AExpr.Rexpr, meta)
		if err != nil {
			return nil, err
		}
		return route, nil
	default:
		return nil, ComplexQuery
	}
}

func (qr *ProxyQrouter) DeparseSelectStmt(ctx context.Context, selectStmt *pgquery.Node, meta *RoutingMetadataContext) (ShardRoute, error) {
	spqrlog.Logger.Printf(spqrlog.DEBUG5, "val selectStmt is %T", selectStmt)

	switch q := selectStmt.Node.(type) {
	case *pgquery.Node_SelectStmt:
		if clause := q.SelectStmt.FromClause; clause != nil {
			// route `insert into rel select from` stmt
			spqrlog.Logger.Printf(spqrlog.DEBUG5, "deparsing select from clause, %+v", clause)
			if err := qr.deparseFromClauseList(clause, meta); err != nil {
				return nil, err
			}
		}

		if clause := q.SelectStmt.WhereClause; clause != nil {
			spqrlog.Logger.Printf(spqrlog.DEBUG5, "deparsing select where clause, %+v", clause)

			shard, err := qr.routeByClause(ctx, clause, meta)
			if err == nil {
				return shard, nil
			}
		}

		if list := q.SelectStmt.ValuesLists; len(list) != 0 {
			// route using first tuple from `VALUES` clause
			valNode := q.SelectStmt.ValuesLists[0]
			return qr.RouteKeyWithRanges(ctx, valNode, meta)
		}

		return nil, ComplexQuery
	default:
		return nil, ComplexQuery
	}
}

func (qr *ProxyQrouter) deparseFromNode(node *pgquery.Node, meta *RoutingMetadataContext) error {
	switch q := node.Node.(type) {
	case *pgquery.Node_RangeVar:
		if q.RangeVar.Alias != nil {
			/* remember table alias */
			meta.tableAliases[q.RangeVar.Alias.Aliasname] = q.RangeVar.Relname
		}
	case *pgquery.Node_JoinExpr:
		if err := qr.deparseFromNode(q.JoinExpr.Rarg, meta); err != nil {
			return err
		}
		if err := qr.deparseFromNode(q.JoinExpr.Larg, meta); err != nil {
			return err
		}
	default:
		// other cases to consider
	}

	return nil
}

func (qr *ProxyQrouter) deparseFromClauseList(clause []*pgquery.Node, meta *RoutingMetadataContext) error {
	for _, node := range clause {
		err := qr.deparseFromNode(node, meta)
		if err != nil {
			return err
		}
	}

	return nil
}

func (qr *ProxyQrouter) matchShards(ctx context.Context, qstmt *pgquery.RawStmt, meta *RoutingMetadataContext) (ShardRoute, error) {
	spqrlog.Logger.Printf(spqrlog.DEBUG5, "mathcing qstmt %T", qstmt.Stmt.Node)
	switch stmt := qstmt.Stmt.Node.(type) {
	case *pgquery.Node_SelectStmt:
		if stmt.SelectStmt.FromClause != nil {
			// collect table alias names, if any
			// for single-table queries, process as usual
			if err := qr.deparseFromClauseList(stmt.SelectStmt.FromClause, meta); err != nil {
				return nil, err
			}
		}
		clause := stmt.SelectStmt.WhereClause
		if clause == nil {
			return &MultiMatchRoute{}, nil
		}
		shroute, err := qr.routeByClause(ctx, clause, meta)
		if err != nil {
			return nil, err
		}
		if shroute.Shkey.Name == NOSHARD {
			return nil, err
		}
		return shroute, nil
	case *pgquery.Node_InsertStmt:

		var cols []string
		var colindxs []int
		for cindx, c := range stmt.InsertStmt.Cols {
			spqrlog.Logger.Printf(spqrlog.DEBUG5, "col tp is %T", c.Node)
			switch res := c.Node.(type) {
			case *pgquery.Node_ResTarget:
				cols = append(cols, res.ResTarget.Name)
				colindxs = append(colindxs, cindx)
			default:
				return nil, ShardingKeysMissing
			}
		}

		spqrlog.Logger.Printf(spqrlog.DEBUG5, "deparsed columns %+v and offset indexes %+v", cols, colindxs)

		if rule, err := ops.MatchShardingRule(ctx, qr.qdb, stmt.InsertStmt.Relation.Relname, cols); err == nil {
			if selectStmt := stmt.InsertStmt.SelectStmt; selectStmt != nil {
				spqrlog.Logger.Printf(spqrlog.DEBUG5, "routing insert stmt on select clause")
				return qr.DeparseSelectStmt(ctx, selectStmt, meta)
			}
			return nil, ShardingKeysMissing
		} else {
			meta.routingRule = rule
			// compute matched sharding rule offsets
			offsets := make([]int, 0)
			j := 0
			for i, s := range cols {
				if j == len(rule.Entries) {
					break
				}
				if s == rule.Entries[j].Column {
					offsets = append(offsets, i)
				}
			}

			meta.offsets = offsets
			return qr.DeparseSelectStmt(ctx, stmt.InsertStmt.SelectStmt, meta)
		}

	case *pgquery.Node_UpdateStmt:
		clause := stmt.UpdateStmt.WhereClause
		if clause == nil {
			return &MultiMatchRoute{}, nil
		}

		shroute, err := qr.routeByClause(ctx, clause, meta)
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

		shroute, err := qr.routeByClause(ctx, clause, meta)
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

		shroute, err := qr.routeByClause(ctx, clause, meta)
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

// CheckTableIsRoutable Given table create statment, check if it is routable with some sharding rule
func (qr *ProxyQrouter) CheckTableIsRoutable(ctx context.Context, node *pgquery.Node_CreateStmt) error {

	var entries []string
	/* Collect sharding rule entries list from create statement */
	for _, elt := range node.CreateStmt.TableElts {
		switch eltTar := elt.Node.(type) {
		case *pgquery.Node_ColumnDef:
			// hashing function name unneeded for sharding rules matching purpose
			entries = append(entries, eltTar.ColumnDef.Colname)
		default:
			spqrlog.Logger.Printf(spqrlog.DEBUG3, "current table elt type is %T %v", elt, elt)
		}
	}

	if _, err := ops.MatchShardingRule(ctx, qr.qdb, node.CreateStmt.Relation.Relname, entries); err == ops.ErrRuleIntersect {
		return nil
	}
	return nil
}

func (qr *ProxyQrouter) Route(ctx context.Context, parsedStmt *pgquery.ParseResult) (RoutingState, error) {
	if parsedStmt == nil {
		return nil, ComplexQuery
	}

	if len(parsedStmt.Stmts) > 1 {
		return nil, ComplexQuery
	}

	/*
	* Currently, deparse only first query from multi-statement query msg (Enhance)
	 */
	stmt := parsedStmt.Stmts[0]

	switch node := stmt.Stmt.Node.(type) {
	case *pgquery.Node_VariableSetStmt:
		/*
		* SET x = y etc, do not dispatch any statement to shards, just process this in router
		 */
		return MultiMatchState{}, nil
	case *pgquery.Node_CreateStmt: // XXX: need alter table which renames sharding column to non-sharding column check
		/*
		* Disallow to create table which does not contain any sharding column
		 */
		if err := qr.CheckTableIsRoutable(ctx, node); err != nil {
			return nil, err
		}
		return MultiMatchState{}, nil

	case *pgquery.Node_IndexStmt:
		/*
		* Disallow to index on table which does not contain any sharding column
		 */
		// XXX: doit
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
	case *pgquery.Node_InsertStmt:
		routes, err := qr.matchShards(ctx, stmt, NewRoutingMetadataContext())
		if err != nil {
			if qr.cfg.MulticastUnroutableInsertStatement {
				switch err {
				case ShardingKeysMissing:
					return MultiMatchState{}, nil
				}
			}
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
	default:
		// SELECT, UPDATE and/or DELETE stmts, which
		// would be routed with their WHERE clause
		routes, err := qr.matchShards(ctx, stmt, NewRoutingMetadataContext())
		if err != nil {
			spqrlog.Logger.Errorf("parse error %v", err)
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
