package qrouter

import (
	"context"
	"fmt"

	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/pkg/spqrlog"
	"github.com/pg-sharding/spqr/qdb/ops"
	pgquery "github.com/pganalyze/pg_query_go/v4"
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
	rels  map[string][]string
	exprs map[string]map[string]*pgquery.Node

	offsets []int

	// needed to parse
	// SELECT * FROM t1 a where a.i = 1
	// rarg:{range_var:{relname:"t2" inh:true relpersistence:"p" alias:{aliasname:"b"}
	tableAliases map[string]string

	// For
	// INSERT INTO x VALUES(**)
	// routing
	ValuesLists    []*pgquery.Node
	InsertStmtCols []string
	InsertStmtRel  string

	// For
	// INSERT INTO x (...) SELECT ...
	TargetList []*pgquery.Node

	// TODO: include client ops and metadata here
}

func NewRoutingMetadataContext() *RoutingMetadataContext {
	return &RoutingMetadataContext{
		rels:         map[string][]string{},
		tableAliases: map[string]string{},
		exprs:        map[string]map[string]*pgquery.Node{},
	}
}

var ComplexQuery = fmt.Errorf("too complex query to parse")
var SkipColumn = fmt.Errorf("skip column for routing")
var ShardingKeysMissing = fmt.Errorf("shardiung keys are missing in query")
var CrossShardQueryUnsupported = fmt.Errorf("cross shard query unsupported")

// DeparseExprShardingEntries deparses sharding column entries(column names or aliased column names)
// e.g {fields:{string:{str:"a"}} fields:{string:{str:"i"}} for `WHERE a.i = 1`
// returns alias and column name
func (qr *ProxyQrouter) DeparseExprShardingEntries(expr *pgquery.Node, meta *RoutingMetadataContext) (string, string, error) {
	var colnames []string

	spqrlog.Logger.Printf(spqrlog.DEBUG5, "deparsing column name %T, value: %v", expr.Node, expr.Node)
	switch texpr := expr.Node.(type) {
	case *pgquery.Node_ColumnRef:
		for _, node := range texpr.ColumnRef.Fields {
			spqrlog.Logger.Printf(spqrlog.DEBUG4, "columnref field %v", node.Node)

			switch colname := node.Node.(type) {
			case *pgquery.Node_String_:
				colnames = append(colnames, colname.String_.Sval)
			default:
				return "", "", ComplexQuery
			}
		}
	default:
		return "", "", ComplexQuery
	}

	switch len(colnames) {
	case 1:
		// pure table column ref
		return "", colnames[0], nil
	case 2:
		// aliased table column ref
		return colnames[0], colnames[1], nil
	default:
		return "", "", ComplexQuery
	}
}

func (qr *ProxyQrouter) deparseKeyWithRangesInternal(ctx context.Context, key string) (*DataShardRoute, error) {
	spqrlog.Logger.Printf(spqrlog.DEBUG1, "checking key %s", key)

	krs, err := qr.mgr.ListKeyRanges(ctx)

	if err != nil {
		return nil, err
	}

	spqrlog.Logger.Printf(spqrlog.DEBUG5, "checking key %s with %d key ranges", key, len(krs))

	for _, krkey := range krs {
		if kr.CmpRangesLess(krkey.LowerBound, []byte(key)) && kr.CmpRangesLess([]byte(key), krkey.UpperBound) {
			if err := qr.mgr.ShareKeyRange(krkey.ID); err != nil {
				return nil, err
			}

			return &DataShardRoute{
				Shkey:     kr.ShardKey{Name: krkey.ShardID},
				Matchedkr: krkey,
			}, nil
		}
	}

	spqrlog.Logger.Printf(spqrlog.DEBUG2, "failed to match key with ranges")

	return nil, ComplexQuery
}

func getbytes(val *pgquery.A_Const) (string, error) {
	switch valt := val.Val.(type) {
	case *pgquery.A_Const_Ival:
		return fmt.Sprintf("%d", valt.Ival.Ival), nil
	case *pgquery.A_Const_Sval:
		return valt.Sval.Sval, nil
	default:
		return "", ComplexQuery
	}
}

func (qr *ProxyQrouter) RouteKeyWithRanges(ctx context.Context, expr *pgquery.Node, meta *RoutingMetadataContext) (*DataShardRoute, error) {
	spqrlog.Logger.Printf(spqrlog.DEBUG5, "routing by key ranges %T, value: %v", expr.Node, expr.Node)

	switch texpr := expr.Node.(type) {
	case *pgquery.Node_RowExpr:
		spqrlog.Logger.Printf(spqrlog.DEBUG5, "looking for row expr with columns %+v",
			texpr.RowExpr.Args[meta.offsets[0]])

		switch valexpr := texpr.RowExpr.Args[meta.offsets[0]].Node.(type) {
		case *pgquery.Node_AConst:
			val, err := getbytes(valexpr.AConst)
			if err != nil {
				return nil, err
			}
			return qr.deparseKeyWithRangesInternal(ctx, val)
		default:
			return nil, ComplexQuery
		}
	case *pgquery.Node_AConst:
		val, err := getbytes(texpr.AConst)
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

func (qr *ProxyQrouter) routeByClause(ctx context.Context, expr *pgquery.Node, meta *RoutingMetadataContext) error {
	spqrlog.Logger.Printf(spqrlog.DEBUG5, "deparsed stmt type %T, value: %v", expr.Node, expr.Node)

	switch texpr := expr.Node.(type) {
	case *pgquery.Node_BoolExpr:
		spqrlog.Logger.Printf(spqrlog.DEBUG2, "boolean expr routing")
		var err error
		for _, inExpr := range texpr.BoolExpr.Args {
			if err = qr.routeByClause(ctx, inExpr, meta); err != nil {
				// failed to parse some references
				// ignore
				continue
			}
		}
		return nil

	case *pgquery.Node_AExpr:
		if !(texpr.AExpr.Kind == pgquery.A_Expr_Kind_AEXPR_OP || texpr.AExpr.Kind == pgquery.A_Expr_Kind_AEXPR_BETWEEN) {
			return ComplexQuery
		}

		alias, colname, err := qr.DeparseExprShardingEntries(texpr.AExpr.Lexpr, meta)
		if err != nil {
			return err
		}

		spqrlog.Logger.Printf(spqrlog.DEBUG5, "deparsed columns references %+v", colname)

		if rls, err := qr.mgr.ListShardingRules(ctx); err != nil {
			return err
		} else {
			ok := false
			for i := range rls {
				for _, c := range rls[i].Entries() {
					if c.Column == colname {
						ok = true
						break
					}
				}
			}
			spqrlog.Logger.Printf(spqrlog.DEBUG5, "skip column %v: no rule mathing", colname)
			if !ok {
				return nil
			}
		}

		if resolvedRelation, ok := meta.tableAliases[alias]; ok {
			// TBD: postpone routing from here to root of parsing tree

			meta.rels[resolvedRelation] = append(meta.rels[resolvedRelation], colname)
			if _, ok := meta.exprs[resolvedRelation]; !ok {
				meta.exprs[resolvedRelation] = map[string]*pgquery.Node{}
			}
			spqrlog.Logger.Printf(spqrlog.DEBUG3, "adding expr to relation %s column %s", resolvedRelation, colname)
			meta.exprs[resolvedRelation][colname] = texpr.AExpr.Rexpr
		} else {
			// TBD: postpone routing from here to root of parsing tree
			if len(meta.rels) > 1 {
				// ambiguity in column aliasing
				return ComplexQuery
			}
			for tbl := range meta.rels {
				resolvedRelation = tbl
			}

			meta.rels[resolvedRelation] = append(meta.rels[resolvedRelation], colname)
			if _, ok := meta.exprs[resolvedRelation]; !ok {
				meta.exprs[resolvedRelation] = map[string]*pgquery.Node{}
			}
			spqrlog.Logger.Printf(spqrlog.DEBUG3, "adding expr to relation %s column %s", resolvedRelation, colname)
			meta.exprs[resolvedRelation][colname] = texpr.AExpr.Rexpr
		}

		return nil
	default:
		return ComplexQuery
	}
}

func (qr *ProxyQrouter) DeparseSelectStmt(ctx context.Context, selectStmt *pgquery.Node, meta *RoutingMetadataContext) error {
	spqrlog.Logger.Printf(spqrlog.DEBUG5, "val selectStmt is %T, %v", selectStmt, selectStmt)

	switch q := selectStmt.Node.(type) {
	case *pgquery.Node_SelectStmt:
		meta.TargetList = q.SelectStmt.TargetList
		if clause := q.SelectStmt.FromClause; clause != nil {
			// route `insert into rel select from` stmt
			spqrlog.Logger.Printf(spqrlog.DEBUG5, "deparsing select from clause, %+v", clause)
			if err := qr.deparseFromClauseList(clause, meta); err != nil {
				return err
			}
		}

		if clause := q.SelectStmt.WhereClause; clause != nil {
			spqrlog.Logger.Printf(spqrlog.DEBUG5, "deparsing select where clause, %+v", clause)

			if err := qr.routeByClause(ctx, clause, meta); err == nil {
				return nil
			}
		}

		if list := q.SelectStmt.ValuesLists; len(list) != 0 {
			// route using first tuple from `VALUES` clause
			meta.ValuesLists = q.SelectStmt.ValuesLists
			return nil
		}

		return ComplexQuery
	default:
		return ComplexQuery
	}
}

func (qr *ProxyQrouter) deparseFromNode(node *pgquery.Node, meta *RoutingMetadataContext) error {
	spqrlog.Logger.Printf(spqrlog.DEBUG5, "deparsing from node %+v", node)
	switch q := node.Node.(type) {
	case *pgquery.Node_RangeVar:
		if _, ok := meta.rels[q.RangeVar.Relname]; !ok {
			meta.rels[q.RangeVar.Relname] = nil
		}
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

func (qr *ProxyQrouter) deparseShardingMapping(
	ctx context.Context,
	qstmt *pgquery.RawStmt,
	meta *RoutingMetadataContext) error {
	spqrlog.Logger.Printf(spqrlog.DEBUG5, "matching qstmt %T", qstmt.Stmt.Node)
	switch stmt := qstmt.Stmt.Node.(type) {
	case *pgquery.Node_SelectStmt:
		if stmt.SelectStmt.FromClause != nil {
			// collect table alias names, if any
			// for single-table queries, process as usual
			if err := qr.deparseFromClauseList(stmt.SelectStmt.FromClause, meta); err != nil {
				return err
			}
		}
		clause := stmt.SelectStmt.WhereClause
		if clause == nil {
			return nil
		}

		return qr.routeByClause(ctx, clause, meta)
	case *pgquery.Node_InsertStmt:
		var cols []string

		for _, c := range stmt.InsertStmt.Cols {
			spqrlog.Logger.Printf(spqrlog.DEBUG5, "column type is %T", c.Node)
			switch res := c.Node.(type) {
			case *pgquery.Node_ResTarget:
				cols = append(cols, res.ResTarget.Name)
			default:
				return ShardingKeysMissing
			}
		}

		spqrlog.Logger.Printf(spqrlog.DEBUG5, "deparsed insert statement columns %+v", cols)

		meta.InsertStmtCols = cols
		meta.InsertStmtRel = stmt.InsertStmt.Relation.Relname
		if selectStmt := stmt.InsertStmt.SelectStmt; selectStmt != nil {
			spqrlog.Logger.Printf(spqrlog.DEBUG5, "routing insert stmt on select clause")
			return qr.DeparseSelectStmt(ctx, selectStmt, meta)
		}
		return ShardingKeysMissing

	case *pgquery.Node_UpdateStmt:
		clause := stmt.UpdateStmt.WhereClause
		if clause == nil {
			return nil
		}
		return qr.routeByClause(ctx, clause, meta)
	case *pgquery.Node_DeleteStmt:
		clause := stmt.DeleteStmt.WhereClause
		if clause == nil {
			return nil
		}

		return qr.routeByClause(ctx, clause, meta)
	case *pgquery.Node_CopyStmt:
		if !stmt.CopyStmt.IsFrom {
			return fmt.Errorf("copy from stdout is not implemented")
		}
		spqrlog.Logger.Printf(spqrlog.DEBUG3, "copy query was: %s", qstmt.Stmt.String())
		clause := stmt.CopyStmt.WhereClause
		if clause == nil {
			// will not work
			return nil
		}

		return qr.routeByClause(ctx, clause, meta)
	}

	return ComplexQuery
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
			spqrlog.Logger.Printf(spqrlog.DEBUG3, "current table element type is %T %v", elt, elt)
		}
	}

	if _, err := ops.MatchShardingRule(ctx, qr.mgr, node.CreateStmt.Relation.Relname, entries); err == ops.ErrRuleIntersect {
		return nil
	}
	return nil
}

func (qr *ProxyQrouter) Route(ctx context.Context, parsedStmt *pgquery.ParseResult) (RoutingState, error) {
	var insert_err error
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
	meta := NewRoutingMetadataContext()

	tsa := config.TargetSessionAttrsAny

	/*
	* Step 1: traverse query tree and deparse mapping from
	* columns to their values (either contant or expression).
	* Note that exact (routing) value of (sharding) column may not be
	* known after this phase, as it can be Parse Step of Extended proto.
	 */

	switch node := stmt.Stmt.Node.(type) {
	case *pgquery.Node_CommentStmt:
		// shold not happen

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
	case *pgquery.Node_VacuumStmt:
		/* Send vacuum to each shard */
		return MultiMatchState{}, nil
	case *pgquery.Node_VacuumRelation:
		/* Send vacuum to each shard */
		return MultiMatchState{}, nil
	case *pgquery.Node_ClusterStmt:
		/* Send vacuum to each shard */
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
		err := qr.deparseShardingMapping(ctx, stmt, meta)
		if err != nil {
			if qr.cfg.MulticastUnroutableInsertStatement {
				switch err {
				case ShardingKeysMissing:
					return MultiMatchState{}, nil
				}
			}
			insert_err = err
		}
	default:
		// SELECT, UPDATE and/or DELETE stmts, which
		// would be routed with their WHERE clause
		err := qr.deparseShardingMapping(ctx, stmt, meta)
		if err != nil {
			spqrlog.Logger.Errorf("parse error %v", err)
			return nil, err
		}
	}

	/*
	* Step 2: match all deparsed rules to sharding rules.
	 */

	var route ShardRoute
	route = nil
	if meta.exprs != nil {
		// traverse each deparsed relation from query
		var route_err error
		for tname, cols := range meta.rels {
			if _, err := ops.MatchShardingRule(ctx, qr.mgr, tname, cols); err != nil {
				for _, col := range cols {
					currroute, err := qr.RouteKeyWithRanges(ctx, meta.exprs[tname][col], meta)
					if err != nil {
						route_err = err
						spqrlog.Logger.Printf(spqrlog.DEBUG1, "Temporarily skip the route error: %v", route_err)
						continue
					}
					spqrlog.Logger.Printf(spqrlog.DEBUG5, "calculated route %+v for table/cols %v %+v", currroute, tname, cols)
					if route == nil {
						route = currroute
					} else {
						route = combine(route, currroute)
					}
				}
			}
		}
		if route == nil && route_err != nil {
			return nil, route_err
		}
	}

	spqrlog.Logger.Printf(spqrlog.DEBUG4, "deparsed values list %+v, insertStmtCols %+v", meta.ValuesLists, meta.InsertStmtCols)
	if len(meta.InsertStmtCols) != 0 {
		if rule, err := ops.MatchShardingRule(ctx, qr.mgr, meta.InsertStmtRel, meta.InsertStmtCols); err != nil {
			// compute matched sharding rule offsets
			offsets := make([]int, 0)
			j := 0
			// TODO: check mapping by rules with multiple columns
			for i, s := range meta.InsertStmtCols {
				if j == len(rule.Entries()) {
					break
				}
				if s == rule.Entries()[j].Column {
					offsets = append(offsets, i)
				}
			}

			meta.offsets = offsets
			routed := false
			if insert_err != nil {
				if len(meta.offsets) != 0 && len(meta.TargetList) > meta.offsets[0] {
					currroute, err := qr.RouteKeyWithRanges(ctx, meta.TargetList[meta.offsets[0]].GetResTarget().Val, meta)
					if err != nil {
						return nil, err
					}

					spqrlog.Logger.Printf(spqrlog.DEBUG4, "deparsed route from %+v", currroute)
					routed = true
					if route == nil {
						route = currroute
					} else {
						route = combine(route, currroute)
					}
				} else {
					return nil, insert_err
				}
			}

			if !routed && meta.ValuesLists != nil {
				// only first value from value list
				currroute, err := qr.RouteKeyWithRanges(ctx, meta.ValuesLists[0], meta)
				if err != nil {
					return nil, err
				}
				spqrlog.Logger.Printf(spqrlog.DEBUG4, "deparsed route from %+v", currroute)
				if route == nil {
					route = currroute
				} else {
					route = combine(route, currroute)
				}
			}
		}
	}

	if route == nil {
		switch qr.cfg.DefaultRouteBehaviour {
		case "BLOCK":
			return SkipRoutingState{}, fmt.Errorf("failed to match query to any sharding rule")
		default:
			return MultiMatchState{}, nil
		}
	}

	spqrlog.Logger.Printf(spqrlog.DEBUG1, "parsed shard route %+v", route)
	switch v := route.(type) {
	case *DataShardRoute:
		return ShardMatchState{
			Routes:             []*DataShardRoute{v},
			TargetSessionAttrs: tsa,
		}, nil
	case *MultiMatchRoute:
		return MultiMatchState{}, nil
	}
	return SkipRoutingState{}, nil
}
