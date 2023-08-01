package qrouter

import (
	"context"
	"fmt"

	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/pkg/models/shrule"
	"github.com/pg-sharding/spqr/pkg/spqrlog"
	"github.com/pg-sharding/spqr/qdb/ops"

	"github.com/pg-sharding/lyx/lyx"
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
	exprs map[string]map[string]string

	unparsed_columns map[string]struct{}

	offsets []int

	// needed to parse
	// SELECT * FROM t1 a where a.i = 1
	// rarg:{range_var:{relname:"t2" inh:true relpersistence:"p" alias:{aliasname:"b"}
	tableAliases map[string]string

	// For
	// INSERT INTO x VALUES(**)
	// routing
	ValuesLists    []lyx.Node
	InsertStmtCols []string
	InsertStmtRel  string

	// For
	// INSERT INTO x (...) SELECT 7
	TargetList []lyx.Node

	rls []*shrule.ShardingRule
	krs []*kr.KeyRange

	// TODO: include client ops and metadata here
}

func (m *RoutingMetadataContext) CheckColumnRls(colname string) bool {
	for i := range m.rls {
		for _, c := range m.rls[i].Entries() {
			if c.Column == colname {
				return true
			}
		}
	}
	return false
}

func NewRoutingMetadataContext(krs []*kr.KeyRange, rls []*shrule.ShardingRule) *RoutingMetadataContext {
	return &RoutingMetadataContext{
		rels:             map[string][]string{},
		tableAliases:     map[string]string{},
		exprs:            map[string]map[string]string{},
		unparsed_columns: map[string]struct{}{},
		krs:              krs,
		rls:              rls,
	}
}

func (meta *RoutingMetadataContext) RecordConstExpr(resolvedRelation, colname string, expr *lyx.AExprConst) {
	meta.rels[resolvedRelation] = append(meta.rels[resolvedRelation], colname)
	if _, ok := meta.exprs[resolvedRelation]; !ok {
		meta.exprs[resolvedRelation] = map[string]string{}
	}
	delete(meta.unparsed_columns, colname)
	meta.exprs[resolvedRelation][colname] = expr.Value
}

func (meta *RoutingMetadataContext) ResolveRelationByAlias(alias string) (string, error) {
	if resolvedRelation, ok := meta.tableAliases[alias]; ok {
		// TBD: postpone routing from here to root of parsing tree
		return resolvedRelation, nil
	} else {
		// TBD: postpone routing from here to root of parsing tree
		if len(meta.rels) != 1 {
			// ambiguity in column aliasing
			return "", ComplexQuery
		}
		for tbl := range meta.rels {
			resolvedRelation = tbl
		}
		return resolvedRelation, nil
	}
}

var ComplexQuery = fmt.Errorf("too complex query to parse")
var FailedToMatch = fmt.Errorf("failed to match query to any sharding rule")
var SkipColumn = fmt.Errorf("skip column for routing")
var ShardingKeysMissing = fmt.Errorf("sharding keys are missing in query")
var CrossShardQueryUnsupported = fmt.Errorf("cross shard query unsupported")

// DeparseExprShardingEntries deparses sharding column entries(column names or aliased column names)
// e.g {fields:{string:{str:"a"}} fields:{string:{str:"i"}} for `WHERE a.i = 1`
// returns alias and column name

func (qr *ProxyQrouter) DeparseExprShardingEntries(expr lyx.Node, meta *RoutingMetadataContext) (string, string, error) {
	switch q := expr.(type) {
	case *lyx.ColumnRef:
		return q.TableAlias, q.ColName, nil
	default:
		return "", "", ComplexQuery
	}
}

func (qr *ProxyQrouter) deparseKeyWithRangesInternal(ctx context.Context, key string, meta *RoutingMetadataContext) (*DataShardRoute, error) {
	spqrlog.Zero.Debug().
		Str("key", key).
		Msg("checking key")

	spqrlog.Zero.Debug().
		Str("key", key).
		Int("key-ranges-count", len(meta.krs)).
		Msg("checking key with key ranges")

	for _, krkey := range meta.krs {
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

	spqrlog.Zero.Debug().Msg("failed to match key with ranges")

	return nil, ComplexQuery
}

func (qr *ProxyQrouter) RouteKeyWithRanges(ctx context.Context, expr lyx.Node, meta *RoutingMetadataContext) (*DataShardRoute, error) {
	switch e := expr.(type) {
	case *lyx.AExprConst:
		return qr.deparseKeyWithRangesInternal(ctx, e.Value, meta)
	default:
		return nil, ComplexQuery
	}
}

/* deparse sharding column-value pair from query Where clause */
func (qr *ProxyQrouter) routeByClause(ctx context.Context, expr lyx.Node, meta *RoutingMetadataContext) error {

	queue := make([]lyx.Node, 0)
	queue = append(queue, expr)

	for len(queue) != 0 {
		var curr lyx.Node
		curr, queue = queue[len(queue)-1], queue[:len(queue)-1]

		switch texpr := curr.(type) {
		case *lyx.AExprOp:

			switch lft := texpr.Left.(type) {
			case *lyx.ColumnRef:
				/* simple key-value pair */
				switch rght := texpr.Right.(type) {
				case *lyx.AExprConst:
					alias, colname := lft.TableAlias, lft.ColName

					if !meta.CheckColumnRls(colname) {
						spqrlog.Zero.Debug().
							Str("colname", colname).
							Msg("skip column due no rule mathing")
						continue
					}

					resolvedRelation, err := meta.ResolveRelationByAlias(alias)
					if err == nil {
						// TBD: postpone routing from here to root of parsing tree
						meta.RecordConstExpr(resolvedRelation, colname, rght)
					} else {
						meta.unparsed_columns[colname] = struct{}{}
					}

				case *lyx.AExprList:
					if len(rght.List) != 0 {
						expr := rght.List[0]
						switch bexpr := expr.(type) {
						case *lyx.AExprConst:
							alias, colname := lft.TableAlias, lft.ColName

							if !meta.CheckColumnRls(colname) {
								spqrlog.Zero.Debug().
									Str("colname", colname).
									Msg("skip column due no rule mathing")
								continue
							}

							resolvedRelation, err := meta.ResolveRelationByAlias(alias)
							if err == nil {
								// TBD: postpone routing from here to root of parsing tree
								meta.RecordConstExpr(resolvedRelation, colname, bexpr)
							} else {
								meta.unparsed_columns[colname] = struct{}{}
							}
						}
					}

				default:
					queue = append(queue, texpr.Left, texpr.Right)
				}
			default:
				/* Consider there cases */
				// if !(texpr.AExpr.Kind == pgquery.A_Expr_Kind_AEXPR_OP || texpr.Kind == pgquery.A_Expr_Kind_AEXPR_BETWEEN) {
				// 	return ComplexQuery
				// }

				queue = append(queue, texpr.Left, texpr.Right)
			}
		case *lyx.ColumnRef:
			/* colref = colref case, skip */
		case *lyx.AExprConst:
			/* should not happend */
		case *lyx.AExprEmpty:
			/*skip*/
		default:
			return ComplexQuery
		}
	}
	return nil
}

func (qr *ProxyQrouter) DeparseSelectStmt(ctx context.Context, selectStmt lyx.Node, meta *RoutingMetadataContext) error {
	switch s := selectStmt.(type) {
	case *lyx.Select:
		if clause := s.FromClause; clause != nil {
			// route `insert into rel select from` stmt
			if err := qr.deparseFromClauseList(clause, meta); err != nil {
				return err
			}
		}

		if clause := s.Where; clause != nil {
			spqrlog.Zero.Debug().
				Interface("clause", clause).
				Msg("deparsing select where clause")

			if err := qr.routeByClause(ctx, clause, meta); err == nil {
				return nil
			}
		}
	}

	/* SELECT * FROM VALUES() ... */
	// if list := selectStmt.; len(list) != 0 {
	// 	// route using first tuple from `VALUES` clause
	// 	meta.ValuesLists = q.SelectStmt.ValuesLists
	// 	return nil
	// }

	return ComplexQuery
}

/* deparses from  cluase  */
func (qr *ProxyQrouter) deparseFromNode(node lyx.FromClauseNode, meta *RoutingMetadataContext) error {
	spqrlog.Zero.Debug().
		Type("node-type", node).
		Msg("deparsing from node")
	switch q := node.(type) {
	case *lyx.RangeVar:
		if _, ok := meta.rels[q.RelationName]; !ok {
			meta.rels[q.RelationName] = nil
		}
		if q.Alias != "" {
			/* remember table alias */
			meta.tableAliases[q.Alias] = q.RelationName
		}
	case *lyx.JoinExpr:
		if err := qr.deparseFromNode(q.Rarg, meta); err != nil {
			return err
		}
		if err := qr.deparseFromNode(q.Larg, meta); err != nil {
			return err
		}
	default:
		// other cases to consider
		// lateral join, natual, etc

	}

	return nil
}

func (qr *ProxyQrouter) deparseFromClauseList(
	clause []lyx.FromClauseNode, meta *RoutingMetadataContext) error {
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
	qstmt lyx.Node,
	meta *RoutingMetadataContext) error {
	switch stmt := qstmt.(type) {
	case *lyx.Select:
		if stmt.FromClause != nil {
			// collect table alias names, if any
			// for single-table queries, process as usual
			if err := qr.deparseFromClauseList(stmt.FromClause, meta); err != nil {
				return err
			}
		}
		clause := stmt.Where
		if clause == nil {
			return nil
		}

		return qr.routeByClause(ctx, clause, meta)

	case *lyx.Insert:
		cols := stmt.Columns

		spqrlog.Zero.Debug().
			Strs("statements", cols).
			Msg("deparsed insert statement columns")

		meta.ValuesLists = stmt.Values

		meta.InsertStmtCols = cols
		switch q := stmt.TableRef.(type) {
		case *lyx.RangeVar:

			meta.InsertStmtRel = q.RelationName
		default:
			return ComplexQuery
		}

		if selectStmt := stmt.SubSelect; selectStmt != nil {
			spqrlog.Zero.Debug().Msg("routing insert stmt on select clause")
			_ = qr.DeparseSelectStmt(ctx, selectStmt, meta)
			/* try target list */
			spqrlog.Zero.Debug().Msg("routing insert stmt on target list")
			/* this target list for some insert (...) sharding column */
			meta.TargetList = selectStmt.(*lyx.Select).TargetList
		}

		return nil
	case *lyx.Update:
		clause := stmt.Where
		if clause == nil {
			return nil
		}

		_ = qr.deparseFromNode(stmt.TableRef, meta)
		return qr.routeByClause(ctx, clause, meta)
	case *lyx.Delete:
		clause := stmt.Where
		if clause == nil {
			return nil
		}

		_ = qr.deparseFromNode(stmt.TableRef, meta)

		return qr.routeByClause(ctx, clause, meta)
	case *lyx.Copy:
		if !stmt.IsFrom {
			return fmt.Errorf("copy from stdin is not implemented")
		}

		_ = qr.deparseFromNode(stmt.TableRef, meta)

		clause := stmt.Where

		if clause == nil {
			// will not work
			return nil
		}

		return qr.routeByClause(ctx, clause, meta)
	}

	return nil
}

var ParseError = fmt.Errorf("parsing stmt error")

// CheckTableIsRoutable Given table create statement, check if it is routable with some sharding rule

func (qr *ProxyQrouter) CheckTableIsRoutable(ctx context.Context, node *lyx.CreateTable, meta *RoutingMetadataContext) error {

	var entries []string
	/* Collect sharding rule entries list from create statement */
	for _, elt := range node.TableElts {
		// hashing function name unneeded for sharding rules matching purpose
		entries = append(entries, elt.ColName)
	}

	if _, err := ops.MatchShardingRule(ctx, qr.mgr, node.TableName, entries, qr.mgr.QDB()); err == ops.ErrRuleIntersect {
		return nil
	}
	return fmt.Errorf("create table stmt ignored: no sharding rule columns found")
}

func (qr *ProxyQrouter) Route(ctx context.Context, stmt lyx.Node) (RoutingState, error) {
	if stmt == nil {
		return nil, ComplexQuery
	}
	/*
	* Currently, deparse only first query from multi-statement query msg (Enhance)
	 */

	krs, err := qr.mgr.ListKeyRanges(ctx)

	if err != nil {
		return nil, err
	}

	rls, err := qr.mgr.ListShardingRules(ctx)
	if err != nil {
		return nil, err
	}

	meta := NewRoutingMetadataContext(krs, rls)

	tsa := config.TargetSessionAttrsAny

	/*
	* Step 1: traverse query tree and deparse mapping from
	* columns to their values (either contant or expression).
	* Note that exact (routing) value of (sharding) column may not be
	* known after this phase, as it can be Parse Step of Extended proto.
	 */

	switch node := stmt.(type) {

	/* TDB: comments? */
	// case *pgquery.Node_CommentStmt:
	// 	// shold not happen

	case *lyx.VarSet:
		/* TBD: maybe skip all set stmts? */
		/*
		* SET x = y etc, do not dispatch any statement to shards, just process this in router
		 */
		return MultiMatchState{}, nil
		// XXX: need alter table which renames sharding column to non-sharding column check

	case *lyx.CreateTable:
		/*
		* Disallow to create table which does not contain any sharding column
		 */
		if err := qr.CheckTableIsRoutable(ctx, node, meta); err != nil {
			return nil, err
		}
		return MultiMatchState{}, nil
	case *lyx.Vacuum:
		/* Send vacuum to each shard */
		return MultiMatchState{}, nil
	case *lyx.Analyze:
		/* Send vacuum to each shard */
		return MultiMatchState{}, nil
	case *lyx.Cluster:
		/* Send vacuum to each shard */
		return MultiMatchState{}, nil
	case *lyx.Index:
		/*
		* Disallow to index on table which does not contain any sharding column
		 */
		// XXX: doit
		return MultiMatchState{}, nil

	case *lyx.Alter, *lyx.Drop, *lyx.Truncate:
		// support simple ddl commands, route them to every chard
		// this is not fully ACID (not atomic at least)
		return MultiMatchState{}, nil
		/*
			case *pgquery.Node_DropdbStmt, *pgquery.Node_DropRoleStmt:
				// forbid under separate setting
				return MultiMatchState{}, nil
		*/
	case *lyx.CreateRole, *lyx.CreateDatabase:
		// forbid under separate setting
		return MultiMatchState{}, nil
	case *lyx.Insert:
		err := qr.deparseShardingMapping(ctx, stmt, meta)
		if err != nil {
			if qr.cfg.MulticastUnroutableInsertStatement {
				switch err {
				case ShardingKeysMissing:
					return MultiMatchState{}, nil
				}
			}
			return nil, err
		}
	case *lyx.Select:

		if len(node.FromClause) == 0 {

			/* Step 1.4.8: select a_expr is routable to any shard in case when a_expr is some type of
			data-independent expr */
			any_routable := true
			for _, expr := range node.TargetList {
				switch expr.(type) {
				case *lyx.AExprConst:
					// ok
				default:
					any_routable = false
				}
			}
			if any_routable {
				rs := qr.DataShardsRoutes()
				return ShardMatchState{
					Routes:             []*DataShardRoute{rs[0]},
					TargetSessionAttrs: tsa,
				}, nil
			}
		}

		// SELECT stmts, which
		// would be routed with their WHERE clause
		err := qr.deparseShardingMapping(ctx, stmt, meta)
		if err != nil {
			return nil, err
		}
	default:
		// UPDATE and/or DELETE stmts, which
		// would be routed with their WHERE clause
		err := qr.deparseShardingMapping(ctx, stmt, meta)
		if err != nil {
			return nil, err
		}
	}

	/* Step 1.5: check if query contains any unparsed columns that are sharding rule column.
	Reject query if so */
	for colname := range meta.unparsed_columns {
		if _, err := ops.MatchShardingRule(ctx, qr.mgr, "", []string{colname}, qr.mgr.QDB()); err == ops.ErrRuleIntersect {
			return nil, ComplexQuery
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
			if _, err := ops.MatchShardingRule(ctx, qr.mgr, tname, cols, qr.mgr.QDB()); err != nil {

				for _, col := range cols {
					currroute, err := qr.deparseKeyWithRangesInternal(ctx, meta.exprs[tname][col], meta)
					if err != nil {
						route_err = err
						spqrlog.Zero.Debug().Err(route_err).Msg("temporarily skip the route error")
						continue
					}
					spqrlog.Zero.Debug().
						Interface("currroute", currroute).
						Str("table", tname).
						Strs("columns", cols).
						Msg("calculated route for table/cols")
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

	spqrlog.Zero.Debug().Interface("deparsed-values-list", meta.ValuesLists)
	spqrlog.Zero.Debug().Interface("insertStmtCols", meta.InsertStmtCols)

	if len(meta.InsertStmtCols) != 0 {
		if rule, err := ops.MatchShardingRule(ctx, qr.mgr, meta.InsertStmtRel, meta.InsertStmtCols, qr.mgr.QDB()); err != nil {
			// compute matched sharding rule offsets
			offsets := make([]int, 0)
			j := 0
			// TODO: check mapping by rules with multiple columns
			for i, s := range meta.InsertStmtCols {
				if j == len(rule.Entries) {
					break
				}
				if s == rule.Entries[j].Column {
					offsets = append(offsets, i)
					j++
				}
			}

			meta.offsets = offsets
			routed := false
			if len(meta.offsets) != 0 && len(meta.TargetList) > meta.offsets[0] {

				currroute, err := qr.RouteKeyWithRanges(ctx, meta.TargetList[meta.offsets[0]], meta)
				if err != nil {
					/* failed, ignore */
				} else {

					spqrlog.Zero.Debug().
						Interface("current-route", currroute).
						Msg("deparsed route from current route")
					routed = true
					if route == nil {
						route = currroute
					} else {
						route = combine(route, currroute)
					}
				}
			}

			if len(meta.offsets) != 0 && len(meta.ValuesLists) > meta.offsets[0] && !routed && meta.ValuesLists != nil {
				// only first value from value list

				currroute, err := qr.RouteKeyWithRanges(ctx, meta.ValuesLists[meta.offsets[0]], meta)
				if err != nil { /* failed, ignore */

				} else {
					spqrlog.Zero.Debug().
						Interface("current-route", currroute).
						Msg("deparsed route from current route")
					if route == nil {
						route = currroute
					} else {
						route = combine(route, currroute)
					}
				}
			}
		}
	}
	if route == nil {
		switch qr.cfg.DefaultRouteBehaviour {
		case "BLOCK":
			return SkipRoutingState{}, FailedToMatch
		default:
			return MultiMatchState{}, nil
		}
	}

	spqrlog.Zero.Debug().
		Interface("route", route).
		Msg("parsed shard route")
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
