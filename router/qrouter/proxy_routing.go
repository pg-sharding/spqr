package qrouter

import (
	"context"
	"fmt"

	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/models/hashfunction"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/pkg/session"
	"github.com/pg-sharding/spqr/pkg/spqrlog"
	"github.com/pg-sharding/spqr/qdb"
	"github.com/pg-sharding/spqr/router/routehint"
	"github.com/pg-sharding/spqr/router/routingstate"

	"github.com/pg-sharding/lyx/lyx"
)

type RelationFQN struct {
	RelationName string
	SchemaName   string
}

func RelationFQNFromRangeRangeVar(rv *lyx.RangeVar) RelationFQN {
	return RelationFQN{
		RelationName: rv.RelationName,
		SchemaName:   rv.SchemaName,
	}
}

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
	rels  map[RelationFQN][]string
	exprs map[RelationFQN]map[string]interface{}

	offsets []int

	// needed to parse
	// SELECT * FROM t1 a where a.i = 1
	// rarg:{range_var:{relname:"t2" inh:true relpersistence:"p" alias:{aliasname:"b"}
	tableAliases map[string]RelationFQN

	// For
	// INSERT INTO x VALUES(**)
	// routing
	ValuesLists    []lyx.Node
	InsertStmtCols []string
	InsertStmtRel  string

	// For
	// INSERT INTO x (...) SELECT 7
	TargetList []lyx.Node

	params [][]byte
	// TODO: include client ops and metadata here
}

func NewRoutingMetadataContext(
	params [][]byte) *RoutingMetadataContext {
	return &RoutingMetadataContext{
		rels:         map[RelationFQN][]string{},
		tableAliases: map[string]RelationFQN{},
		exprs:        map[RelationFQN]map[string]interface{}{},
		params:       params,
	}
}

// TODO : unit tests
func (meta *RoutingMetadataContext) RecordConstExpr(resolvedRelation RelationFQN, colname string, exprval interface{}) {
	meta.rels[resolvedRelation] = append(meta.rels[resolvedRelation], colname)
	if _, ok := meta.exprs[resolvedRelation]; !ok {
		meta.exprs[resolvedRelation] = map[string]interface{}{}
	}
	meta.exprs[resolvedRelation][colname] = exprval
}

// TODO : unit tests
func (meta *RoutingMetadataContext) ResolveRelationByAlias(alias string) (RelationFQN, error) {
	if resolvedRelation, ok := meta.tableAliases[alias]; ok {
		// TBD: postpone routing from here to root of parsing tree
		return resolvedRelation, nil
	} else {
		// TBD: postpone routing from here to root of parsing tree
		if len(meta.rels) != 1 {
			// ambiguity in column aliasing
			return RelationFQN{}, ComplexQuery
		}
		for tbl := range meta.rels {
			resolvedRelation = tbl
		}
		return resolvedRelation, nil
	}
}

var ComplexQuery = fmt.Errorf("too complex query to parse")
var InformationSchemaCombinedQuery = fmt.Errorf("combined information schema and regular relation is not supported")
var FailedToFindKeyRange = fmt.Errorf("failed to match key with ranges")
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

// TODO : unit tests
func (qr *ProxyQrouter) DeparseKeyWithRangesInternal(ctx context.Context, evals []interface{}, krs []*kr.KeyRange) (*routingstate.DataShardRoute, error) {

	spqrlog.Zero.Debug().
		Int("key-ranges-count", len(krs)).
		Msg("checking key with key ranges")

	for _, krkey := range krs {
		if kr.CmpRangesLessEqual(krkey.LowerBound, evals) &&
			kr.CmpRangesLess(evals, krkey.UpperBound) {
			if err := qr.mgr.ShareKeyRange(krkey.ID); err != nil {
				return nil, err
			}

			return &routingstate.DataShardRoute{
				Shkey:     kr.ShardKey{Name: krkey.ShardID},
				Matchedkr: krkey,
			}, nil
		}
	}

	spqrlog.Zero.Debug().Msg("failed to match key with ranges")

	return nil, FailedToFindKeyRange
}

// TODO : unit tests
func (qr *ProxyQrouter) RouteKeyWithRanges(ctx context.Context, expr lyx.Node, meta *RoutingMetadataContext, hf hashfunction.HashFunctionType) (*routingstate.DataShardRoute, error) {

	switch e := expr.(type) {
	case *lyx.ParamRef:
		if e.Number > len(meta.params) {
			return nil, ComplexQuery
		}
		hashedKey, err := hashfunction.ApplyHashFunction(meta.params[e.Number-1], hf)
		if err != nil {
			return nil, err
		}
		spqrlog.Zero.Debug().Str("key", string(meta.params[e.Number-1])).Str("hashed key", string(hashedKey)).Msg("applying hash function on key")

		return qr.DeparseKeyWithRangesInternal(ctx, string(hashedKey), meta)
	case *lyx.AExprIConst:
		// if
		hashedKey, err := hashfunction.ApplyHashFunction([]byte(e.Value), hf)
		if err != nil {
			return nil, err
		}

		spqrlog.Zero.Debug().Str("key", e.Value).Str("hashed key", string(hashedKey)).Msg("applying hash function on key")
		return qr.DeparseKeyWithRangesInternal(ctx, string(hashedKey), meta)
	default:
		return nil, ComplexQuery
	}
}

func (meta *RoutingMetadataContext) RecordConstExprOnColumnReference(colref *lyx.ColumnRef, val interface{}) {

	alias, colname := colref.TableAlias, colref.ColName

	resolvedRelation, err := meta.ResolveRelationByAlias(alias)
	if err == nil {
		// TBD: postpone routing from here to root of parsing tree
		meta.RecordConstExpr(resolvedRelation, colname, val)
	}
}

// TODO : unit tests
// deparse sharding column-value pair from query Where clause
// basically a AExpr parsing
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
				case *lyx.AExprIConst:
					meta.RecordConstExprOnColumnReference(lft, &rght.Value)
				case *lyx.AExprBConst:
					meta.RecordConstExprOnColumnReference(lft, &rght.Value)
				case *lyx.AExprSConst:
					meta.RecordConstExprOnColumnReference(lft, &rght.Value)
				/* do not record null value here AExprNConst*/
				case *lyx.AExprNConst:

				case *lyx.AExprList:
					if len(rght.List) != 0 {
						// only first value from list
						expr := rght.List[0]
						switch bexpr := expr.(type) {
						case *lyx.AExprIConst:
							meta.RecordConstExprOnColumnReference(lft, &bexpr.Value)
						case *lyx.AExprBConst:
							meta.RecordConstExprOnColumnReference(lft, &bexpr.Value)
						case *lyx.AExprSConst:
							meta.RecordConstExprOnColumnReference(lft, &bexpr.Value)
						/* do not record null value here AExprNConst*/
						case *lyx.AExprNConst:
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
		case *lyx.AExprSConst, *lyx.AExprBConst, *lyx.AExprNConst, *lyx.AExprIConst:
			/* should not happend */
		case *lyx.AExprEmpty:
			/*skip*/
		default:
			return ComplexQuery
		}
	}
	return nil
}

// TODO : unit tests
func (qr *ProxyQrouter) DeparseSelectStmt(ctx context.Context, selectStmt lyx.Node, meta *RoutingMetadataContext) error {
	switch s := selectStmt.(type) {
	case *lyx.ValueClause:
		meta.ValuesLists = s.Values
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

// TODO : unit tests
// deparses from clause
func (qr *ProxyQrouter) deparseFromNode(node lyx.FromClauseNode, meta *RoutingMetadataContext) error {
	spqrlog.Zero.Debug().
		Type("node-type", node).
		Msg("deparsing from node")
	switch q := node.(type) {
	case *lyx.RangeVar:
		rqdn := RelationFQNFromRangeRangeVar(q)
		if _, ok := meta.rels[rqdn]; !ok {
			meta.rels[rqdn] = nil
		}
		if q.Alias != "" {
			/* remember table alias */
			meta.tableAliases[q.Alias] = RelationFQNFromRangeRangeVar(q)
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

// TODO : unit tests
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

// TODO : unit tests
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
var ErrRuleIntersect = fmt.Errorf("sharding rule intersects with existing one")

// TODO : unit tests
// CheckTableIsRoutable Given table create statement, check if it is routable with some sharding rule
func (qr *ProxyQrouter) CheckTableIsRoutable(ctx context.Context, node *lyx.CreateTable, meta *RoutingMetadataContext) error {

	var entries []string
	/* Collect sharding rule entries list from create statement */
	for _, elt := range node.TableElts {
		// hashing function name unneeded for sharding rules matching purpose
		entries = append(entries, elt.ColName)
	}

	if _, err := MatchDataspace(ctx, node.TableName, entries, qr.mgr.QDB()); err == ErrRuleIntersect {
		return nil
	}
	return fmt.Errorf("create table stmt ignored: no sharding rule columns found")
}

func (qr *ProxyQrouter) routeWithRules(ctx context.Context, stmt lyx.Node, sph session.SessionParamsHolder) (routingstate.RoutingState, error) {
	if stmt == nil {
		// empty statement
		return routingstate.RandomMatchState{}, nil
	}

	// Step 0. Immediately return in we got explicit hint from client.
	// if route hint forces us to route on particular route, do it
	switch v := sph.RouteHint().(type) {
	case *routehint.EmptyRouteHint:
		// nothing
	case *routehint.TargetRouteHint:
		return v.State, nil
	case *routehint.ScatterRouteHint:
		// still, need to check config settings (later)
		return routingstate.MultiMatchState{}, nil
	}

	/*
	* Currently, deparse only first query from multi-statement query msg (Enhance)
	 */

	meta := NewRoutingMetadataContext(sph.BindParams())

	tsa := config.TargetSessionAttrsAny

	/*
	 * Step 1: traverse query tree and deparse mapping from
	 * columns to their values (either contant or expression).
	 * Note that exact (routing) value of (sharding) column may not be
	 * known after this phase, as it can be Parse Step of Extended proto.
	 */

	switch node := stmt.(type) {

	/* TDB: comments? */

	case *lyx.VariableSetStmt:
		/* TBD: maybe skip all set stmts? */
		/*
		 * SET x = y etc, do not dispatch any statement to shards, just process this in router
		 */
		return routingstate.RandomMatchState{}, nil

	case *lyx.VariableShowStmt:
		/*
		 if we want to reroute to execute this stmt, route to random shard
		 XXX: support intelegent show support, without direct query dispatch
		*/
		return routingstate.RandomMatchState{}, nil

	// XXX: need alter table which renames sharding column to non-sharding column check
	case *lyx.CreateTable:
		/*
		 * Disallow to create table which does not contain any sharding column
		 */
		if err := qr.CheckTableIsRoutable(ctx, node, meta); err != nil {
			return nil, err
		}
		return routingstate.MultiMatchState{}, nil
	case *lyx.Vacuum:
		/* Send vacuum to each shard */
		return routingstate.MultiMatchState{}, nil
	case *lyx.Analyze:
		/* Send vacuum to each shard */
		return routingstate.MultiMatchState{}, nil
	case *lyx.Cluster:
		/* Send vacuum to each shard */
		return routingstate.MultiMatchState{}, nil
	case *lyx.Index:
		/*
		 * Disallow to index on table which does not contain any sharding column
		 */
		// XXX: doit
		return routingstate.MultiMatchState{}, nil

	case *lyx.Alter, *lyx.Drop, *lyx.Truncate:
		// support simple ddl commands, route them to every chard
		// this is not fully ACID (not atomic at least)
		return routingstate.MultiMatchState{}, nil
		/*
			 case *pgquery.Node_DropdbStmt, *pgquery.Node_DropRoleStmt:
				 // forbid under separate setting
				 return MultiMatchState{}, nil
		*/
	case *lyx.CreateRole, *lyx.CreateDatabase:
		// forbid under separate setting
		return routingstate.MultiMatchState{}, nil
	case *lyx.Insert:
		err := qr.deparseShardingMapping(ctx, stmt, meta)
		if err != nil {
			if qr.cfg.MulticastUnroutableInsertStatement {
				switch err {
				case ShardingKeysMissing:
					return routingstate.MultiMatchState{}, nil
				}
			}
			return nil, err
		}
	case *lyx.Select:
		/* We cannot route SQL stmt with no FROM clause provided, but there is still
		* a few cases to consider
		 */
		if len(node.FromClause) == 0 {
			/* Step 1.4.8: select a_expr is routable to any shard in case when a_expr is some type of
			data-independent expr */

			spqrlog.Zero.Debug().Msg("checking any routable")

			any_routable := false
			for _, expr := range node.TargetList {
				switch e := expr.(type) {
				case *lyx.FuncApplication:
					/* Step 1.4.8.1 - SELECT current_schema() special case */
					if e.Name == "current_schema" {
						any_routable = true
					}
				case *lyx.ColumnRef:
					/* Step 1.4.8.2 - SELECT current_schema special case */
					if e.ColName == "current_schema" {
						any_routable = true
					}
				case *lyx.AExprIConst:
					// ok
					any_routable = true
				case *lyx.AExprSConst:
					// ok
					any_routable = true
				case *lyx.AExprBConst:
					// ok
					any_routable = true
				case *lyx.AExprNConst:
					// ok
					any_routable = true
				default:
				}
			}
			if any_routable {
				return routingstate.RandomMatchState{}, nil
			}
		} else {
			// SELECT stmts, which
			// would be routed with their WHERE clause
			err := qr.deparseShardingMapping(ctx, stmt, meta)
			if err != nil {
				return nil, err
			}
		}

		/* immidiately error-out some corner cases, for example, when client
		* tries to access information schema AND other relation in same TX
		* as we are unable to serve this properly. Or can we?
		 */
		has_inf_schema := false
		has_other_schema := false
		for rqfn := range meta.rels {
			if rqfn.SchemaName == "information_schema" {
				has_inf_schema = true
			} else {
				has_other_schema = true
			}
		}

		if has_inf_schema && has_other_schema {
			return nil, InformationSchemaCombinedQuery
		}
		if has_inf_schema {
			/* metadata-only relation can actually be routed somewhere */
			return routingstate.RandomMatchState{}, nil
		}

	case *lyx.Delete, *lyx.Update, *lyx.Copy:
		// UPDATE and/or DELETE, COPY stmts, which
		// would be routed with their WHERE clause
		err := qr.deparseShardingMapping(ctx, stmt, meta)
		if err != nil {
			return nil, err
		}
	default:
		spqrlog.Zero.Debug().Interface("statement", stmt).Msg("proxy-routing message to all shards")
	}

	/*
	 * Step 2: match all deparsed rules (relation + column names) to dataspace.
	 * route query within given dataspace. Combine computed routes.
	 */

	var route routingstate.RoutingState
	route = nil
	if meta.exprs != nil {
		// traverse each deparsed relation from query
		var route_err error
		for rfqn, cols := range meta.rels {
			if ds, err := MatchDataspace(ctx, rfqn.RelationName, cols, qr.mgr.QDB()); err != nil {

				// get key ranges for current query dataspace.
				krs, err := qr.mgr.ListKeyRanges(ctx, ds.ID)
				if err != nil {
					return nil, err
				}

				current_key := make([]interface{}, len(cols))
				for i := 0; i < len(cols); i++ {
					current_key[i] = meta.exprs[rfqn][cols[i]]
				}

				currroute, err := qr.DeparseKeyWithRangesInternal(ctx, current_key, krs, ds.ColTypes)
				if err != nil {
					route_err = err
					spqrlog.Zero.Debug().Err(route_err).Msg("temporarily skip the route error")
					continue
				}

				spqrlog.Zero.Debug().
					Interface("currroute", currroute).
					Str("table", rfqn.RelationName).
					Strs("columns", cols).
					Msg("calculated route for table/cols")

				route = routingstate.Combine(route, routingstate.ShardMatchState{
					Route:              currroute,
					TargetSessionAttrs: tsa,
				})

			}
		}
		if route == nil && route_err != nil {
			return nil, route_err
		}
	}

	spqrlog.Zero.Debug().Interface("deparsed-values-list", meta.ValuesLists)
	spqrlog.Zero.Debug().Interface("insertStmtCols", meta.InsertStmtCols)

	if len(meta.InsertStmtCols) != 0 {
		if ds, err := MatchDataspace(ctx, meta.InsertStmtRel, meta.InsertStmtCols, qr.mgr.QDB()); err != nil {
			// compute matched sharding rule offsets
			offsets := make([]int, 0)
			j := 0
			cols := ds.Relations[meta.InsertStmtRel]
			// TODO: check mapping by rules with multiple columns
			for i, s := range meta.InsertStmtCols {
				if j == len(ds.ColTypes) {
					break
				}
				if s == cols.ColNames[j] {
					offsets = append(offsets, i)
					j++
				}
			}

			hf, err := hashfunction.HashFunctionByName(rule.Entries[0].HashFunction)
			if err != nil {
				/* failed to resolve hash function */
				return nil, err
			}

			meta.offsets = offsets
			routed := false
			if len(meta.offsets) != 0 && len(meta.TargetList) > meta.offsets[0] {
				currroute, err := qr.RouteKeyWithRanges(ctx, meta.TargetList[meta.offsets[0]], meta, hf)
				if err == nil {
					/* else failed, ignore */
					spqrlog.Zero.Debug().
						Interface("current-route", currroute).
						Msg("deparsed route from current route")
					routed = true

					route = routingstate.Combine(route, routingstate.ShardMatchState{
						Route:              currroute,
						TargetSessionAttrs: tsa,
					})
				}
			}

			if len(meta.offsets) != 0 && len(meta.ValuesLists) > meta.offsets[0] && !routed && meta.ValuesLists != nil {
				// only first value from value list

				currroute, err := qr.RouteKeyWithRanges(ctx, meta.ValuesLists[meta.offsets[0]], meta, hf)
				if err == nil { /* else failed, ignore */
					spqrlog.Zero.Debug().
						Interface("current-route", currroute).
						Msg("deparsed route from current route")

					route = routingstate.Combine(route, routingstate.ShardMatchState{
						Route:              currroute,
						TargetSessionAttrs: tsa,
					})
				}
			}
		}
	}
	// set up this varibale if not yet
	if route == nil {
		route = routingstate.MultiMatchState{}
	}

	return route, nil
}

// TODO : unit tests
func (qr *ProxyQrouter) Route(ctx context.Context, stmt lyx.Node, sph session.SessionParamsHolder) (routingstate.RoutingState, error) {
	route, err := qr.routeWithRules(ctx, stmt, sph)
	if err != nil {
		return nil, err
	}

	switch v := route.(type) {
	case routingstate.ShardMatchState:
		return v, nil
	case routingstate.RandomMatchState:
		return v, nil
	case routingstate.MultiMatchState:
		switch sph.DefaultRouteBehaviour() {
		case "BLOCK":
			return routingstate.SkipRoutingState{}, FailedToMatch
		default:
			return routingstate.MultiMatchState{}, nil
		}
	}
	return routingstate.SkipRoutingState{}, nil
}

// TODO : unit tests
func MatchDataspace(ctx context.Context, relationName string, shardingEntries []string, db qdb.QDB) (*qdb.Dataspace, error) {
	/*
	* Create set to search column names in `shardingEntries`
	 */
	checkSet := make(map[string]struct{}, len(shardingEntries))

	for _, k := range shardingEntries {
		checkSet[k] = struct{}{}
	}

	ds, err := db.GetDataspaceForRelation(ctx, relationName)

	// Simple optimisation
	if len(ds.ColTypes) > len(shardingEntries) {
		return nil, nil
	}

	for _, v := range ds.Relations[relationName].ColNames {
		if _, ok := checkSet[v]; !ok {
			return nil, nil
		}
	}

	spqrlog.Zero.Debug().Str("daname", ds.ID).Msg("matching dataspace")
	/* In this rule, we successfully matched all columns */
	return ds, err
}
