package qrouter

import (
	"context"
	"fmt"
	"strings"

	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/models/hashfunction"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/pkg/models/shrule"
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
	exprs map[RelationFQN]map[string]string

	unparsed_columns map[string]struct{}

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

	rls       []*shrule.ShardingRule
	krs       []*kr.KeyRange
	dataspace string

	params [][]byte
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

func NewRoutingMetadataContext(
	krs []*kr.KeyRange,
	rls []*shrule.ShardingRule,
	ds string,
	params [][]byte) *RoutingMetadataContext {
	return &RoutingMetadataContext{
		rels:             map[RelationFQN][]string{},
		tableAliases:     map[string]RelationFQN{},
		exprs:            map[RelationFQN]map[string]string{},
		unparsed_columns: map[string]struct{}{},
		krs:              krs,
		rls:              rls,
		dataspace:        ds,
		params:           params,
	}
}

// TODO : unit tests
func (meta *RoutingMetadataContext) RecordConstExpr(resolvedRelation RelationFQN, colname string, expr string) {
	meta.rels[resolvedRelation] = append(meta.rels[resolvedRelation], colname)
	if _, ok := meta.exprs[resolvedRelation]; !ok {
		meta.exprs[resolvedRelation] = map[string]string{}
	}
	delete(meta.unparsed_columns, colname)
	meta.exprs[resolvedRelation][colname] = expr
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
func (qr *ProxyQrouter) DeparseKeyWithRangesInternal(ctx context.Context, key string, meta *RoutingMetadataContext) (*routingstate.DataShardRoute, error) {
	spqrlog.Zero.Debug().
		Str("key", key).
		Msg("checking key")

	spqrlog.Zero.Debug().
		Str("key", key).
		Int("key-ranges-count", len(meta.krs)).
		Msg("checking key with key ranges")

	for _, krkey := range meta.krs {
		if kr.CmpRangesLessEqual(krkey.LowerBound, []byte(key)) &&
			kr.CmpRangesLess([]byte(key), krkey.UpperBound) {
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
	case *lyx.AExprSConst:
		hashedKey, err := hashfunction.ApplyHashFunction([]byte(e.Value), hf)
		if err != nil {
			return nil, err
		}

		spqrlog.Zero.Debug().Str("key", e.Value).Str("hashed key", string(hashedKey)).Msg("applying hash function on key")
		return qr.DeparseKeyWithRangesInternal(ctx, string(hashedKey), meta)
	case *lyx.AExprIConst:
		val := fmt.Sprintf("%d", e.Value)
		hashedKey, err := hashfunction.ApplyHashFunction([]byte(val), hf)
		if err != nil {
			return nil, err
		}

		spqrlog.Zero.Debug().Int("key", e.Value).Str("hashed key", string(hashedKey)).Msg("applying hash function on key")
		return qr.DeparseKeyWithRangesInternal(ctx, string(hashedKey), meta)
	default:
		return nil, ComplexQuery
	}
}

func (meta *RoutingMetadataContext) RecordShardingColumnValue(alias, colname, value string) {
	if !meta.CheckColumnRls(colname) {
		spqrlog.Zero.Debug().
			Str("colname", colname).
			Msg("skip column due no rule mathing")
		return
	}

	resolvedRelation, err := meta.ResolveRelationByAlias(alias)
	if err != nil {
		// failed to relove relation, skip column
		meta.unparsed_columns[colname] = struct{}{}
		return
	}

	// will not work not ints
	meta.RecordConstExpr(resolvedRelation, colname, value)
}

// TODO : unit tests
// deparse sharding column-value pair from query Where clause
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

				alias, colname := lft.TableAlias, lft.ColName

				/* simple key-value pair */
				switch rght := texpr.Right.(type) {
				case *lyx.ParamRef:
					if rght.Number <= len(meta.params) {
						meta.RecordShardingColumnValue(alias, colname, string(meta.params[rght.Number-1]))
					}
					// else  error out?
				case *lyx.AExprSConst:
					// TBD: postpone routing from here to root of parsing tree
					meta.RecordShardingColumnValue(alias, colname, rght.Value)
				case *lyx.AExprIConst:
					// TBD: postpone routing from here to root of parsing tree
					// maybe expimely inefficient. Will be fixed in SPQR-2.0
					meta.RecordShardingColumnValue(alias, colname, fmt.Sprintf("%d", rght.Value))
				case *lyx.AExprList:
					if len(rght.List) != 0 {
						expr := rght.List[0]
						switch bexpr := expr.(type) {
						case *lyx.AExprSConst:
							// TBD: postpone routing from here to root of parsing tree
							meta.RecordShardingColumnValue(alias, colname, bexpr.Value)
						case *lyx.AExprIConst:
							// TBD: postpone routing from here to root of parsing tree
							// maybe expimely inefficient. Will be fixed in SPQR-2.0
							meta.RecordShardingColumnValue(alias, colname, fmt.Sprintf("%d", bexpr.Value))
						}
					}
				case *lyx.FuncApplication:
					// there are several types of queries like DELETE FROM rel WHERE colref = func_applicion
					// and func_applicion is actually routable statement.
					// ANY(ARRAY(subselect)) if one type.

					if strings.ToLower(rght.Name) == "any" {
						if len(rght.Args) > 0 {
							// maybe we should consider not only first arg.
							// however, consider only it

							switch argexpr := rght.Args[0].(type) {
							case *lyx.SubLink:

								// ignore all errors.
								_ = qr.DeparseSelectStmt(ctx, argexpr.SubSelect, meta)
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
		case *lyx.AExprIConst, *lyx.AExprSConst, *lyx.AExprBConst, *lyx.AExprNConst:
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

	/* SELECT * FROM VALUES() ... */
	case *lyx.ValueClause:
		meta.ValuesLists = s.Values
		return nil
	}

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

type StatementRelation interface {
	iRelation()
}

type AnyRelation struct{}
type SpecificRelation struct {
	Name string
}
type RelationList struct {
	Relations []string
}

func (r AnyRelation) iRelation()      {}
func (r SpecificRelation) iRelation() {}
func (r RelationList) iRelation()     {}

var _ StatementRelation = AnyRelation{}
var _ StatementRelation = SpecificRelation{}

// TODO : unit tests
func (qr *ProxyQrouter) getRelations(qstmt lyx.Node) (StatementRelation, error) {
	switch stmt := qstmt.(type) {

	/* TDB: comments? */

	case *lyx.VariableSetStmt:
		/* TBD: maybe skip all set stmts? */
		/*
		 * SET x = y etc., do not dispatch any statement to shards, just process this in router
		 */
		return &AnyRelation{}, nil

	case *lyx.VariableShowStmt:
		/*
		 if we want to reroute to execute this stmt, route to random shard
		 XXX: support intelegent show support, without direct query dispatch
		*/
		return &AnyRelation{}, nil

	// XXX: need alter table which renames sharding column to non-sharding column check
	case *lyx.CreateTable:
		/*
		 * Disallow to create table which does not contain any sharding column
		 */
		return &AnyRelation{}, nil
	case *lyx.Vacuum:
		/* Send vacuum to each shard */
		return &AnyRelation{}, nil
	case *lyx.Analyze:
		/* Send vacuum to each shard */
		return &AnyRelation{}, nil
	case *lyx.Cluster:
		/* Send vacuum to each shard */
		return &AnyRelation{}, nil
	case *lyx.Index:
		/*
		 * Disallow to index on table which does not contain any sharding column
		 */
		// XXX: doit
		return &AnyRelation{}, nil

	case *lyx.Alter, *lyx.Drop, *lyx.Truncate:
		// support simple ddl commands, route them to every chard
		// this is not fully ACID (not atomic at least)
		return &AnyRelation{}, nil
		/*
			 case *pgquery.Node_DropdbStmt, *pgquery.Node_DropRoleStmt:
				 // forbid under separate setting
				 return MultiMatchState{}, nil
		*/
	case *lyx.CreateRole, *lyx.CreateDatabase:
		// forbid under separate setting
		return &AnyRelation{}, nil
	case *lyx.Insert:
		switch q := stmt.TableRef.(type) {
		case *lyx.RangeVar:

			return &SpecificRelation{Name: q.RelationName}, nil
		default:
			return &AnyRelation{}, ComplexQuery
		}
	case *lyx.Select:
		if stmt.FromClause == nil || len(stmt.FromClause) == 0 {
			return &AnyRelation{}, nil
		}

		// Get relation names out of FROM clause
		return qr.getRelationFromNode(stmt.FromClause[0])
	case *lyx.Delete:
		return qr.getRelationFromNode(stmt.TableRef)
	case *lyx.Update:
		return qr.getRelationFromNode(stmt.TableRef)
	case *lyx.Copy:
		return qr.getRelationFromNode(stmt.TableRef)
	default:
		spqrlog.Zero.Debug().Interface("statement", stmt).Msg("proxy-routing message to all shards")
	}
	return nil, nil
}

// TODO : unit tests
// get all relations out of FROM clause
func (qr *ProxyQrouter) getRelationFromNode(node lyx.FromClauseNode) (*RelationList, error) {
	spqrlog.Zero.Debug().
		Type("node-type", node).
		Msg("getting relation name out of from node")
	switch q := node.(type) {
	case *lyx.RangeVar:
		return &RelationList{Relations: []string{q.RelationName}}, nil
	case *lyx.JoinExpr:
		var rRel, lRel *RelationList
		var err error
		if rRel, err = qr.getRelationFromNode(q.Rarg); err != nil {
			return nil, err
		}
		if lRel, err = qr.getRelationFromNode(q.Larg); err != nil {
			return nil, err
		}
		lRel.Relations = append(lRel.Relations, rRel.Relations...)
		return lRel, nil
	default:
		// other cases to consider
		// lateral join, natual, etc
	}

	return nil, nil
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
			switch subS := selectStmt.(type) {
			case *lyx.Select:
				spqrlog.Zero.Debug().Msg("routing insert stmt on select clause")
				_ = qr.DeparseSelectStmt(ctx, subS, meta)
				/* try target list */
				spqrlog.Zero.Debug().Msg("routing insert stmt on target list")
				/* this target list for some insert (...) sharding column */
				meta.TargetList = selectStmt.(*lyx.Select).TargetList
			case *lyx.ValueClause:
				meta.ValuesLists = subS.Values
			}
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

	if _, err := MatchShardingRule(ctx, node.TableName, entries, qr.mgr.QDB()); err == ErrRuleIntersect {
		return nil
	}
	return fmt.Errorf("create table stmt ignored: no sharding rule columns found")
}

func (qr *ProxyQrouter) routeWithRules(ctx context.Context, stmt lyx.Node, sph session.SessionParamsHolder) (routingstate.RoutingState, error) {
	if stmt == nil {
		// empty statement
		return routingstate.RandomMatchState{}, nil
	}

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

	queryDataspace := ""
	if sph.DataspaceIsDefault() {
		rel, err := qr.getRelations(stmt)
		if err == nil && rel != nil {
			switch t := rel.(type) {
			case *SpecificRelation:
				if relDataspace, err := qr.mgr.GetDataspace(ctx, t.Name); err != nil {
					return nil, err
				} else {
					queryDataspace = relDataspace.Id
				}
			case *RelationList:
				var dataspace string
				for _, relName := range t.Relations {
					if relDataspace, err := qr.mgr.GetDataspace(ctx, relName); err != nil {
						return nil, err
					} else {
						if dataspace != "" && dataspace != relDataspace.Id {
							return nil, fmt.Errorf("mismatching dataspaces %s and %s", dataspace, relDataspace)
						}
						dataspace = relDataspace.Id
					}
				}
				queryDataspace = dataspace
			case *AnyRelation:
				break
			default:
				return nil, fmt.Errorf("unknown statement relation type %T", rel)
			}
		}
	} else {
		queryDataspace = sph.Dataspace()
	}

	krs, err := qr.mgr.ListKeyRanges(ctx, queryDataspace)
	if err != nil {
		return nil, err
	}

	rls, err := qr.mgr.ListShardingRules(ctx, queryDataspace)
	if err != nil {
		return nil, err
	}

	meta := NewRoutingMetadataContext(krs, rls, queryDataspace, sph.BindParams())

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
				case *lyx.AExprIConst, *lyx.AExprSConst, *lyx.AExprNConst, *lyx.AExprBConst:
					// ok
					any_routable = true

				case *lyx.ColumnRef:
					/* Step 1.4.8.2 - SELECT current_schema special case */
					if e.ColName == "current_schema" {
						any_routable = true
					}
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

	/* Step 1.5: check if query contains any unparsed columns that are sharding rule column.
	Reject query if so */
	for colname := range meta.unparsed_columns {
		if _, err := MatchShardingRule(ctx, "", []string{colname}, qr.mgr.QDB()); err == ErrRuleIntersect {
			return nil, ComplexQuery
		}
	}

	/*
	 * Step 2: match all deparsed rules to sharding rules.
	 */

	var route routingstate.RoutingState
	route = nil
	if meta.exprs != nil {
		// traverse each deparsed relation from query
		var route_err error
		for rfqn, cols := range meta.rels {
			if rule, err := MatchShardingRule(ctx, rfqn.RelationName, cols, qr.mgr.QDB()); err != nil {
				for _, col := range cols {
					// TODO: multi-column hash functions
					hf, err := hashfunction.HashFunctionByName(rule.Entries[0].HashFunction)
					if err != nil {
						spqrlog.Zero.Debug().Err(err).Msg("failed to resolve hash function")
						continue
					}

					hashedKey, err := hashfunction.ApplyHashFunction([]byte(meta.exprs[rfqn][col]), hf)

					spqrlog.Zero.Debug().Str("key", meta.exprs[rfqn][col]).Str("hashed key", string(hashedKey)).Msg("applying hash function on key")

					if err != nil {
						spqrlog.Zero.Debug().Err(err).Msg("failed to apply hash function")
						continue
					}

					currroute, err := qr.DeparseKeyWithRangesInternal(ctx, string(hashedKey), meta)
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
		}
		if route == nil && route_err != nil {
			return nil, route_err
		}
	}

	spqrlog.Zero.Debug().Interface("deparsed-values-list", meta.ValuesLists)
	spqrlog.Zero.Debug().Interface("insertStmtCols", meta.InsertStmtCols)

	if len(meta.InsertStmtCols) != 0 {
		if rule, err := MatchShardingRule(ctx, meta.InsertStmtRel, meta.InsertStmtCols, qr.mgr.QDB()); err != nil {
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
func MatchShardingRule(ctx context.Context, relationName string, shardingEntries []string, db qdb.QDB) (*qdb.ShardingRule, error) {
	/*
	* Create set to search column names in `shardingEntries`
	 */
	checkSet := make(map[string]struct{}, len(shardingEntries))

	for _, k := range shardingEntries {
		checkSet[k] = struct{}{}
	}

	var mrule *qdb.ShardingRule

	mrule = nil

	err := db.MatchShardingRules(ctx, func(rules map[string]*qdb.ShardingRule) error {
		for _, rule := range rules {
			// Simple optimisation
			if len(rule.Entries) > len(shardingEntries) {
				continue
			}

			if rule.TableName != "" && rule.TableName != relationName {
				continue
			}

			allColumnsMatched := true

			for _, v := range rule.Entries {
				if _, ok := checkSet[v.Column]; !ok {
					allColumnsMatched = false
					break
				}
			}

			spqrlog.Zero.Debug().Str("rname", rule.ID).Bool("matched", allColumnsMatched).Msg("matching rule")

			/* In this rule, we successfully matched all columns */
			if allColumnsMatched {
				mrule = rule
				return ErrRuleIntersect
			}
		}

		return nil
	})

	return mrule, err
}
