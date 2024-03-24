package qrouter

import (
	"context"
	"fmt"
	"github.com/pg-sharding/spqr/pkg/models/spqrerror"
	"strings"

	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/models/hashfunction"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/pkg/session"
	"github.com/pg-sharding/spqr/pkg/spqrlog"
	"github.com/pg-sharding/spqr/qdb"
	"github.com/pg-sharding/spqr/router/routehint"
	"github.com/pg-sharding/spqr/router/routingstate"
	"github.com/pg-sharding/spqr/router/xproto"

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
	rels  map[RelationFQN]struct{}
	exprs map[RelationFQN]map[string]string

	unparsed_columns map[string]struct{}

	// needed to parse
	// SELECT * FROM t1 a where a.i = 1
	// rarg:{range_var:{relname:"t2" inh:true relpersistence:"p" alias:{aliasname:"b"}
	tableAliases map[string]RelationFQN

	params            [][]byte
	paramsFormatCodes []int16
	// TODO: include client ops and metadata here
}

func NewRoutingMetadataContext(params [][]byte, paramsFormatCodes []int16) *RoutingMetadataContext {
	meta := &RoutingMetadataContext{
		rels:             map[RelationFQN]struct{}{},
		tableAliases:     map[string]RelationFQN{},
		exprs:            map[RelationFQN]map[string]string{},
		unparsed_columns: map[string]struct{}{},
		params:           params,
	}
	// https://github.com/postgres/postgres/blob/master/src/backend/tcop/pquery.c#L635-L658
	if len(paramsFormatCodes) > 1 {
		meta.paramsFormatCodes = paramsFormatCodes
	} else if len(paramsFormatCodes) == 1 {

		/* single format specified, use for all columns */
		meta.paramsFormatCodes = make([]int16, len(meta.params))

		for i := 0; i < len(meta.params); i++ {
			meta.paramsFormatCodes[i] = paramsFormatCodes[0]
		}
	} else {
		/* use default format for all columns */
		meta.paramsFormatCodes = make([]int16, len(meta.params))
		for i := 0; i < len(meta.params); i++ {
			meta.paramsFormatCodes[i] = xproto.FormatCodeText
		}
	}

	return meta
}

// TODO : unit tests
func (meta *RoutingMetadataContext) RecordConstExpr(resolvedRelation RelationFQN, colname string, expr string) {
	meta.rels[resolvedRelation] = struct{}{}
	if _, ok := meta.exprs[resolvedRelation]; !ok {
		meta.exprs[resolvedRelation] = map[string]string{}
	}
	delete(meta.unparsed_columns, colname)
	meta.exprs[resolvedRelation][colname] = expr
}

// TODO : unit tests
func (meta *RoutingMetadataContext) ResolveRelationByAlias(alias string) (RelationFQN, error) {
	if _, ok := meta.rels[RelationFQN{RelationName: alias}]; ok {
		return RelationFQN{RelationName: alias}, nil
	}
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
func (qr *ProxyQrouter) DeparseKeyWithRangesInternal(_ context.Context, key string, krs []*kr.KeyRange) (*routingstate.DataShardRoute, error) {
	spqrlog.Zero.Debug().
		Str("key", key).
		Msg("checking key")

	spqrlog.Zero.Debug().
		Str("key", key).
		Int("key-ranges-count", len(krs)).
		Msg("checking key with key ranges")

	var matched_krkey *kr.KeyRange = nil

	for _, krkey := range krs {
		if kr.CmpRangesLessEqual(krkey.LowerBound, []byte(key)) &&
			(matched_krkey == nil || kr.CmpRangesLessEqual(matched_krkey.LowerBound, krkey.LowerBound)) {
			matched_krkey = krkey
		}
	}

	if matched_krkey != nil {
		if err := qr.mgr.ShareKeyRange(matched_krkey.ID); err != nil {
			return nil, err
		}
		return &routingstate.DataShardRoute{
			Shkey:     kr.ShardKey{Name: matched_krkey.ShardID},
			Matchedkr: matched_krkey,
		}, nil
	}
	spqrlog.Zero.Debug().Msg("failed to match key with ranges")

	return nil, FailedToFindKeyRange
}

func (qr *ProxyQrouter) RecordDistributionKeyColumnValueOnRFQN(meta *RoutingMetadataContext, resolvedRelation RelationFQN, colname, value string) {

	/* do not process non-distributed relations or columns not from relation distribution key */
	if ds, err := qr.Mgr().GetRelationDistribution(context.TODO(), resolvedRelation.RelationName); err != nil {
		return
	} else {
		// TODO: optimize
		ok := false
		for _, c := range ds.Relations[resolvedRelation.RelationName].DistributionKey {
			if c.Column == colname {
				ok = true
				break
			}
		}
		if !ok {
			// some junk column
			return
		}
	}

	// will not work not ints
	meta.RecordConstExpr(resolvedRelation, colname, value)
}

// TODO : unit tests
func (qr *ProxyQrouter) RecordDistributionKeyExprOnRFQN(meta *RoutingMetadataContext, resolvedRelation RelationFQN, colname string, expr lyx.Node) error {
	switch e := expr.(type) {
	case *lyx.ParamRef:
		if e.Number > len(meta.params) {
			return ComplexQuery
		}

		// switch parameter format code and convert into proper representation

		var routeParam []byte
		fc := meta.paramsFormatCodes[e.Number-1]

		switch fc {
		case xproto.FormatCodeBinary:
			// TODO: here we need to invoke out function for convertion
			// actually, we need to convert everything to binary format
		case xproto.FormatCodeText:
			routeParam = meta.params[e.Number-1]
		default:
			// ??? protoc violation
		}

		qr.RecordDistributionKeyColumnValueOnRFQN(meta, resolvedRelation, colname, string(routeParam))
		return nil
	case *lyx.AExprSConst:
		qr.RecordDistributionKeyColumnValueOnRFQN(meta, resolvedRelation, colname, string(e.Value))
		return nil
	case *lyx.AExprIConst:
		val := fmt.Sprintf("%d", e.Value)
		qr.RecordDistributionKeyColumnValueOnRFQN(meta, resolvedRelation, colname, string(val))
		return nil
	default:
		return ComplexQuery
	}
}

func (qr *ProxyQrouter) RecordDistributionKeyColumnValue(meta *RoutingMetadataContext, alias, colname, value string) {

	resolvedRelation, err := meta.ResolveRelationByAlias(alias)
	if err != nil {
		// failed to resolve relation, skip column
		meta.unparsed_columns[colname] = struct{}{}
		return
	}

	qr.RecordDistributionKeyColumnValueOnRFQN(meta, resolvedRelation, colname, value)
}

// routeByClause de-parses sharding column-value pair from Where clause of the query
// TODO : unit tests
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
						qr.RecordDistributionKeyColumnValue(meta, alias, colname, string(meta.params[rght.Number-1]))
					}
					// else  error out?
				case *lyx.AExprSConst:
					// TBD: postpone routing from here to root of parsing tree
					qr.RecordDistributionKeyColumnValue(meta, alias, colname, rght.Value)
				case *lyx.AExprIConst:
					// TBD: postpone routing from here to root of parsing tree
					// maybe expimely inefficient. Will be fixed in SPQR-2.0
					qr.RecordDistributionKeyColumnValue(meta, alias, colname, fmt.Sprintf("%d", rght.Value))
				case *lyx.AExprList:
					if len(rght.List) != 0 {
						expr := rght.List[0]
						switch bexpr := expr.(type) {
						case *lyx.AExprSConst:
							// TBD: postpone routing from here to root of parsing tree
							qr.RecordDistributionKeyColumnValue(meta, alias, colname, bexpr.Value)
						case *lyx.AExprIConst:
							// TBD: postpone routing from here to root of parsing tree
							// maybe expimely inefficient. Will be fixed in SPQR-2.0
							qr.RecordDistributionKeyColumnValue(meta, alias, colname, fmt.Sprintf("%d", bexpr.Value))
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
		/* random route */
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
			meta.rels[rqdn] = struct{}{}
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

func (qr *ProxyQrouter) deparseInsertFromSelectOffsets(ctx context.Context, stmt *lyx.Insert) ([]int, RelationFQN, bool, error) {
	insertCols := stmt.Columns

	spqrlog.Zero.Debug().
		Strs("insert columns", insertCols).
		Msg("deparsed insert statement columns")

	// compute matched sharding rule offsets
	offsets := make([]int, 0)
	var rfqn RelationFQN

	switch q := stmt.TableRef.(type) {
	case *lyx.RangeVar:
		rfqn = RelationFQNFromRangeRangeVar(q)

		var ds *qdb.Distribution
		var err error

		if ds, err = qr.Mgr().QDB().GetRelationDistribution(ctx, rfqn.RelationName); err != nil {
			return nil, RelationFQN{}, false, err
		}

		insertColsPos := map[string]int{}
		for i, c := range insertCols {
			insertColsPos[c] = i
		}

		distributionKey := ds.Relations[rfqn.RelationName].DistributionKey
		// TODO: check mapping by rules with multiple columns
		for _, col := range distributionKey {
			if val, ok := insertColsPos[col.Column]; !ok {
				/* Do not return err here.
				* This particulat insert stmt is un-routable, but still, give it a try
				* and continue parsing.
				* Example: INSERT INTO xx SELECT * FROM xx a WHERE a.w_id = 20;
				* we have no insert cols specified, but still able to route on select
				 */
				return nil, RelationFQN{}, false, nil
			} else {
				offsets = append(offsets, val)
			}
		}

		return offsets, rfqn, true, nil
	default:
		return nil, RelationFQN{}, false, ComplexQuery
	}
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
		if selectStmt := stmt.SubSelect; selectStmt != nil {

			insertCols := stmt.Columns

			switch subS := selectStmt.(type) {
			case *lyx.Select:
				spqrlog.Zero.Debug().Msg("routing insert stmt on select clause")
				_ = qr.DeparseSelectStmt(ctx, subS, meta)
				/* try target list */
				spqrlog.Zero.Debug().Msg("routing insert stmt on target list")
				/* this target list for some insert (...) sharding column */

				offsets, rfqn, success, err := qr.deparseInsertFromSelectOffsets(ctx, stmt)
				if err != nil {
					return err
				}
				if !success {
					break
				}

				targetList := subS.TargetList
				/* record all values from tl */

				tlUsable := true
				for i := range offsets {
					if offsets[i] >= len(targetList) {
						tlUsable = false
						break
					} else {
						switch targetList[offsets[i]].(type) {
						case *lyx.AExprIConst, *lyx.AExprBConst, *lyx.AExprSConst, *lyx.ParamRef, *lyx.AExprNConst:
						default:
							tlUsable = false
						}
					}
				}

				if tlUsable {
					for i := range offsets {
						_ = qr.RecordDistributionKeyExprOnRFQN(meta, rfqn, insertCols[offsets[i]], targetList[offsets[i]])
					}
				}

			case *lyx.ValueClause:
				valList := subS.Values
				/* record all values from values scan */
				offsets, rfqn, success, err := qr.deparseInsertFromSelectOffsets(ctx, stmt)
				if err != nil {
					return err
				}
				if !success {
					break
				}

				vlUsable := true
				for i := range offsets {
					if offsets[i] >= len(valList) {
						vlUsable = false
						break
					}
				}

				if vlUsable {
					for i := range offsets {
						_ = qr.RecordDistributionKeyExprOnRFQN(meta, rfqn, insertCols[offsets[i]], valList[offsets[i]])
					}
				}

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

// CheckTableIsRoutable Given table create statement, check if it is routable with some sharding rule
// TODO : unit tests
func (qr *ProxyQrouter) CheckTableIsRoutable(ctx context.Context, node *lyx.CreateTable, meta *RoutingMetadataContext) error {
	ds, err := qr.mgr.GetRelationDistribution(ctx, node.TableName)
	if err != nil {
		return err
	}

	entries := make(map[string]struct{})
	/* Collect sharding rule entries list from create statement */
	for _, elt := range node.TableElts {
		// hashing function name unneeded for sharding rules matching purpose
		entries[elt.ColName] = struct{}{}
	}

	rel, ok := ds.Relations[node.TableName]
	if !ok {
		return spqrerror.Newf(spqrerror.SPQR_METADATA_CORRUPTION, "relation \"%s\" not present in distribution \"%s\" it's attached to", node.TableName, ds.Id)
	}
	check := true
	for _, entry := range rel.DistributionKey {
		if _, ok = entries[entry.Column]; !ok {
			check = false
			break
		}
	}
	if check {
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

	/* TODO: delay this until step 2. */

	meta := NewRoutingMetadataContext(sph.BindParams(), sph.BindParamFormatCodes())

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
					/* case 1.4.8.1 - SELECT current_schema() special case */
					if e.Name == "current_schema" {
						any_routable = true
						/* case 1.4.8.2 - SELECT set_config(...) special case */
					} else if e.Name == "set_config" {
						any_routable = true
						/* case 1.4.8.3 - SELECT pg_is_in_recovery special case */
					} else if e.Name == "pg_is_in_recovery" {
						any_routable = true
					}
				case *lyx.AExprIConst, *lyx.AExprSConst, *lyx.AExprNConst, *lyx.AExprBConst:
					// ok
					any_routable = true

				case *lyx.ColumnRef:
					/* Step 1.4.8.4 - SELECT current_schema special case */
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

	/*
	 * Step 2: traverse all aggregated relation distribution tuples and route on them.
	 */

	var route routingstate.RoutingState
	route = nil
	var route_err error
	for rfqn := range meta.rels {
		// TODO: check by whole RFQN
		ds, err := qr.mgr.GetRelationDistribution(ctx, rfqn.RelationName)
		if err != nil {
			return nil, err
		}

		krs, err := qr.mgr.ListKeyRanges(ctx, ds.Id)
		if err != nil {
			return nil, err
		}

		distrKey := ds.Relations[rfqn.RelationName].DistributionKey

		ok := true

		var hashedKey []byte

		// TODO: multi-column routing. This works only for one-dim routing
		for i := 0; i < len(distrKey); i++ {
			hf, err := hashfunction.HashFunctionByName(distrKey[i].HashFunction)
			if err != nil {
				ok = false
				spqrlog.Zero.Debug().Err(err).Msg("failed to resolve hash function")
				break
			}

			col := distrKey[i].Column

			val, valOk := meta.exprs[rfqn][col]
			if !valOk {
				ok = false
				break
			}

			hashedKey, err = hashfunction.ApplyHashFunction([]byte(val), hf)

			spqrlog.Zero.Debug().Str("key", meta.exprs[rfqn][col]).Str("hashed key", string(hashedKey)).Msg("applying hash function on key")

			if err != nil {
				spqrlog.Zero.Debug().Err(err).Msg("failed to apply hash function")
				ok = false
				break
			}
		}

		if !ok {
			// skip this relation
			continue
		}

		currroute, err := qr.DeparseKeyWithRangesInternal(ctx, string(hashedKey), krs)
		if err != nil {
			route_err = err
			spqrlog.Zero.Debug().Err(route_err).Msg("temporarily skip the route error")
			continue
		}

		spqrlog.Zero.Debug().
			Interface("currroute", currroute).
			Str("table", rfqn.RelationName).
			Msg("calculated route for table/cols")

		route = routingstate.Combine(route, routingstate.ShardMatchState{
			Route:              currroute,
			TargetSessionAttrs: tsa,
		})

	}
	if route == nil && route_err != nil {
		return nil, route_err
	}

	// set up this variable if not yet
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
