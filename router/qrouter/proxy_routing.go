package qrouter

import (
	"context"
	"fmt"
	"strings"

	"github.com/pg-sharding/spqr/pkg/models/distributions"
	"github.com/pg-sharding/spqr/pkg/models/spqrerror"

	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/models/hashfunction"
	"github.com/pg-sharding/spqr/pkg/session"
	"github.com/pg-sharding/spqr/pkg/spqrlog"
	"github.com/pg-sharding/spqr/router/plan"
	"github.com/pg-sharding/spqr/router/planner"
	"github.com/pg-sharding/spqr/router/rerrors"
	"github.com/pg-sharding/spqr/router/rfqn"
	"github.com/pg-sharding/spqr/router/rmeta"
	"github.com/pg-sharding/spqr/router/routehint"
	"github.com/pg-sharding/spqr/router/xproto"

	"github.com/pg-sharding/lyx/lyx"
)

// DeparseExprShardingEntries deparses sharding column entries(column names or aliased column names)
// e.g {fields:{string:{str:"a"}} fields:{string:{str:"i"}} for `WHERE a.i = 1`
// returns alias and column name
func (qr *ProxyQrouter) DeparseExprShardingEntries(expr lyx.Node, meta *rmeta.RoutingMetadataContext) (string, string, error) {
	switch q := expr.(type) {
	case *lyx.ColumnRef:
		return q.TableAlias, q.ColName, nil
	default:
		return "", "", rerrors.ErrComplexQuery
	}
}

func (qr *ProxyQrouter) processConstExpr(alias, colname string, expr lyx.Node, meta *rmeta.RoutingMetadataContext) error {
	resolvedRelation, err := meta.ResolveRelationByAlias(alias)
	if err != nil {
		// failed to resolve relation, skip column
		return nil
	}

	return qr.processConstExprOnRFQN(resolvedRelation, colname, []lyx.Node{expr}, meta)
}

func (qr *ProxyQrouter) processConstExprOnRFQN(resolvedRelation rfqn.RelationFQN, colname string, exprs []lyx.Node, meta *rmeta.RoutingMetadataContext) error {
	off, tp := meta.GetDistributionKeyOffsetType(resolvedRelation, colname)
	if off == -1 {
		// column not from distr key
		return nil
	}

	for _, expr := range exprs {
		/* simple key-value pair */
		if err := meta.ProcessSingleExpr(resolvedRelation, tp, colname, expr); err != nil {
			return err
		}
	}

	return nil
}

// routeByClause de-parses sharding column-value pair from Where clause of the query
// TODO : unit tests
func (qr *ProxyQrouter) routeByClause(ctx context.Context, expr lyx.Node, meta *rmeta.RoutingMetadataContext) error {

	queue := make([]lyx.Node, 0)
	queue = append(queue, expr)

	for len(queue) != 0 {
		var curr lyx.Node
		curr, queue = queue[len(queue)-1], queue[:len(queue)-1]

		switch texpr := curr.(type) {
		case *lyx.AExprIn:

			switch lft := texpr.Expr.(type) {
			case *lyx.ColumnRef:

				alias, colname := lft.TableAlias, lft.ColName

				switch q := texpr.SubLink.(type) {
				case *lyx.AExprList:
					for _, expr := range q.List {
						if err := qr.processConstExpr(alias, colname, expr, meta); err != nil {
							return err
						}
					}
				case *lyx.Select:
					/* TODO support subquery here */
					/* SELECT * FROM t WHERE id IN (SELECT 1, 2) */
				}
			}

		case *lyx.AExprOp:

			switch lft := texpr.Left.(type) {
			case *lyx.ColumnRef:

				alias, colname := lft.TableAlias, lft.ColName

				/* simple key-value pair */
				switch right := texpr.Right.(type) {
				case *lyx.ParamRef, *lyx.AExprSConst, *lyx.AExprIConst:
					// else  error out?

					// TBD: postpone routing from here to root of parsing tree
					// maybe extremely inefficient. Will be fixed in SPQR-2.0
					if err := qr.processConstExpr(alias, colname, right, meta); err != nil {
						return err
					}

				case *lyx.AExprList:
					for _, expr := range right.List {
						if err := qr.processConstExpr(alias, colname, expr, meta); err != nil {
							return err
						}
					}
				case *lyx.FuncApplication:
					// there are several types of queries like DELETE FROM rel WHERE colref = func_application
					// and func_application is actually routable statement.
					// ANY(ARRAY(subselect)) if one type.

					if strings.ToLower(right.Name) == "any" {
						if len(right.Args) > 0 {
							// maybe we should consider not only first arg.
							// however, consider only it

							switch argexpr := right.Args[0].(type) {
							case *lyx.SubLink:

								// ignore all errors.
								_ = qr.DeparseSelectStmt(ctx, argexpr.SubSelect, meta)
							}
						}
					}

				default:
					queue = append(queue, texpr.Left, texpr.Right)
				}
			case *lyx.Select:
				if err := qr.DeparseSelectStmt(ctx, lft, meta); err != nil {
					return err
				}
			default:
				if texpr.Left != nil {
					queue = append(queue, texpr.Left)
				}
				if texpr.Right != nil {
					queue = append(queue, texpr.Right)
				}
			}
		case *lyx.ColumnRef:
			/* colref = colref case, skip */
		case *lyx.AExprIConst, *lyx.AExprSConst, *lyx.AExprBConst, *lyx.AExprNConst:
			/* should not happen */
		case *lyx.AExprEmpty:
			/*skip*/
		case *lyx.Select:
			/* in engine v2 we skip subplans */
		default:
			return fmt.Errorf("route by clause, unknown expr %T: %w", curr, rerrors.ErrComplexQuery)
		}
	}
	return nil
}

// TODO : unit tests
func (qr *ProxyQrouter) DeparseSelectStmt(ctx context.Context, selectStmt lyx.Node, meta *rmeta.RoutingMetadataContext) error {
	switch s := selectStmt.(type) {
	case *lyx.Select:
		if clause := s.FromClause; clause != nil {
			// route `insert into rel select from` stmt
			if err := qr.processFromClauseList(ctx, clause, meta); err != nil {
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

		if s.LArg != nil {
			if err := qr.DeparseSelectStmt(ctx, s.LArg, meta); err != nil {
				return err
			}
		}
		if s.RArg != nil {
			if err := qr.DeparseSelectStmt(ctx, s.RArg, meta); err != nil {
				return err
			}
		}

	/* SELECT * FROM VALUES() ... */
	case *lyx.ValueClause:
		/* random route */
		return nil
	}

	return rerrors.ErrComplexQuery
}

// TODO : unit tests
// deparses from clause
func (qr *ProxyQrouter) deparseFromNode(ctx context.Context, node lyx.FromClauseNode, meta *rmeta.RoutingMetadataContext) error {
	spqrlog.Zero.Debug().
		Type("node-type", node).
		Msg("deparsing from node")
	switch q := node.(type) {
	case *lyx.RangeVar:
		rqdn := rfqn.RelationFQNFromRangeRangeVar(q)

		// CTE, skip
		if meta.RFQNIsCTE(rqdn) {
			return nil
		}

		if _, err := meta.GetRelationDistribution(ctx, rqdn); err != nil {
			return err
		}

		if _, ok := meta.Rels[rqdn]; !ok {
			meta.Rels[rqdn] = struct{}{}
		}
		if q.Alias != "" {
			/* remember table alias */
			meta.TableAliases[q.Alias] = rfqn.RelationFQNFromRangeRangeVar(q)
		}
	case *lyx.JoinExpr:
		if err := qr.deparseFromNode(ctx, q.Rarg, meta); err != nil {
			return err
		}
		if err := qr.deparseFromNode(ctx, q.Larg, meta); err != nil {
			return err
		}
	case *lyx.SubSelect:
		return qr.DeparseSelectStmt(ctx, q.Arg, meta)
	default:
		// other cases to consider
		// lateral join, natural, etc

	}

	return nil
}

// TODO : unit tests
func (qr *ProxyQrouter) processFromClauseList(
	ctx context.Context,
	clause []lyx.FromClauseNode, meta *rmeta.RoutingMetadataContext) error {
	for _, node := range clause {
		err := qr.deparseFromNode(ctx, node, meta)
		if err != nil {
			return err
		}
	}

	return nil
}

func (qr *ProxyQrouter) processInsertFromSelectOffsets(ctx context.Context, stmt *lyx.Insert, meta *rmeta.RoutingMetadataContext) ([]int, rfqn.RelationFQN, plan.Plan, error) {
	insertCols := stmt.Columns

	spqrlog.Zero.Debug().
		Strs("insert columns", insertCols).
		Msg("deparsed insert statement columns")

	// compute matched sharding rule offsets
	offsets := make([]int, 0)
	var curr_rfqn rfqn.RelationFQN

	switch q := stmt.TableRef.(type) {
	case *lyx.RangeVar:

		spqrlog.Zero.Debug().
			Str("relname", q.RelationName).
			Str("schemaname", q.SchemaName).
			Msg("deparsed insert statement table ref")

		curr_rfqn = rfqn.RelationFQNFromRangeRangeVar(q)

		var ds *distributions.Distribution
		var err error

		if ds, err = meta.GetRelationDistribution(ctx, curr_rfqn); err != nil {
			return nil, rfqn.RelationFQN{}, nil, err
		}

		/* Omit distributed relations */
		if ds.Id == distributions.REPLICATED {
			return nil, rfqn.RelationFQN{}, plan.ReferenceRelationState{}, nil
		}

		insertColsPos := map[string]int{}
		for i, c := range insertCols {
			insertColsPos[c] = i
		}

		distributionKey := ds.Relations[curr_rfqn.RelationName].DistributionKey
		// TODO: check mapping by rules with multiple columns
		for _, col := range distributionKey {
			if val, ok := insertColsPos[col.Column]; !ok {
				/* Do not return err here.
				* This particular insert stmt is un-routable, but still, give it a try
				* and continue parsing.
				* Example: INSERT INTO xx SELECT * FROM xx a WHERE a.w_id = 20;
				* we have no insert cols specified, but still able to route on select
				 */
				return nil, rfqn.RelationFQN{}, plan.ShardDispatchPlan{}, nil
			} else {
				offsets = append(offsets, val)
			}
		}

		return offsets, curr_rfqn, plan.ShardDispatchPlan{}, nil
	default:
		return nil, rfqn.RelationFQN{}, nil, rerrors.ErrComplexQuery
	}
}

/* find this a better place */
func projectionList(l [][]lyx.Node, prjIndx int) []lyx.Node {
	var rt []lyx.Node
	for _, el := range l {
		rt = append(rt, el[prjIndx])
	}
	return rt
}

// TODO : unit tests
// May return nil routing state here - thats ok
func (qr *ProxyQrouter) planQueryV1(
	ctx context.Context,
	qstmt lyx.Node,
	meta *rmeta.RoutingMetadataContext) (plan.Plan, error) {
	switch stmt := qstmt.(type) {
	case *lyx.Select:
		if stmt.WithClause != nil {
			for _, cte := range stmt.WithClause {
				meta.CteNames[cte.Name] = struct{}{}
				if _, err := qr.planQueryV1(ctx, cte.SubQuery, meta); err != nil {
					return nil, err
				}
			}
		}

		if stmt.FromClause != nil {
			// collect table alias names, if any
			// for single-table queries, process as usual
			if err := qr.processFromClauseList(ctx, stmt.FromClause, meta); err != nil {
				return nil, err
			}
		}
		if stmt.Where == nil {
			return nil, nil
		}

		return nil, qr.routeByClause(ctx, stmt.Where, meta)

	case *lyx.Insert:
		if selectStmt := stmt.SubSelect; selectStmt != nil {

			insertCols := stmt.Columns

			var routingList [][]lyx.Node

			switch subS := selectStmt.(type) {
			case *lyx.Select:
				spqrlog.Zero.Debug().Msg("routing insert stmt on select clause")
				_ = qr.DeparseSelectStmt(ctx, subS, meta)
				/* try target list */
				spqrlog.Zero.Debug().Msg("routing insert stmt on target list")
				/* this target list for some insert (...) sharding column */

				routingList = [][]lyx.Node{subS.TargetList}
				/* record all values from tl */

			case *lyx.ValueClause:
				/* record all values from values scan */
				routingList = subS.Values
			default:
				return nil, nil
			}

			offsets, rfqn, state, err := qr.processInsertFromSelectOffsets(ctx, stmt, meta)
			if err != nil {
				return nil, err
			}

			switch state.(type) {
			case plan.ShardDispatchPlan:

				tlUsable := true
				if len(routingList) > 0 {
					/* check first tuple only */
					for i := range offsets {
						if offsets[i] >= len(routingList[0]) {
							tlUsable = false
							break
						} else {
							switch routingList[0][offsets[i]].(type) {
							case *lyx.AExprIConst, *lyx.AExprBConst, *lyx.AExprSConst, *lyx.ParamRef, *lyx.AExprNConst:
							default:
								tlUsable = false
							}
						}
					}
				}

				if tlUsable {
					for i := range offsets {
						_ = qr.processConstExprOnRFQN(rfqn, insertCols[offsets[i]], projectionList(routingList, offsets[i]), meta)
					}
				}
			case plan.ReferenceRelationState:
				if meta.SPH.EnhancedMultiShardProcessing() {
					return planner.PlanDistributedQuery(ctx, meta, stmt)
				}
				return nil, spqrerror.NewByCode(spqrerror.SPQR_NOT_IMPLEMENTED)
			}
		}

		return nil, nil
	case *lyx.Update:
		clause := stmt.Where
		if clause == nil {
			return nil, nil
		}

		switch q := stmt.TableRef.(type) {
		case *lyx.RangeVar:

			rqdn := rfqn.RelationFQNFromRangeRangeVar(q)

			if d, err := meta.GetRelationDistribution(ctx, rqdn); err != nil {
				return nil, err
			} else if d.Id == distributions.REPLICATED {
				if meta.SPH.EnhancedMultiShardProcessing() {
					return planner.PlanDistributedQuery(ctx, meta, stmt)
				}
				return nil, spqrerror.NewByCode(spqrerror.SPQR_NOT_IMPLEMENTED)
			}
		default:
			return nil, spqrerror.NewByCode(spqrerror.SPQR_NOT_IMPLEMENTED)
		}

		_ = qr.deparseFromNode(ctx, stmt.TableRef, meta)
		return nil, qr.routeByClause(ctx, clause, meta)
	case *lyx.Delete:
		clause := stmt.Where
		if clause == nil {
			return nil, nil
		}

		switch q := stmt.TableRef.(type) {
		case *lyx.RangeVar:

			rqdn := rfqn.RelationFQNFromRangeRangeVar(q)

			if d, err := meta.GetRelationDistribution(ctx, rqdn); err != nil {
				return nil, err
			} else if d.Id == distributions.REPLICATED {
				if meta.SPH.EnhancedMultiShardProcessing() {
					return planner.PlanDistributedQuery(ctx, meta, stmt)
				}
				return nil, spqrerror.NewByCode(spqrerror.SPQR_NOT_IMPLEMENTED)
			}
		default:
			return nil, spqrerror.NewByCode(spqrerror.SPQR_NOT_IMPLEMENTED)
		}

		_ = qr.deparseFromNode(ctx, stmt.TableRef, meta)

		return nil, qr.routeByClause(ctx, clause, meta)
	}

	return nil, nil
}

// CheckTableIsRoutable Given table create statement, check if it is routable with some sharding rule
// TODO : unit tests
func (qr *ProxyQrouter) CheckTableIsRoutable(ctx context.Context, node *lyx.CreateTable) error {
	var err error
	var ds *distributions.Distribution
	var relname string

	if node.PartitionOf != nil {
		switch q := node.PartitionOf.(type) {
		case *lyx.RangeVar:
			relname = q.RelationName
			_, err = qr.mgr.GetRelationDistribution(ctx, relname)
			return err
		default:
			return fmt.Errorf("Partition of is not a range var")
		}
	}

	switch q := node.TableRv.(type) {
	case *lyx.RangeVar:
		relname = q.RelationName
		ds, err = qr.mgr.GetRelationDistribution(ctx, relname)
		if err != nil {
			return err
		}
		if ds.Id == distributions.REPLICATED {
			return nil
		}
	default:
		return fmt.Errorf("wrong type of table range var")
	}

	entries := make(map[string]struct{})
	/* Collect sharding rule entries list from create statement */
	for _, elt := range node.TableElts {
		// hashing function name unneeded for sharding rules matching purpose
		switch q := elt.(type) {
		case *lyx.TableElt:
			entries[q.ColName] = struct{}{}
		}
	}

	rel, ok := ds.Relations[relname]
	if !ok {
		return spqrerror.Newf(spqrerror.SPQR_METADATA_CORRUPTION, "relation \"%s\" not present in distribution \"%s\" it's attached to", relname, ds.Id)
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

func (qr *ProxyQrouter) resolveValue(meta *rmeta.RoutingMetadataContext, rfqn rfqn.RelationFQN, col string, bindParams [][]byte, paramResCodes []int16) ([]interface{}, bool) {

	if vals, ok := meta.Exprs[rfqn][col]; ok {
		return vals, true
	}

	inds, ok := meta.ParamRefs[rfqn][col]
	if !ok {
		return nil, false
	}

	off, tp := meta.GetDistributionKeyOffsetType(rfqn, col)
	if off == -1 {
		// column not from distr key
		return nil, false
	}

	// TODO: switch column type here
	// only works for one value
	ind := inds[0]
	fc := paramResCodes[ind]

	singleVal, res := plan.ParseResolveParamValue(fc, ind, tp, bindParams)

	return []interface{}{singleVal}, res
}

// Returns state, is read-only flag and err if any
func (qr *ProxyQrouter) routeWithRules(ctx context.Context, rm *rmeta.RoutingMetadataContext, stmt lyx.Node) (plan.Plan, bool, error) {
	if stmt == nil {
		// empty statement
		return plan.RandomDispatchPlan{}, false, nil
	}

	rh, err := rm.ResolveRouteHint()

	if err != nil {
		return nil, false, err
	}

	// if route hint forces us to route on particular route, do it
	switch v := rh.(type) {
	case *routehint.EmptyRouteHint:
		// nothing
	case *routehint.TargetRouteHint:
		return v.State, false, nil
	case *routehint.ScatterRouteHint:
		// still, need to check config settings (later)
		/* XXX: we return true for RO here to fool DRB */
		return plan.ScatterPlan{}, true, nil
	}

	/*
	* Currently, deparse only first query from multi-statement query msg (Enhance)
	 */

	/* TODO: delay this until step 2. */

	tsa := config.TargetSessionAttrsAny

	var route plan.Plan
	route = nil

	/*
	 * Step 1: traverse query tree and deparse mapping from
	 * columns to their values (either constant or expression).
	 * Note that exact (routing) value of (sharding) column may not be
	 * known after this phase, as it can be Parse Step of Extended proto.
	 */

	ro := true

	switch node := stmt.(type) {

	/* TDB: comments? */

	case *lyx.VariableSetStmt:
		/* TBD: maybe skip all set stmts? */
		/*
		 * SET x = y etc., do not dispatch any statement to shards, just process this in router
		 */
		return plan.RandomDispatchPlan{}, true, nil

	case *lyx.VariableShowStmt:
		/*
		 if we want to reroute to execute this stmt, route to random shard
		 XXX: support intelligent show support, without direct query dispatch
		*/
		return plan.RandomDispatchPlan{}, true, nil

	// XXX: need alter table which renames sharding column to non-sharding column check
	case *lyx.CreateSchema:
		return plan.DDLState{}, false, nil
	case *lyx.Grant:
		return plan.DDLState{}, false, nil
	case *lyx.CreateTable:
		if val := rm.SPH.AutoDistribution(); val != "" {

			switch q := node.TableRv.(type) {
			case *lyx.RangeVar:

				/* pre-attach relation to its distribution
				 * sic! this is not transactional not abortable
				 */
				if err := qr.mgr.AlterDistributionAttach(ctx, val, []*distributions.DistributedRelation{
					{
						Name: q.RelationName,
						DistributionKey: []distributions.DistributionKeyEntry{
							{
								Column: rm.SPH.DistributionKey(),
								/* support hash function here */
							},
						},
					},
				}); err != nil {
					return nil, false, err
				}
			}
		}
		/*
		 * Disallow to create table which does not contain any sharding column
		 */
		if err := qr.CheckTableIsRoutable(ctx, node); err != nil {
			return nil, false, err
		}
		return plan.DDLState{}, false, nil
	case *lyx.Vacuum:
		/* Send vacuum to each shard */
		return plan.DDLState{}, false, nil
	case *lyx.Analyze:
		/* Send vacuum to each shard */
		return plan.DDLState{}, false, nil
	case *lyx.Cluster:
		/* Send vacuum to each shard */
		return plan.DDLState{}, false, nil
	case *lyx.Index:
		/*
		 * Disallow to index on table which does not contain any sharding column
		 */
		// XXX: do it
		return plan.DDLState{}, false, nil

	case *lyx.Alter, *lyx.Drop, *lyx.Truncate:
		// support simple ddl commands, route them to every chard
		// this is not fully ACID (not atomic at least)
		return plan.DDLState{}, false, nil
		/*
			 case *pgquery.Node_DropdbStmt, *pgquery.Node_DropRoleStmt:
				 // forbid under separate setting
				 return MultiMatchState{}, nil
		*/
	case *lyx.CreateRole, *lyx.CreateDatabase:
		// forbid under separate setting
		return plan.DDLState{}, false, nil
	case *lyx.Insert:
		rs, err := qr.planQueryV1(ctx, stmt, rm)
		if err != nil {
			return nil, false, err
		}

		ro = false

		route = plan.Combine(route, rs)
	case *lyx.Select:

		var deparseError error

		/* We cannot route SQL statements without a FROM clause. However, there are a few cases to consider. */
		if len(node.FromClause) == 0 && (node.LArg == nil || node.RArg == nil) {
			for _, expr := range node.TargetList {
				switch e := expr.(type) {
				/* Special cases for SELECT current_schema(), SELECT set_config(...), and SELECT pg_is_in_recovery() */
				case *lyx.FuncApplication:

					/* for queries, that need to access data on shard, ignore these "virtual" func.	 */
					if e.Name == "current_schema" || e.Name == "set_config" || e.Name == "pg_is_in_recovery" || e.Name == "version" {
						return plan.RandomDispatchPlan{}, ro, nil
					}
					for _, innerExp := range e.Args {
						switch iE := innerExp.(type) {
						case *lyx.Select:
							_, _ = qr.planQueryV1(ctx, iE, rm)
						}
					}
				/* Expression like SELECT 1, SELECT 'a', SELECT 1.0, SELECT true, SELECT false */
				case *lyx.AExprIConst, *lyx.AExprSConst, *lyx.AExprNConst, *lyx.AExprBConst:
					return plan.RandomDispatchPlan{}, ro, nil

				/* Special case for SELECT current_schema */
				case *lyx.ColumnRef:
					if e.ColName == "current_schema" {
						return plan.RandomDispatchPlan{}, ro, nil
					}
				case *lyx.Select:
					_, _ = qr.planQueryV1(ctx, e, rm)
				}
			}

		} else if node.LArg != nil && node.RArg != nil {
			/* deparse populates the FromClause info,
			 * so it do recurse into both branches, even if an error is encountered
			 */
			if _, err := qr.planQueryV1(ctx, node.LArg, rm); err != nil {
				deparseError = err
			}
			if _, err := qr.planQueryV1(ctx, node.RArg, rm); err != nil {
				deparseError = err
			}
		} else {
			/*  SELECT stmts, which would be routed with their WHERE clause */
			_, deparseError = qr.planQueryV1(ctx, stmt, rm)
		}

		/*
		 *  Sometimes we have problems with some cases. For example, if a client
		 *  tries to access information schema AND other relation in same TX.
		 *  We are unable to serve this properly.
		 *  But if this is a catalog-only query, we can route it to any shard.
		 */
		hasInfSchema, onlyCatalog, anyCatalog, hasOtherSchema := false, true, false, false

		for rqfn := range rm.Rels {
			if strings.HasPrefix(rqfn.RelationName, "pg_") {
				anyCatalog = true
			} else {
				onlyCatalog = false
			}
			if rqfn.SchemaName == "information_schema" {
				hasInfSchema = true
			} else {
				hasOtherSchema = true
			}
		}

		if onlyCatalog && anyCatalog {
			return plan.RandomDispatchPlan{}, ro, nil
		}
		if hasInfSchema && hasOtherSchema {
			return nil, false, rerrors.ErrInformationSchemaCombinedQuery
		}
		if hasInfSchema {
			return plan.RandomDispatchPlan{}, ro, nil
		}

		if deparseError != nil {
			return nil, false, deparseError
		}

	case *lyx.Delete, *lyx.Update:
		// UPDATE and/or DELETE, COPY stmts, which
		// would be routed with their WHERE clause
		rs, err := qr.planQueryV1(ctx, stmt, rm)
		if err != nil {
			return nil, false, err
		}
		ro = false
		route = plan.Combine(route, rs)
	case *lyx.Copy:
		return plan.CopyState{}, false, nil
	default:
		return nil, false, spqrerror.NewByCode(spqrerror.SPQR_NOT_IMPLEMENTED)
	}

	/*
	 * Step 2: traverse all aggregated relation distribution tuples and route on them.
	 */

	paramsFormatCodes := rm.SPH.BindParamFormatCodes()
	var queryParamsFormatCodes []int16

	paramsLen := len(rm.SPH.BindParams())

	// https://github.com/postgres/postgres/blob/master/src/backend/tcop/pquery.c#L635-L658
	if len(paramsFormatCodes) > 1 {
		queryParamsFormatCodes = paramsFormatCodes
	} else if len(paramsFormatCodes) == 1 {

		/* single format specified, use for all columns */
		queryParamsFormatCodes = make([]int16, paramsLen)

		for i := range paramsLen {
			queryParamsFormatCodes[i] = paramsFormatCodes[0]
		}
	} else {
		/* use default format for all columns */
		queryParamsFormatCodes = make([]int16, paramsLen)
		for i := range paramsLen {
			queryParamsFormatCodes[i] = xproto.FormatCodeText
		}
	}

	for rfqn := range rm.Rels {
		// TODO: check by whole RFQN
		ds, err := rm.GetRelationDistribution(ctx, rfqn)
		if err != nil {
			return nil, false, err
		} else if ds.Id == distributions.REPLICATED {
			route = plan.Combine(route, plan.ReferenceRelationState{})
			continue
		}

		krs, err := qr.mgr.ListKeyRanges(ctx, ds.Id)
		if err != nil {
			return nil, false, err
		}

		relation, exists := ds.Relations[rfqn.RelationName]
		if !exists {
			return nil, false, fmt.Errorf("relation %s not found in distribution %s", rfqn.RelationName, ds.Id)
		}

		compositeKey := make([]interface{}, len(relation.DistributionKey))

		// TODO: multi-column routing. This works only for one-dim routing
		for i := range len(relation.DistributionKey) {
			hf, err := hashfunction.HashFunctionByName(relation.DistributionKey[i].HashFunction)
			if err != nil {
				spqrlog.Zero.Debug().Err(err).Msg("failed to resolve hash function")
				break
			}

			col := relation.DistributionKey[i].Column

			vals, valOk := qr.resolveValue(rm, rfqn, col, rm.SPH.BindParams(), queryParamsFormatCodes)

			if !valOk {
				break
			}

			/* TODO: correct support for composite keys here */

			for _, val := range vals {
				compositeKey[i], err = hashfunction.ApplyHashFunction(val, ds.ColTypes[i], hf)
				spqrlog.Zero.Debug().Interface("key", val).Interface("hashed key", compositeKey[i]).Msg("applying hash function on key")

				if err != nil {
					spqrlog.Zero.Debug().Err(err).Msg("failed to apply hash function")
					return nil, false, err
				}

				currroute, err := rm.DeparseKeyWithRangesInternal(ctx, compositeKey, krs)
				if err != nil {
					spqrlog.Zero.Debug().Interface("composite key", compositeKey).Err(err).Msg("encountered the route error")
					return nil, false, err
				}

				spqrlog.Zero.Debug().
					Interface("currroute", currroute).
					Str("table", rfqn.RelationName).
					Msg("calculated route for table/cols")

				route = plan.Combine(route, plan.ShardDispatchPlan{
					ExecTarget:         currroute,
					TargetSessionAttrs: tsa,
				})
			}
		}
	}

	// set up this variable if not yet
	if route == nil {
		route = plan.ScatterPlan{}
	}

	return route, ro, nil
}

// TODO : unit tests
func (qr *ProxyQrouter) Route(ctx context.Context, stmt lyx.Node, sph session.SessionParamsHolder) (plan.Plan, error) {
	meta := rmeta.NewRoutingMetadataContext(sph, qr.mgr)
	route, ro, err := qr.routeWithRules(ctx, meta, stmt)
	if err != nil {
		return nil, err
	}

	switch v := route.(type) {
	case plan.ShardDispatchPlan:
		return v, nil
	case plan.RandomDispatchPlan:
		return v, nil
	case plan.ReferenceRelationState:
		/* check for unroutable here - TODO */
		return plan.RandomDispatchPlan{}, nil
	case plan.DDLState:
		return v, nil
	case plan.CopyState:
		/* temporary */
		return plan.ScatterPlan{}, nil
	case plan.ScatterPlan:
		if sph.EnhancedMultiShardProcessing() {
			if v.SubPlan == nil {
				v.SubPlan, err = planner.PlanDistributedQuery(ctx, meta, stmt)
				if err != nil {
					return nil, err
				}
			}
			return v, nil
		}
		/*
		* Here we have a chance for advanced multi-shard query processing.
		* Try to build distributed plan, else scatter-out.
		 */
		switch strings.ToUpper(sph.DefaultRouteBehaviour()) {
		case "BLOCK":
			return nil, spqrerror.NewByCode(spqrerror.SPQR_NO_DATASHARD)
		case "ALLOW":
			fallthrough
		default:
			if ro {
				/* TODO: config options for this */
				return v, nil
			}
			return nil, spqrerror.NewByCode(spqrerror.SPQR_NO_DATASHARD)
		}
	}
	return nil, rerrors.ErrComplexQuery
}
