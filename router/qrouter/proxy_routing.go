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

func (qr *ProxyQrouter) routingTuples(rm *rmeta.RoutingMetadataContext,
	ds *distributions.Distribution, rfqn rfqn.RelationFQN, relation *distributions.DistributedRelation) ([][]interface{}, error) {
	paramsFormatCodes := rm.SPH.BindParamFormatCodes()
	var queryParamsFormatCodes []int16

	paramsLen := len(rm.SPH.BindParams())

	/* https://github.com/postgres/postgres/blob/c65bc2e1d14a2d4daed7c1921ac518f2c5ac3d17/src/backend/tcop/pquery.c#L664-L691 */ /* #no-spell-check-line */
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

	tuples := make([][]interface{}, 0)

	// TODO: multi-column routing. This works only for one-dim routing
	for i := range len(relation.DistributionKey) {
		hf, err := hashfunction.HashFunctionByName(relation.DistributionKey[i].HashFunction)
		if err != nil {
			spqrlog.Zero.Debug().Err(err).Msg("failed to resolve hash function")
			return nil, err
		}

		col := relation.DistributionKey[i].Column

		vals, valOk := qr.resolveValue(rm, rfqn, col, rm.SPH.BindParams(), queryParamsFormatCodes)

		if !valOk {
			break
		}

		/* TODO: correct support for composite keys here */

		for _, val := range vals {
			compositeKey := make([]interface{}, len(relation.DistributionKey))

			compositeKey[i], err = hashfunction.ApplyHashFunction(val, ds.ColTypes[i], hf)

			if err != nil {
				spqrlog.Zero.Debug().Err(err).Msg("failed to apply hash function")
				return nil, err
			}

			spqrlog.Zero.Debug().Interface("key", val).Interface("hashed key", compositeKey[i]).Msg("applying hash function on key")
			tuples = append(tuples, compositeKey)
		}
	}

	return tuples, nil
}

func (qr *ProxyQrouter) analyzeWhereClause(ctx context.Context, expr lyx.Node, meta *rmeta.RoutingMetadataContext) error {

	spqrlog.Zero.Debug().
		Interface("clause", expr).
		Msg("analyzing select where clause")

	switch texpr := expr.(type) {
	case *lyx.AExprIn:

		switch texpr.Expr.(type) {
		case *lyx.ColumnRef:
			switch q := texpr.SubLink.(type) {
			case *lyx.Select:
				/* TODO properly support subquery here */
				/* SELECT * FROM t WHERE id IN (SELECT 1, 2) */

				if err := qr.AnalyzeQueryV1(ctx, q, meta); err != nil {
					return err
				}
			}
		}

	case *lyx.AExprOp:

		switch lft := texpr.Left.(type) {
		case *lyx.ColumnRef:
			/* simple key-value pair */
			switch right := texpr.Right.(type) {

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
							return qr.analyzeSelectStmt(ctx, argexpr.SubSelect, meta)
						}
					}
				}

			default:
				if texpr.Right != nil {
					if err := qr.analyzeWhereClause(ctx, texpr.Right, meta); err != nil {
						return err
					}
				}
			}

		case *lyx.Select:
			if err := qr.AnalyzeQueryV1(ctx, lft, meta); err != nil {
				return err
			}
		default:
			if texpr.Left != nil {
				if err := qr.analyzeWhereClause(ctx, texpr.Left, meta); err != nil {
					return err
				}
			}
			if texpr.Right != nil {
				if err := qr.analyzeWhereClause(ctx, texpr.Right, meta); err != nil {
					return err
				}
			}
		}
	case *lyx.ColumnRef:
		/* colref = colref case, skip */
	case *lyx.AExprIConst, *lyx.AExprSConst, *lyx.AExprBConst, *lyx.AExprNConst, *lyx.ParamRef:
		/* should not happen */
	case *lyx.AExprEmpty:
		/*skip*/
	case *lyx.Select:
		/* in engine v2 we skip subplans */
	case *lyx.FuncApplication:
		// there are several types of queries like DELETE FROM rel WHERE colref = func_application
		// and func_application is actually routable statement.
		// ANY(ARRAY(subselect)) if one type.

		if strings.ToLower(texpr.Name) == "any" {
			if len(texpr.Args) > 0 {
				// maybe we should consider not only first arg.
				// however, consider only it

				switch argexpr := texpr.Args[0].(type) {
				case *lyx.SubLink:

					// ignore all errors.
					return qr.AnalyzeQueryV1(ctx, argexpr.SubSelect, meta)
				}
			}
		}
	case *lyx.AExprList:
		/* ok */
	default:
		return fmt.Errorf("analyze where clause, unknown expr %T: %w", expr, rerrors.ErrComplexQuery)
	}
	return nil
}

// routeByClause de-parses sharding column-value pair from Where clause of the query
// TODO : unit tests
func (qr *ProxyQrouter) planByWhereClause(ctx context.Context, expr lyx.Node, meta *rmeta.RoutingMetadataContext) (plan.Plan, error) {

	spqrlog.Zero.Debug().
		Interface("clause", expr).
		Msg("planning select where clause")

	var p plan.Plan = nil
	switch texpr := expr.(type) {
	case *lyx.AExprIn:

		switch lft := texpr.Expr.(type) {
		case *lyx.ColumnRef:

			alias, colname := lft.TableAlias, lft.ColName

			switch q := texpr.SubLink.(type) {
			case *lyx.AExprList:
				for _, expr := range q.List {
					if err := qr.processConstExpr(alias, colname, expr, meta); err != nil {
						return nil, err
					}
				}
			case *lyx.Select:
				/* TODO properly support subquery here */
				/* SELECT * FROM t WHERE id IN (SELECT 1, 2) */

				return qr.planQueryV1(ctx, q, meta)
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
					return nil, err
				}

			case *lyx.AExprList:
				for _, expr := range right.List {
					if err := qr.processConstExpr(alias, colname, expr, meta); err != nil {
						return nil, err
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
							return qr.planQueryV1(ctx, argexpr.SubSelect, meta)
						}
					}
				}

			default:
				if texpr.Left != nil {
					if tmp, err := qr.planByWhereClause(ctx, texpr.Left, meta); err != nil {
						return nil, err
					} else {
						p = plan.Combine(p, tmp)
					}
				}
				if texpr.Right != nil {
					if tmp, err := qr.planByWhereClause(ctx, texpr.Right, meta); err != nil {
						return nil, err
					} else {
						p = plan.Combine(p, tmp)
					}
				}
			}
		case *lyx.Select:
			return qr.planQueryV1(ctx, lft, meta)
		default:
			if texpr.Left != nil {
				if tmp, err := qr.planByWhereClause(ctx, texpr.Left, meta); err != nil {
					return nil, err
				} else {
					p = plan.Combine(p, tmp)
				}
			}
			if texpr.Right != nil {
				if tmp, err := qr.planByWhereClause(ctx, texpr.Right, meta); err != nil {
					return nil, err
				} else {
					p = plan.Combine(p, tmp)
				}
			}
		}
	case *lyx.ColumnRef:
		/* colref = colref case, skip */
	case *lyx.AExprIConst, *lyx.AExprSConst, *lyx.AExprBConst, *lyx.AExprNConst, *lyx.ParamRef:
		/* should not happen */
	case *lyx.AExprEmpty:
		/*skip*/
	case *lyx.Select:
		/* in engine v2 we skip subplans */
	case *lyx.FuncApplication:
		// there are several types of queries like DELETE FROM rel WHERE colref = func_application
		// and func_application is actually routable statement.
		// ANY(ARRAY(subselect)) if one type.

		if strings.ToLower(texpr.Name) == "any" {
			if len(texpr.Args) > 0 {
				// maybe we should consider not only first arg.
				// however, consider only it

				switch argexpr := texpr.Args[0].(type) {
				case *lyx.SubLink:

					// ignore all errors.
					return qr.planQueryV1(ctx, argexpr.SubSelect, meta)
				}
			}
		}
	default:
		return nil, fmt.Errorf("route by clause, unknown expr %T: %w", expr, rerrors.ErrComplexQuery)
	}
	return p, nil
}

// TODO : unit tests
func (qr *ProxyQrouter) analyzeSelectStmt(ctx context.Context, selectStmt lyx.Node, meta *rmeta.RoutingMetadataContext) error {

	switch s := selectStmt.(type) {
	case *lyx.Select:
		if clause := s.FromClause; clause != nil {
			// route `insert into rel select from` stmt
			if err := qr.analyzeFromClauseList(ctx, clause, meta); err != nil {
				return err
			}
		}

		if clause := s.Where; clause != nil {
			if err := qr.analyzeWhereClause(ctx, clause, meta); err != nil {
				return err
			}
		}

		if s.LArg != nil {
			if err := qr.analyzeSelectStmt(ctx, s.LArg, meta); err != nil {
				return err
			}
		}
		if s.RArg != nil {
			if err := qr.analyzeSelectStmt(ctx, s.RArg, meta); err != nil {
				return err
			}
		}

		return nil
	/* SELECT * FROM VALUES() ... */
	case *lyx.ValueClause:
		/* random route */
		return nil
	}

	return rerrors.ErrComplexQuery
}

// TODO : unit tests
// analyzes from clause
func (qr *ProxyQrouter) analyzeFromNode(ctx context.Context, node lyx.FromClauseNode, meta *rmeta.RoutingMetadataContext) error {
	spqrlog.Zero.Debug().
		Type("node-type", node).
		Msg("analyzing from node")
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
		if err := qr.analyzeFromNode(ctx, q.Rarg, meta); err != nil {
			return err
		}
		if err := qr.analyzeFromNode(ctx, q.Larg, meta); err != nil {
			return err
		}
	case *lyx.SubSelect:
		return qr.analyzeSelectStmt(ctx, q.Arg, meta)
	default:
		// other cases to consider
		// lateral join, natural, etc
	}

	return nil
}

func (qr *ProxyQrouter) planFromNode(ctx context.Context, node lyx.FromClauseNode, meta *rmeta.RoutingMetadataContext) (plan.Plan, error) {
	spqrlog.Zero.Debug().
		Type("node-type", node).
		Msg("planning from node")

	var p plan.Plan = nil

	switch q := node.(type) {
	case *lyx.RangeVar:
		rqdn := rfqn.RelationFQNFromRangeRangeVar(q)

		// CTE, skip
		if meta.RFQNIsCTE(rqdn) {
			return nil, nil
		}

		if _, err := meta.GetRelationDistribution(ctx, rqdn); err != nil {
			return nil, err
		}

		if _, ok := meta.Rels[rqdn]; !ok {
			meta.Rels[rqdn] = struct{}{}
		}
		if q.Alias != "" {
			/* remember table alias */
			meta.TableAliases[q.Alias] = rfqn.RelationFQNFromRangeRangeVar(q)
		}
	case *lyx.JoinExpr:
		if tmp, err := qr.planFromNode(ctx, q.Rarg, meta); err != nil {
			return nil, err
		} else {
			p = plan.Combine(p, tmp)
		}
		if tmp, err := qr.planFromNode(ctx, q.Larg, meta); err != nil {
			return nil, err
		} else {
			p = plan.Combine(p, tmp)
		}
	case *lyx.SubSelect:
		return qr.planQueryV1(ctx, q.Arg, meta)
	default:
		// other cases to consider
		// lateral join, natural, etc
	}

	return p, nil
}

// TODO : unit tests
func (qr *ProxyQrouter) analyzeFromClauseList(
	ctx context.Context,
	clause []lyx.FromClauseNode, meta *rmeta.RoutingMetadataContext) error {
	for _, node := range clause {
		err := qr.analyzeFromNode(ctx, node, meta)
		if err != nil {
			return err
		}
	}

	return nil
}

func (qr *ProxyQrouter) planFromClauseList(
	ctx context.Context,
	clause []lyx.FromClauseNode, meta *rmeta.RoutingMetadataContext) (plan.Plan, error) {

	var p plan.Plan = nil

	for _, node := range clause {
		tmp, err := qr.planFromNode(ctx, node, meta)
		if err != nil {
			return nil, err
		}
		p = plan.Combine(p, tmp)
	}

	return p, nil
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

func (qr *ProxyQrouter) AnalyzeQueryV1(
	ctx context.Context,
	qstmt lyx.Node,
	rm *rmeta.RoutingMetadataContext) error {

	spqrlog.Zero.Debug().
		Interface("clause", qstmt).
		Msg("AnalyzeQueryV1: enter")

	analyseHelper := func(tr lyx.FromClauseNode) error {
		switch q := tr.(type) {
		case *lyx.RangeVar:
			rqdn := rfqn.RelationFQNFromRangeRangeVar(q)
			if _, err := rm.GetRelationDistribution(ctx, rqdn); err != nil {
				return err
			}
		default:
			return spqrerror.NewByCode(spqrerror.SPQR_NOT_IMPLEMENTED)
		}

		if err := qr.analyzeFromNode(ctx, tr, rm); err != nil {
			return err
		}

		return nil
	}

	switch stmt := qstmt.(type) {
	case *lyx.Select:
		/* We cannot route SQL statements without a FROM clause. However, there are a few cases to consider. */
		if len(stmt.FromClause) == 0 && (stmt.LArg == nil || stmt.RArg == nil) {
			for _, expr := range stmt.TargetList {
				switch e := expr.(type) {
				/* Special cases for SELECT current_schema(), SELECT set_config(...), and SELECT pg_is_in_recovery() */
				case *lyx.FuncApplication:
					for _, innerExp := range e.Args {
						switch iE := innerExp.(type) {
						case *lyx.Select:
							if err := qr.AnalyzeQueryV1(ctx, iE, rm); err != nil {
								return err
							}
						}
					}
				/* Expression like SELECT 1, SELECT 'a', SELECT 1.0, SELECT true, SELECT false */
				case *lyx.AExprIConst, *lyx.AExprSConst, *lyx.AExprNConst, *lyx.AExprBConst:
					return nil

				/* Special case for SELECT current_schema */
				case *lyx.ColumnRef:
					if e.ColName == "current_schema" {
						return nil
					}
				case *lyx.Select:
					if err := qr.AnalyzeQueryV1(ctx, e, rm); err != nil {
						return err
					}
				}
			}
		}

		/*
		 * analyse both recurse branches to populates the FromClause info,
		 */

		if stmt.LArg != nil {
			if err := qr.AnalyzeQueryV1(ctx, stmt.LArg, rm); err != nil {
				return err
			}
		}

		if stmt.RArg != nil {
			if err := qr.AnalyzeQueryV1(ctx, stmt.RArg, rm); err != nil {
				return err
			}
		}

		if stmt.WithClause != nil {
			for _, cte := range stmt.WithClause {
				rm.CteNames[cte.Name] = struct{}{}
				if err := qr.AnalyzeQueryV1(ctx, cte.SubQuery, rm); err != nil {
					return err
				}
			}
		}

		if stmt.FromClause != nil {
			// collect table alias names, if any
			// for single-table queries, process as usual
			if err := qr.analyzeFromClauseList(ctx, stmt.FromClause, rm); err != nil {
				return err
			}
		}

		/* XXX: analyse where clause here, because it can contain col op subselect patterns */

		if stmt.Where != nil {
			return qr.analyzeWhereClause(ctx, stmt.Where, rm)
		}
		return nil
	case *lyx.Insert:
		if err := analyseHelper(stmt.TableRef); err != nil {
			return err
		}
		if selectStmt := stmt.SubSelect; selectStmt != nil {
			switch subS := selectStmt.(type) {
			case *lyx.Select:
				spqrlog.Zero.Debug().Msg("analyze insert stmt on select clause")
				return qr.AnalyzeQueryV1(ctx, subS, rm)
			default:
				return nil
			}
		}

		return nil
	case *lyx.Update:
		if err := analyseHelper(stmt.TableRef); err != nil {
			return err
		}

		clause := stmt.Where
		if clause == nil {
			return nil
		}

		return qr.AnalyzeQueryV1(ctx, clause, rm)

	case *lyx.Delete:
		if err := analyseHelper(stmt.TableRef); err != nil {
			return err
		}

		clause := stmt.Where
		if clause == nil {
			return nil
		}

		return qr.AnalyzeQueryV1(ctx, clause, rm)
	}
	return nil
}

// TODO : unit tests
// May return nil routing state here - thats ok
func (qr *ProxyQrouter) planQueryV1(
	ctx context.Context,
	qstmt lyx.Node,
	rm *rmeta.RoutingMetadataContext) (plan.Plan, error) {
	switch stmt := qstmt.(type) {
	case *lyx.Select:

		var p plan.Plan

		/* We cannot route SQL statements without a FROM clause. However, there are a few cases to consider. */
		if len(stmt.FromClause) == 0 && (stmt.LArg == nil || stmt.RArg == nil) {
			for _, expr := range stmt.TargetList {
				switch e := expr.(type) {
				/* Special cases for SELECT current_schema(), SELECT set_config(...), and SELECT pg_is_in_recovery() */
				case *lyx.FuncApplication:

					/* for queries, that need to access data on shard, ignore these "virtual" func.	 */
					if e.Name == "current_schema" || e.Name == "set_config" || e.Name == "pg_is_in_recovery" || e.Name == "version" {
						return plan.RandomDispatchPlan{}, nil
					}
					for _, innerExp := range e.Args {
						switch iE := innerExp.(type) {
						case *lyx.Select:
							if tmp, err := qr.planQueryV1(ctx, iE, rm); err != nil {
								return nil, err
							} else {
								p = plan.Combine(p, tmp)
							}
						}
					}
				/* Expression like SELECT 1, SELECT 'a', SELECT 1.0, SELECT true, SELECT false */
				case *lyx.AExprIConst, *lyx.AExprSConst, *lyx.AExprNConst, *lyx.AExprBConst:
					return plan.RandomDispatchPlan{}, nil

				/* Special case for SELECT current_schema */
				case *lyx.ColumnRef:
					if e.ColName == "current_schema" {
						return plan.RandomDispatchPlan{}, nil
					}
				case *lyx.Select:

					if tmp, err := qr.planQueryV1(ctx, e, rm); err != nil {
						return nil, err
					} else {
						p = plan.Combine(p, tmp)
					}
				}
			}

		}
		/*
		 * Then try to route  both branches
		 */

		if stmt.LArg != nil {
			if tmp, err := qr.planQueryV1(ctx, stmt.LArg, rm); err != nil {
				return nil, err
			} else {
				p = plan.Combine(p, tmp)
			}
		}
		if stmt.RArg != nil {
			if tmp, err := qr.planQueryV1(ctx, stmt.RArg, rm); err != nil {
				return nil, err

			} else {
				p = plan.Combine(p, tmp)
			}
		}

		if stmt.WithClause != nil {
			for _, cte := range stmt.WithClause {
				if tmp, err := qr.planQueryV1(ctx, cte.SubQuery, rm); err != nil {
					return nil, err
				} else {
					p = plan.Combine(p, tmp)
				}
			}
		}

		if stmt.FromClause != nil {
			// collect table alias names, if any
			// for single-table queries, process as usual
			if tmp, err := qr.planFromClauseList(ctx, stmt.FromClause, rm); err != nil {
				return nil, err
			} else {
				p = plan.Combine(p, tmp)
			}
		}

		if stmt.Where != nil {
			/* return plan from where clause and route on it */
			/*  SELECT stmts, which would be routed with their WHERE clause */
			tmp, err := qr.planByWhereClause(ctx, stmt.Where, rm)
			if err != nil {
				return nil, err
			}
			p = plan.Combine(p, tmp)
		}

		return p, nil

	case *lyx.Insert:
		var p plan.Plan = nil
		if selectStmt := stmt.SubSelect; selectStmt != nil {

			insertCols := stmt.Columns

			var routingList [][]lyx.Node

			switch subS := selectStmt.(type) {
			case *lyx.Select:
				spqrlog.Zero.Debug().Msg("routing insert stmt on select clause")
				_ = qr.analyzeSelectStmt(ctx, subS, rm)

				p, _ = qr.planQueryV1(ctx, subS, rm)

				/* try target list */
				spqrlog.Zero.Debug().Msg("routing insert stmt on target list")
				/* this target list for some insert (...) sharding column */

				routingList = [][]lyx.Node{subS.TargetList}
				/* record all values from tl */

			case *lyx.ValueClause:
				/* record all values from values scan */
				routingList = subS.Values
			default:
				return p, nil
			}

			offsets, rfqn, state, err := qr.processInsertFromSelectOffsets(ctx, stmt, rm)
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
						_ = qr.processConstExprOnRFQN(rfqn, insertCols[offsets[i]], projectionList(routingList, offsets[i]), rm)
					}
				}
			case plan.ReferenceRelationState:
				if rm.SPH.EnhancedMultiShardProcessing() {
					return planner.PlanDistributedQuery(ctx, rm, stmt)
				}
				return nil, spqrerror.NewByCode(spqrerror.SPQR_NOT_IMPLEMENTED)
			}
		}

		return p, nil
	case *lyx.Update:
		clause := stmt.Where
		if clause == nil {
			return nil, nil
		}

		switch q := stmt.TableRef.(type) {
		case *lyx.RangeVar:

			rqdn := rfqn.RelationFQNFromRangeRangeVar(q)

			if d, err := rm.GetRelationDistribution(ctx, rqdn); err != nil {
				return nil, err
			} else if d.Id == distributions.REPLICATED {
				if rm.SPH.EnhancedMultiShardProcessing() {
					return planner.PlanDistributedQuery(ctx, rm, stmt)
				}
				return nil, spqrerror.NewByCode(spqrerror.SPQR_NOT_IMPLEMENTED)
			}
		default:
			return nil, spqrerror.NewByCode(spqrerror.SPQR_NOT_IMPLEMENTED)
		}

		return qr.planByWhereClause(ctx, clause, rm)
	case *lyx.Delete:
		clause := stmt.Where
		if clause == nil {
			return nil, nil
		}

		switch q := stmt.TableRef.(type) {
		case *lyx.RangeVar:

			rqdn := rfqn.RelationFQNFromRangeRangeVar(q)

			if d, err := rm.GetRelationDistribution(ctx, rqdn); err != nil {
				return nil, err
			} else if d.Id == distributions.REPLICATED {
				if rm.SPH.EnhancedMultiShardProcessing() {
					return planner.PlanDistributedQuery(ctx, rm, stmt)
				}
				return nil, spqrerror.NewByCode(spqrerror.SPQR_NOT_IMPLEMENTED)
			}
		default:
			return nil, spqrerror.NewByCode(spqrerror.SPQR_NOT_IMPLEMENTED)
		}

		return qr.planByWhereClause(ctx, clause, rm)
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
	if len(paramResCodes) < ind {
		return nil, false
	}
	fc := paramResCodes[ind]

	singleVal, res := plan.ParseResolveParamValue(fc, ind, tp, bindParams)

	return []any{singleVal}, res
}

func (qr *ProxyQrouter) routeByTuples(ctx context.Context, rm *rmeta.RoutingMetadataContext, tsa string) (plan.Plan, error) {

	var route plan.Plan
	/*
	 * Step 2: traverse all aggregated relation distribution tuples and route on them.
	 */

	for rfqn := range rm.Rels {
		// TODO: check by whole RFQN
		ds, err := rm.GetRelationDistribution(ctx, rfqn)
		if err != nil {
			return nil, err
		} else if ds.Id == distributions.REPLICATED {
			route = plan.Combine(route, plan.ReferenceRelationState{})
			continue
		}

		krs, err := qr.mgr.ListKeyRanges(ctx, ds.Id)
		if err != nil {
			return nil, err
		}

		relation, exists := ds.Relations[rfqn.RelationName]
		if !exists {
			return nil, fmt.Errorf("relation %s not found in distribution %s", rfqn.RelationName, ds.Id)
		}

		rt, err := qr.routingTuples(rm, ds, rfqn, relation)
		if err != nil {
			return nil, err
		}

		// TODO: multi-column routing. This works only for one-dim routing
		for _, tup := range rt {

			currroute, err := rm.DeparseKeyWithRangesInternal(ctx, tup, krs)
			if err != nil {
				spqrlog.Zero.Debug().Interface("composite key", tup).Err(err).Msg("encountered the route error")
				return nil, err
			}

			spqrlog.Zero.Debug().
				Interface("currntroute", currroute).
				Str("table", rfqn.RelationName).
				Msg("calculated route for table/cols")

			route = plan.Combine(route, plan.ShardDispatchPlan{
				ExecTarget:         currroute,
				TargetSessionAttrs: tsa,
			})
		}
	}

	return route, nil
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
		if err := qr.AnalyzeQueryV1(ctx, stmt, rm); err != nil {
			return nil, false, err
		}

		rs, err := qr.planQueryV1(ctx, stmt, rm)
		if err != nil {
			return nil, false, err
		}

		ro = false

		route = plan.Combine(route, rs)
	case *lyx.Select:

		err := qr.AnalyzeQueryV1(ctx, stmt, rm)
		if err != nil {
			spqrlog.Zero.Debug().Err(err).Msg("failed to analyze select query")
			return nil, false, err
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

		p, err := qr.planQueryV1(ctx, stmt, rm)

		if err != nil {
			return nil, false, err
		}

		route = plan.Combine(route, p)

	case *lyx.Delete, *lyx.Update:
		if err := qr.AnalyzeQueryV1(ctx, stmt, rm); err != nil {
			return nil, false, err
		}
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

	tmp, err := qr.routeByTuples(ctx, rm, tsa)
	if err != nil {
		return nil, false, err
	}

	route = plan.Combine(route, tmp)

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
