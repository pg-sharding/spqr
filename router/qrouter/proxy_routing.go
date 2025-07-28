package qrouter

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/pg-sharding/spqr/pkg/catalog"
	"github.com/pg-sharding/spqr/pkg/models/distributions"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/pkg/models/spqrerror"
	"github.com/pg-sharding/spqr/pkg/tsa"

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

func (qr *ProxyQrouter) processConstExprOnRFQN(resolvedRelation *rfqn.RelationFQN, colname string, exprs []lyx.Node, meta *rmeta.RoutingMetadataContext) error {
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

func (qr *ProxyQrouter) routingTuples(ctx context.Context, rm *rmeta.RoutingMetadataContext,
	ds *distributions.Distribution, qualName *rfqn.RelationFQN, relation *distributions.DistributedRelation, tsa tsa.TSA) (plan.Plan, error) {

	queryParamsFormatCodes := planner.GetParams(rm)

	krs, err := qr.mgr.ListKeyRanges(ctx, ds.Id)
	if err != nil {
		return nil, err
	}

	var rec func(lvl int) error
	var p plan.Plan = nil

	compositeKey := make([]any, len(relation.DistributionKey))

	rec = func(lvl int) error {

		hf, err := hashfunction.HashFunctionByName(relation.DistributionKey[lvl].HashFunction)
		if err != nil {
			spqrlog.Zero.Debug().Err(err).Msg("failed to resolve hash function")
			return err
		}

		col := relation.DistributionKey[lvl].Column

		vals, valOk := rm.ResolveValue(qualName, col, queryParamsFormatCodes)

		if !valOk {
			return nil
		}

		/* TODO: correct support for composite keys here */

		for _, val := range vals {

			compositeKey[lvl], err = hashfunction.ApplyHashFunction(val, ds.ColTypes[lvl], hf)

			if err != nil {
				spqrlog.Zero.Debug().Err(err).Msg("failed to apply hash function")
				return err
			}

			spqrlog.Zero.Debug().Interface("key", val).Interface("hashed key", compositeKey[lvl]).Msg("applying hash function on key")
			if lvl+1 == len(relation.DistributionKey) {

				currroute, err := rm.DeparseKeyWithRangesInternal(ctx, compositeKey, krs)
				if err != nil {
					spqrlog.Zero.Debug().Interface("composite key", compositeKey).Err(err).Msg("encountered the route error")
					return err
				}

				spqrlog.Zero.Debug().
					Interface("current route", currroute).
					Str("table", qualName.RelationName).
					Msg("calculated route for table/cols")

				p = plan.Combine(p, &plan.ShardDispatchPlan{
					ExecTarget:         currroute,
					TargetSessionAttrs: tsa,
				})

			} else {
				if err := rec(lvl + 1); err != nil {
					return err
				}
			}
		}

		return nil
	}

	if err := rec(0); err != nil {
		return nil, err
	}

	return p, nil
}

func (qr *ProxyQrouter) analyzeWhereClause(ctx context.Context, expr lyx.Node, meta *rmeta.RoutingMetadataContext) error {

	spqrlog.Zero.Debug().
		Interface("clause", expr).
		Msg("analyzing select where clause")

	switch texpr := expr.(type) {
	/* lyx.ResTarget is unexpected here */
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
func (qr *ProxyQrouter) planByQualExpr(ctx context.Context, expr lyx.Node, meta *rmeta.RoutingMetadataContext) (plan.Plan, error) {

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
		/* lyx.ResTarget is unexpected here */
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

			case *lyx.ColumnRef:
				/* colref = colref case, skip, expect when we know exact value of ColumnRef */
				for _, v := range meta.AuxExprByColref(right) {
					if err := qr.processConstExpr(alias, colname, v, meta); err != nil {
						return nil, err
					}
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
					if tmp, err := qr.planByQualExpr(ctx, texpr.Left, meta); err != nil {
						return nil, err
					} else {
						p = plan.Combine(p, tmp)
					}
				}
				if texpr.Right != nil {
					if tmp, err := qr.planByQualExpr(ctx, texpr.Right, meta); err != nil {
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
				if tmp, err := qr.planByQualExpr(ctx, texpr.Left, meta); err != nil {
					return nil, err
				} else {
					p = plan.Combine(p, tmp)
				}
			}
			if texpr.Right != nil {
				if tmp, err := qr.planByQualExpr(ctx, texpr.Right, meta); err != nil {
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
	/* functional table expressions */
	case *lyx.FuncApplication:
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
		if err := qr.processRangeNode(ctx, meta, q); err != nil {
			return err
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

func (qr *ProxyQrouter) processRangeNode(ctx context.Context, meta *rmeta.RoutingMetadataContext, q *lyx.RangeVar) error {
	qualName := rfqn.RelationFQNFromRangeRangeVar(q)

	// CTE, skip
	if meta.RFQNIsCTE(qualName) {
		/* remember cte alias */
		meta.CTEAliases[q.Alias] = qualName.RelationName
		return nil
	}

	if _, err := meta.GetRelationDistribution(ctx, qualName); err != nil {
		return err
	}

	if _, ok := meta.Rels[*qualName]; !ok {
		meta.Rels[*qualName] = struct{}{}
	}
	if q.Alias != "" {
		/* remember table alias */
		meta.TableAliases[q.Alias] = *rfqn.RelationFQNFromRangeRangeVar(q)
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

		if err := qr.processRangeNode(ctx, meta, q); err != nil {
			return nil, err
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

		if q.JoinQual != nil {
			if tmp, err := qr.planByQualExpr(ctx, q.JoinQual, meta); err != nil {
				return nil, err
			} else {
				p = plan.Combine(p, tmp)
			}
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

		if stmt.WithClause != nil {
			for _, cte := range stmt.WithClause {
				rm.CteNames[cte.Name] = struct{}{}
				if err := qr.AnalyzeQueryV1(ctx, cte.SubQuery, rm); err != nil {
					return err
				}
			}
		}

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

		if stmt.WithClause != nil {
			for _, cte := range stmt.WithClause {
				rm.CteNames[cte.Name] = struct{}{}
				if err := qr.AnalyzeQueryV1(ctx, cte.SubQuery, rm); err != nil {
					return err
				}
			}
		}

		if err := analyseHelper(stmt.TableRef); err != nil {
			return err
		}

		clause := stmt.Where
		if clause == nil {
			return nil
		}

		return qr.AnalyzeQueryV1(ctx, clause, rm)

	case *lyx.Delete:

		if stmt.WithClause != nil {
			for _, cte := range stmt.WithClause {
				rm.CteNames[cte.Name] = struct{}{}
				if err := qr.AnalyzeQueryV1(ctx, cte.SubQuery, rm); err != nil {
					return err
				}
			}
		}

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

func (qr *ProxyQrouter) planWithClauseV1(ctx context.Context, rm *rmeta.RoutingMetadataContext, WithClause []*lyx.CommonTableExpr) (plan.Plan, error) {
	var p plan.Plan
	for _, cte := range WithClause {
		switch qq := cte.SubQuery.(type) {
		case *lyx.ValueClause:
			/* special case */
			if len(qq.Values) > 0 {
				for i, name := range cte.NameList {
					if i < len(cte.NameList) && i < len(qq.Values[0]) {
						/* XXX: currently only one-tuple aux values supported */
						rm.RecordAuxExpr(cte.Name, name, qq.Values[0][i])
					}
				}
			}
		default:
			if tmp, err := qr.planQueryV1(ctx, cte.SubQuery, rm); err != nil {
				return nil, err
			} else {
				p = plan.Combine(p, tmp)
			}
		}
	}

	return p, nil
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
			virtualRowCols := []pgproto3.FieldDescription{}
			virtualRowVals := [][]byte{}

			for _, expr := range stmt.TargetList {
				actualExpr := expr
				colname := "?column?"
				if rt, ok := expr.(*lyx.ResTarget); ok {
					actualExpr = rt.Value
					colname = rt.Name
				}

				switch e := actualExpr.(type) {
				case *lyx.SVFOP_CURRENT_USER:
					p = plan.Combine(p, &plan.VirtualPlan{})
					virtualRowCols = append(virtualRowCols,
						pgproto3.FieldDescription{
							Name:                 []byte(colname),
							DataTypeOID:          catalog.TEXTOID,
							TypeModifier:         -1,
							DataTypeSize:         -1,
							TableAttributeNumber: 0,
							TableOID:             0,
							Format:               0,
						})

					virtualRowVals = append(virtualRowVals, []byte(rm.SPH.Usr()))

				case *lyx.AExprNot:
					/* inspect our arg. If this is pg_is_in_recovery, apply NOT */
					switch arg := e.Arg.(type) {
					case *lyx.FuncApplication:
						if arg.Name == "pg_is_in_recovery" {
							p = plan.Combine(p, &plan.VirtualPlan{})
							virtualRowCols = append(virtualRowCols,
								pgproto3.FieldDescription{
									Name:                 []byte("pg_is_in_recovery"),
									DataTypeOID:          catalog.ARRAYOID,
									TypeModifier:         -1,
									DataTypeSize:         1,
									TableAttributeNumber: 0,
									TableOID:             0,
									Format:               0,
								})

							/* notice this sign */
							if rm.SPH.GetTsa() != config.TargetSessionAttrsRW {
								virtualRowVals = append(virtualRowVals, []byte{byte('f')})
							} else {
								virtualRowVals = append(virtualRowVals, []byte{byte('t')})
							}
							continue
						}
					}
				/* Special cases for SELECT current_schema(), SELECT set_config(...), and SELECT pg_is_in_recovery() */
				case *lyx.FuncApplication:

					/* for queries, that need to access data on shard, ignore these "virtual" func.	 */
					if e.Name == "pg_is_in_recovery" {
						p = plan.Combine(p, &plan.VirtualPlan{})
						virtualRowCols = append(virtualRowCols,
							pgproto3.FieldDescription{
								Name:                 []byte("pg_is_in_recovery"),
								DataTypeOID:          catalog.ARRAYOID,
								TypeModifier:         -1,
								DataTypeSize:         1,
								TableAttributeNumber: 0,
								TableOID:             0,
								Format:               0,
							})

						if rm.SPH.GetTsa() == config.TargetSessionAttrsRW {
							virtualRowVals = append(virtualRowVals, []byte{byte('f')})
						} else {
							virtualRowVals = append(virtualRowVals, []byte{byte('t')})
						}
						continue
					} else if e.Name == "spqr_is_opened" {
						p = plan.Combine(p, &plan.VirtualPlan{})
						virtualRowCols = append(virtualRowCols,
							pgproto3.FieldDescription{
								Name:                 []byte("spqr_is_opened"),
								DataTypeOID:          catalog.ARRAYOID,
								TypeModifier:         -1,
								DataTypeSize:         1,
								TableAttributeNumber: 0,
								TableOID:             0,
								Format:               0,
							})

						if qr.IsOpened() {
							virtualRowVals = append(virtualRowVals, []byte{byte('t')})
						} else {
							virtualRowVals = append(virtualRowVals, []byte{byte('f')})
						}
						continue
					} else if e.Name == "current_setting" && len(e.Args) == 1 {
						if val, ok := e.Args[0].(*lyx.AExprSConst); ok && val.Value == "transaction_read_only" {
							p = plan.Combine(p, &plan.VirtualPlan{})
							virtualRowCols = append(virtualRowCols,
								pgproto3.FieldDescription{
									Name:                 []byte("current_setting"),
									DataTypeOID:          catalog.ARRAYOID,
									TypeModifier:         -1,
									DataTypeSize:         1,
									TableAttributeNumber: 0,
									TableOID:             0,
									Format:               0,
								})

							if rm.SPH.GetTsa() == config.TargetSessionAttrsRW {
								virtualRowVals = append(virtualRowVals, []byte{byte('f')})
							} else {
								virtualRowVals = append(virtualRowVals, []byte{byte('t')})
							}
							continue
						}
					}

					if e.Name == "current_schema" || e.Name == "now" || e.Name == "set_config" || e.Name == "version" || e.Name == "current_setting" {
						p = plan.Combine(p, &plan.RandomDispatchPlan{})
						continue
					}
					deduced := false
					for _, innerExp := range e.Args {
						switch iE := innerExp.(type) {
						case *lyx.Select:
							if tmp, err := qr.planQueryV1(ctx, iE, rm); err != nil {
								return nil, err
							} else {
								p = plan.Combine(p, tmp)
								deduced = true
							}
						}
					}
					if !deduced {
						/* very questionable. */
						p = plan.Combine(p, &plan.RandomDispatchPlan{})
					}
				/* Expression like SELECT 1, SELECT 'a', SELECT 1.0, SELECT true, SELECT false */
				case *lyx.AExprSConst:

					p = plan.Combine(p, &plan.VirtualPlan{})
					virtualRowCols = append(virtualRowCols,
						pgproto3.FieldDescription{
							Name:                 []byte(colname),
							DataTypeOID:          catalog.TEXTOID,
							TypeModifier:         -1,
							DataTypeSize:         -1,
							TableAttributeNumber: 0,
							TableOID:             0,
							Format:               0,
						})

					virtualRowVals = append(virtualRowVals, []byte(e.Value))

				case *lyx.AExprIConst:

					p = plan.Combine(p, &plan.VirtualPlan{})
					virtualRowCols = append(virtualRowCols,
						pgproto3.FieldDescription{
							Name:                 []byte(colname),
							DataTypeOID:          catalog.INT4OID,
							TypeModifier:         -1,
							DataTypeSize:         4,
							TableAttributeNumber: 0,
							TableOID:             0,
							Format:               0,
						})

					virtualRowVals = append(virtualRowVals, []byte(fmt.Sprintf("%d", e.Value)))
				case *lyx.AExprNConst, *lyx.AExprBConst:
					p = plan.Combine(p, &plan.RandomDispatchPlan{})

				/* Special case for SELECT current_schema */
				case *lyx.ColumnRef:
					if e.ColName == "current_schema" {
						p = plan.Combine(p, &plan.RandomDispatchPlan{})
					}
				case *lyx.Select:

					if tmp, err := qr.planQueryV1(ctx, e, rm); err != nil {
						return nil, err
					} else {
						p = plan.Combine(p, tmp)
					}
				}
			}

			switch q := p.(type) {
			case *plan.VirtualPlan:
				q.VirtualRowCols = virtualRowCols
				q.VirtualRowVals = virtualRowVals

				return q, nil
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

		tmp, err := qr.planWithClauseV1(ctx, rm, stmt.WithClause)
		if err != nil {
			return nil, err
		}

		p = plan.Combine(p, tmp)

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
			tmp, err := qr.planByQualExpr(ctx, stmt.Where, rm)
			if err != nil {
				return nil, err
			}
			switch tmp.(type) {
			case *plan.VirtualPlan:
				if stmt.FromClause != nil {
					/* de-virtualize */
					tmp = nil
				}
			}
			p = plan.Combine(p, tmp)
		}

		return p, nil

	case *lyx.Insert:

		p, err := qr.planWithClauseV1(ctx, rm, stmt.WithClause)
		if err != nil {
			return nil, err
		}
		selectStmt := stmt.SubSelect
		if selectStmt == nil {
			return p, nil
		}

		var routingList [][]lyx.Node

		switch subS := selectStmt.(type) {
		case *lyx.Select:
			spqrlog.Zero.Debug().Msg("routing insert stmt on select clause")

			p, _ = qr.planQueryV1(ctx, subS, rm)

			/* try target list */
			spqrlog.Zero.Debug().Msgf("routing insert stmt on target list:%T", p)
			/* this target list for some insert (...) sharding column */

			routingList = [][]lyx.Node{subS.TargetList}
			/* record all values from tl */

			switch rf := stmt.TableRef.(type) {
			case *lyx.RangeVar:

				qualName := rfqn.RelationFQNFromRangeRangeVar(rf)

				if rs, err := rm.IsReferenceRelation(ctx, rf); err != nil {
					return nil, err
				} else if rs {
					rel, err := rm.Mgr.GetReferenceRelation(ctx, qualName)
					if err != nil {
						return nil, err
					}
					if len(rel.ColumnSequenceMapping) == 0 {
						// ok
						if p == nil {
							return &plan.ScatterPlan{
								ExecTargets: rel.ListStorageRoutes(),
							}, nil
						}
						// XXX: todo - check that sub select is not doing anything insane
						switch p.(type) {
						case *plan.VirtualPlan, *plan.ScatterPlan, *plan.RandomDispatchPlan:
							if stmt.Returning != nil {
								return &plan.DataRowFilter{
									SubPlan: &plan.ScatterPlan{
										ExecTargets: rel.ListStorageRoutes(),
									},
									FilterIndex: 0,
								}, nil
							}
							return &plan.ScatterPlan{
								ExecTargets: rel.ListStorageRoutes(),
							}, nil
						default:
							return nil, rerrors.ErrComplexQuery
						}
					}
					return nil, rerrors.ErrComplexQuery
				} else {
					shs, err := planner.PlanDistributedRelationInsert(ctx, routingList, rm, stmt)
					if err != nil {
						return nil, err
					}
					for _, sh := range shs {
						if sh.Name != shs[0].Name {
							return nil, rerrors.ErrComplexQuery
						}
					}
					if len(shs) > 0 {
						p = plan.Combine(p, &plan.ShardDispatchPlan{
							ExecTarget:         shs[0],
							TargetSessionAttrs: config.TargetSessionAttrsRW,
						})
					}
					return p, nil
				}
			default:
				return nil, rerrors.ErrComplexQuery
			}

		case *lyx.ValueClause:
			/* record all values from values scan */
			routingList = subS.Values

			switch rf := stmt.TableRef.(type) {
			case *lyx.RangeVar:
				if rs, err := rm.IsReferenceRelation(ctx, rf); err != nil {
					return nil, err
				} else if rs {
					/* If reference relation, use planner v2 */
					p, err := planner.PlanReferenceRelationInsertValues(ctx, qr.query, rm, stmt.Columns, rf, subS)
					if err != nil {
						return nil, err
					}
					if stmt.Returning != nil {
						return &plan.DataRowFilter{
							SubPlan:     p,
							FilterIndex: 0,
						}, nil
					}
					return p, nil
				} else {
					shs, err := planner.PlanDistributedRelationInsert(ctx, routingList, rm, stmt)
					if err != nil {
						return nil, err
					}
					/* XXX: give change for engine v2 to rewrite queries */
					for _, sh := range shs {
						if sh.Name != shs[0].Name {
							return nil, rerrors.ErrComplexQuery
						}
					}

					if len(shs) > 0 {
						p = plan.Combine(p, &plan.ShardDispatchPlan{
							ExecTarget:         shs[0],
							TargetSessionAttrs: config.TargetSessionAttrsRW,
						})
					}
					return p, nil
				}
			default:
				return nil, rerrors.ErrComplexQuery
			}

		default:
			return p, nil
		}

	case *lyx.Update:

		p, err := qr.planWithClauseV1(ctx, rm, stmt.WithClause)
		if err != nil {
			return nil, err
		}

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
					tmp, err := planner.PlanDistributedQuery(ctx, rm, stmt)
					if err != nil {
						return nil, err
					}
					p = plan.Combine(p, tmp)
					return p, nil
				}
				return nil, spqrerror.NewByCode(spqrerror.SPQR_NOT_IMPLEMENTED)
			}
		default:
			return nil, spqrerror.NewByCode(spqrerror.SPQR_NOT_IMPLEMENTED)
		}

		tmp, err := qr.planByQualExpr(ctx, clause, rm)
		if err != nil {
			return nil, err
		}
		switch tmp.(type) {
		case *plan.VirtualPlan:
			if stmt.TableRef != nil {
				/* de-virtualize */
				tmp = nil
			}
		}
		p = plan.Combine(p, tmp)
		return p, nil
	case *lyx.Delete:

		p, err := qr.planWithClauseV1(ctx, rm, stmt.WithClause)
		if err != nil {
			return nil, err
		}

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
					tmp, err := planner.PlanDistributedQuery(ctx, rm, stmt)
					if err != nil {
						return nil, err
					}
					p = plan.Combine(p, tmp)
					return p, nil
				}
				return nil, spqrerror.NewByCode(spqrerror.SPQR_NOT_IMPLEMENTED)
			}
		default:
			return nil, spqrerror.NewByCode(spqrerror.SPQR_NOT_IMPLEMENTED)
		}

		tmp, err := qr.planByQualExpr(ctx, clause, rm)
		if err != nil {
			return nil, err
		}
		switch tmp.(type) {
		case *plan.VirtualPlan:
			if stmt.TableRef != nil {
				/* de-virtualize */
				tmp = nil
			}
		}
		p = plan.Combine(p, tmp)
		return p, nil
	}

	return nil, nil
}

// CheckTableIsRoutable Given table create statement, check if it is routable with some sharding rule
// TODO : unit tests
func (qr *ProxyQrouter) CheckTableIsRoutable(ctx context.Context, node *lyx.CreateTable) error {
	var err error
	var ds *distributions.Distribution
	var relname *rfqn.RelationFQN

	if node.PartitionOf != nil {
		switch q := node.PartitionOf.(type) {
		case *lyx.RangeVar:
			relname := rfqn.RelationFQNFromRangeRangeVar(q)
			_, err = qr.mgr.GetRelationDistribution(ctx, relname)
			return err
		default:
			return fmt.Errorf("partition of is not a range var")
		}
	}

	switch q := node.TableRv.(type) {
	case *lyx.RangeVar:
		relname = rfqn.RelationFQNFromRangeRangeVar(q)
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
	rel, ok := ds.TryGetRelation(relname)
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

func (qr *ProxyQrouter) routeByTuples(ctx context.Context, rm *rmeta.RoutingMetadataContext, tsa tsa.TSA) (plan.Plan, error) {

	var queryPlan plan.Plan
	/*
	 * Step 2: traverse all aggregated relation distribution tuples and route on them.
	 */

	for qualName := range rm.Rels {
		// TODO: check by whole RFQN
		ds, err := rm.GetRelationDistribution(ctx, &qualName)
		if err != nil {
			return nil, err
		} else if ds.Id == distributions.REPLICATED {
			var shs []kr.ShardKey
			if rmeta.IsRelationCatalog(&qualName) {
				shs = nil
			} else {
				r, err := rm.Mgr.GetReferenceRelation(ctx, &qualName)
				if err != nil {
					return nil, err
				}
				shs = r.ListStorageRoutes()
			}

			queryPlan = plan.Combine(queryPlan, &plan.RandomDispatchPlan{
				ExecTargets: shs,
			})
			continue
		}

		relation, exists := ds.TryGetRelation(&qualName)
		if !exists {
			return nil, fmt.Errorf("relation %s not found in distribution %s", qualName.RelationName, ds.Id)
		}

		tmp, err := qr.routingTuples(ctx, rm, ds, &qualName, relation, tsa)
		if err != nil {
			return nil, err
		}

		queryPlan = plan.Combine(queryPlan, tmp)
	}

	return queryPlan, nil
}

// Returns state, is read-only flag and err if any
func (qr *ProxyQrouter) RouteWithRules(ctx context.Context, rm *rmeta.RoutingMetadataContext, stmt lyx.Node, tsa tsa.TSA) (plan.Plan, bool, error) {
	if stmt == nil {
		// empty statement
		return &plan.RandomDispatchPlan{}, false, nil
	}

	rh, err := rm.ResolveRouteHint(ctx)

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
		return &plan.ScatterPlan{
			ExecTargets: qr.DataShardsRoutes(),
		}, true, nil
	}

	/*
	* Currently, deparse only first query from multi-statement query msg (Enhance)
	 */

	/* TODO: delay this until step 2. */

	var pl plan.Plan
	pl = nil

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
		return &plan.RandomDispatchPlan{}, true, nil

	case *lyx.VariableShowStmt:
		/*
		 if we want to reroute to execute this stmt, route to random shard
		 XXX: support intelligent show support, without direct query dispatch
		*/
		return &plan.RandomDispatchPlan{}, true, nil

	// XXX: need alter table which renames sharding column to non-sharding column check
	case *lyx.CreateSchema:
		return &plan.ScatterPlan{
			IsDDL: true,
		}, false, nil
	case *lyx.CreateExtension:
		return &plan.ScatterPlan{
			IsDDL: true,
		}, false, nil
	case *lyx.Grant:
		return &plan.ScatterPlan{
			IsDDL: true,
		}, false, nil
	case *lyx.CreateTable:
		ds, err := planner.PlanCreateTable(ctx, rm, node)
		if err != nil {
			return nil, false, err
		}
		/*
		 * Disallow to create table which does not contain any sharding column
		 */
		if err := qr.CheckTableIsRoutable(ctx, node); err != nil {
			return nil, false, err
		}
		return ds, false, nil
	case *lyx.Vacuum:
		/* Send vacuum to each shard */
		return &plan.ScatterPlan{
			IsDDL: true,
		}, false, nil
	case *lyx.Analyze:
		/* Send vacuum to each shard */
		return &plan.ScatterPlan{
			IsDDL: true,
		}, false, nil
	case *lyx.Cluster:
		/* Send vacuum to each shard */
		return &plan.ScatterPlan{
			IsDDL: true,
		}, false, nil
	case *lyx.Index:
		/*
		 * Disallow to index on table which does not contain any sharding column
		 */
		// XXX: do it
		return &plan.ScatterPlan{
			IsDDL: true,
		}, false, nil

	case *lyx.Alter, *lyx.Drop, *lyx.Truncate:
		// support simple ddl commands, route them to every chard
		// this is not fully ACID (not atomic at least)
		return &plan.ScatterPlan{
			IsDDL: true,
		}, false, nil
		/*
			 case *pgquery.Node_DropdbStmt, *pgquery.Node_DropRoleStmt:
				 // forbid under separate setting
				 return MultiMatchState{}, nil
		*/
	case *lyx.CreateRole, *lyx.CreateDatabase:
		// forbid under separate setting
		return &plan.ScatterPlan{
			IsDDL: true,
		}, false, nil
	case *lyx.Insert:
		if err := qr.AnalyzeQueryV1(ctx, stmt, rm); err != nil {
			return nil, false, err
		}

		rs, err := qr.planQueryV1(ctx, stmt, rm)
		if err != nil {
			return nil, false, err
		}

		ro = false

		pl = plan.Combine(pl, rs)
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
			return &plan.RandomDispatchPlan{}, ro, nil
		}
		if hasInfSchema && hasOtherSchema {
			return nil, false, rerrors.ErrInformationSchemaCombinedQuery
		}
		if hasInfSchema {
			return &plan.RandomDispatchPlan{}, ro, nil
		}

		p, err := qr.planQueryV1(ctx, stmt, rm)

		if err != nil {
			return nil, false, err
		}

		pl = plan.Combine(pl, p)

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
		pl = plan.Combine(pl, rs)
	case *lyx.Copy:
		return &plan.CopyPlan{}, false, nil
	default:
		return nil, false, spqrerror.NewByCode(spqrerror.SPQR_NOT_IMPLEMENTED)
	}

	tmp, err := qr.routeByTuples(ctx, rm, tsa)
	if err != nil {
		return nil, false, err
	}

	pl = plan.Combine(pl, tmp)

	// set up this variable if not yet
	if pl == nil {
		pl = &plan.ScatterPlan{
			ExecTargets: qr.DataShardsRoutes(),
		}
	}

	return pl, ro, nil
}

/*
* This function assumes that INSTEAD OF rules on selects in PostgreSQL are only RIR
 */
func CheckRoOnlyQuery(stmt lyx.Node) bool {
	switch v := stmt.(type) {
	/*
		*  XXX: should we be bit restrictive here than upstream?
		There are some possible cases when values clause is NOT ro-query.
		for example (as of now unsupported, but):

				example/postgres M # values((with d as (insert into zz select 1 returning *) table d));
				ERROR:  0A000: WITH clause containing a data-modifying statement must be at the top level
				LINE 1: values((with d as (insert into zz select 1 returning *) table...
				                     ^

	*/
	case *lyx.ValueClause:
		for _, ve := range v.Values {
			for _, e := range ve {
				if !CheckRoOnlyQuery(e) {
					return false
				}
			}
		}

		return true

	case *lyx.AExprBConst, *lyx.AExprIConst, *lyx.EmptyQuery, *lyx.AExprNConst:
		return true
	case *lyx.ColumnRef:
		return true
	case *lyx.AExprOp:
		return CheckRoOnlyQuery(v.Left) && CheckRoOnlyQuery(v.Right)
	case *lyx.CommonTableExpr:
		return CheckRoOnlyQuery(v.SubQuery)
	case *lyx.Select:
		if v.LArg != nil {
			if !CheckRoOnlyQuery(v.LArg) {
				return false
			}
		}

		if v.RArg != nil {
			if !CheckRoOnlyQuery(v.RArg) {
				return false
			}
		}

		for _, ch := range v.WithClause {
			if !CheckRoOnlyQuery(ch) {
				return false
			}
		}

		/* XXX:

		there can be sub-selects in from clause. Can they be non-readonly?

		db1=# create table zz( i int);
		CREATE TABLE
		db1=# create function f () returns int as $$ insert into zz values(1) returning * $$ language sql;
		CREATE FUNCTION
		db1=# select * from zz, (select f());
		 i | f
		---+---
		(0 rows)

		db1=# table zz;
		 i
		---
		 1
		(1 row)
		*/

		for _, rte := range v.FromClause {
			switch ch := rte.(type) {
			case *lyx.JoinExpr, *lyx.RangeVar:
				/* hope this is ro */
			case *lyx.SubSelect:
				if !CheckRoOnlyQuery(ch) {
					return false
				}
			default:
				/* We do not really expect here anything else */
				return false
			}
		}

		for _, tle := range v.TargetList {
			switch v := tle.(type) {
			case *lyx.Select:
				if !CheckRoOnlyQuery(tle) {
					return false
				}
			case *lyx.FuncApplication:
				/* only allow white list of functions here */
				switch v.Name {
				case "now", "pg_is_in_recovery", "spqr_is_opened":
					/* these cases ok */
				default:
					return false
				}
			}
		}

		return true
	default:
		return false
	}
}

func (qr *ProxyQrouter) InitExecutionTargets(ctx context.Context, rm *rmeta.RoutingMetadataContext, stmt lyx.Node, p plan.Plan, ro bool, sph session.SessionParamsHolder) (plan.Plan, error) {

	switch v := p.(type) {
	case *plan.DataRowFilter:
		sp, err := qr.InitExecutionTargets(ctx, rm, stmt, v.SubPlan, ro, sph)
		if err != nil {
			return nil, err
		}

		/* XXX: Can we do better? */

		return &plan.DataRowFilter{
			SubPlan:     sp,
			FilterIndex: 0,
		}, err
	case *plan.ShardDispatchPlan:
		return v, nil
	case *plan.VirtualPlan:
		return v, nil
	case *plan.RandomDispatchPlan:
		if v.ExecTargets == nil {
			return planner.SelectRandomDispatchPlan(qr.DataShardsRoutes())
		} else {
			/* reference relation case */
			return planner.SelectRandomDispatchPlan(v.ExecTargets)
		}

	case *plan.CopyPlan:
		/* temporary */
		return &plan.ScatterPlan{
			ExecTargets: qr.DataShardsRoutes(),
		}, nil
	case *plan.ScatterPlan:
		if v.IsDDL {
			v.ExecTargets = qr.DataShardsRoutes()
			return v, nil
		}

		if sph.EnhancedMultiShardProcessing() {
			var err error
			if v.SubPlan == nil {
				v.SubPlan, err = planner.PlanDistributedQuery(ctx, rm, stmt)
				if err != nil {
					return nil, err
				}
			}
			if v.ExecTargets == nil {
				v.ExecTargets = qr.DataShardsRoutes()
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
	default:
		return nil, rerrors.ErrComplexQuery
	}
}

// TODO : unit tests
func (qr *ProxyQrouter) PlanQuery(ctx context.Context, stmt lyx.Node, sph session.SessionParamsHolder) (plan.Plan, error) {

	if !config.RouterConfig().Qr.AlwaysCheckRules {
		if len(config.RouterConfig().ShardMapping) == 1 {
			firstShard := ""
			for s := range config.RouterConfig().ShardMapping {
				firstShard = s
			}

			ro := true

			if config.RouterConfig().Qr.AutoRouteRoOnStandby {
				ro = CheckRoOnlyQuery(stmt)
			}

			return &plan.ShardDispatchPlan{
				ExecTarget: kr.ShardKey{
					Name: firstShard,
					RO:   ro,
				},
				PStmt: stmt,
			}, nil
		}
	}

	meta := rmeta.NewRoutingMetadataContext(sph, qr.mgr)
	p, ro, err := qr.RouteWithRules(ctx, meta, stmt, sph.GetTsa())
	if err != nil {
		return nil, err
	}

	np, err := qr.InitExecutionTargets(ctx, meta, stmt, p, ro, sph)
	if err == nil {
		np.SetStmt(stmt)
	}
	return np, err
}
