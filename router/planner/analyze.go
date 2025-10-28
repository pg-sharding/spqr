package planner

import (
	"context"
	"fmt"
	"strings"

	"github.com/pg-sharding/lyx/lyx"
	"github.com/pg-sharding/spqr/pkg/models/spqrerror"
	"github.com/pg-sharding/spqr/pkg/spqrlog"
	"github.com/pg-sharding/spqr/router/rerrors"
	"github.com/pg-sharding/spqr/router/rfqn"
	"github.com/pg-sharding/spqr/router/rmeta"
)

// TODO : unit tests
func analyzeFromClauseList(
	ctx context.Context,
	clause []lyx.FromClauseNode, meta *rmeta.RoutingMetadataContext) error {
	for _, node := range clause {
		err := analyzeFromNode(ctx, node, meta)
		if err != nil {
			return err
		}
	}

	return nil
}

// TODO : unit tests
func analyzeSelectStmt(ctx context.Context, selectStmt lyx.Node, meta *rmeta.RoutingMetadataContext) error {

	switch s := selectStmt.(type) {
	case *lyx.Select:
		if clause := s.FromClause; clause != nil {
			// route `insert into rel select from` stmt
			if err := analyzeFromClauseList(ctx, clause, meta); err != nil {
				return err
			}
		}

		if clause := s.Where; clause != nil {
			if err := analyzeWhereClause(ctx, clause, meta); err != nil {
				return err
			}
		}

		if s.LArg != nil {
			if err := analyzeSelectStmt(ctx, s.LArg, meta); err != nil {
				return err
			}
		}
		if s.RArg != nil {
			if err := analyzeSelectStmt(ctx, s.RArg, meta); err != nil {
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
func analyzeFromNode(ctx context.Context, node lyx.FromClauseNode, meta *rmeta.RoutingMetadataContext) error {
	spqrlog.Zero.Debug().
		Type("node-type", node).
		Msg("analyzing from node")
	switch q := node.(type) {
	case *lyx.RangeVar:
		if err := ProcessRangeNode(ctx, meta, q); err != nil {
			return err
		}

	case *lyx.JoinExpr:
		if err := analyzeFromNode(ctx, q.Rarg, meta); err != nil {
			return err
		}
		if err := analyzeFromNode(ctx, q.Larg, meta); err != nil {
			return err
		}
	case *lyx.SubSelect:
		return analyzeSelectStmt(ctx, q.Arg, meta)
	default:
		// other cases to consider
		// lateral join, natural, etc
	}

	return nil
}

func analyzeWhereClause(ctx context.Context, expr lyx.Node, meta *rmeta.RoutingMetadataContext) error {
	if expr == nil {
		return nil
	}
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

				if err := AnalyzeQueryV1(ctx, q, meta); err != nil {
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
							return analyzeSelectStmt(ctx, argexpr.SubSelect, meta)
						}
					}
				}

			default:
				if err := analyzeWhereClause(ctx, texpr.Right, meta); err != nil {
					return err
				}
			}

		case *lyx.Select:
			if err := AnalyzeQueryV1(ctx, lft, meta); err != nil {
				return err
			}
		default:
			if err := analyzeWhereClause(ctx, texpr.Left, meta); err != nil {
				return err
			}
			if err := analyzeWhereClause(ctx, texpr.Right, meta); err != nil {
				return err
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
					return AnalyzeQueryV1(ctx, argexpr.SubSelect, meta)
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

func AnalyzeQueryV1(
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

		if err := analyzeFromNode(ctx, tr, rm); err != nil {
			return err
		}

		return nil
	}

	switch stmt := qstmt.(type) {
	case *lyx.Select:

		for _, expr := range stmt.TargetList {
			actualExpr := expr
			if rt, ok := expr.(*lyx.ResTarget); ok {
				actualExpr = rt.Value
			}

			switch e := actualExpr.(type) {
			/* Special cases for SELECT current_schema(), SELECT set_config(...), and SELECT pg_is_in_recovery() */
			case *lyx.FuncApplication:
				for _, innerExp := range e.Args {
					switch iE := innerExp.(type) {
					case *lyx.Select:
						if err := AnalyzeQueryV1(ctx, iE, rm); err != nil {
							return err
						}
					}
				}
			/* Expression like SELECT 1, SELECT 'a', SELECT 1.0, SELECT true, SELECT false */
			case *lyx.AExprIConst, *lyx.AExprSConst, *lyx.AExprNConst, *lyx.AExprBConst, *lyx.ColumnRef:
				/* Ok, skip analyze */
			case *lyx.Select:
				if err := AnalyzeQueryV1(ctx, e, rm); err != nil {
					return err
				}
			}
		}

		/*
		 * analyse both recurse branches to populates the FromClause info,
		 */

		if err := AnalyzeQueryV1(ctx, stmt.LArg, rm); err != nil {
			return err
		}

		if err := AnalyzeQueryV1(ctx, stmt.RArg, rm); err != nil {
			return err
		}

		if stmt.WithClause != nil {
			for _, cte := range stmt.WithClause {
				rm.CteNames[cte.Name] = struct{}{}
				if err := AnalyzeQueryV1(ctx, cte.SubQuery, rm); err != nil {
					return err
				}
			}
		}

		if stmt.FromClause != nil {
			// collect table alias names, if any
			// for single-table queries, process as usual
			if err := analyzeFromClauseList(ctx, stmt.FromClause, rm); err != nil {
				return err
			}
		}

		/* XXX: analyse where clause here, because it can contain col op subselect patterns */

		return analyzeWhereClause(ctx, stmt.Where, rm)
	case *lyx.Insert:

		if stmt.WithClause != nil {
			for _, cte := range stmt.WithClause {
				rm.CteNames[cte.Name] = struct{}{}
				if err := AnalyzeQueryV1(ctx, cte.SubQuery, rm); err != nil {
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
				return AnalyzeQueryV1(ctx, subS, rm)
			default:
				return nil
			}
		}

		return nil
	case *lyx.Update:

		if stmt.WithClause != nil {
			for _, cte := range stmt.WithClause {
				rm.CteNames[cte.Name] = struct{}{}
				if err := AnalyzeQueryV1(ctx, cte.SubQuery, rm); err != nil {
					return err
				}
			}
		}

		if err := analyseHelper(stmt.TableRef); err != nil {
			return err
		}

		return analyzeWhereClause(ctx, stmt.Where, rm)

	case *lyx.Delete:

		if stmt.WithClause != nil {
			for _, cte := range stmt.WithClause {
				rm.CteNames[cte.Name] = struct{}{}
				if err := AnalyzeQueryV1(ctx, cte.SubQuery, rm); err != nil {
					return err
				}
			}
		}

		if err := analyseHelper(stmt.TableRef); err != nil {
			return err
		}

		return analyzeWhereClause(ctx, stmt.Where, rm)
	}
	return nil
}
