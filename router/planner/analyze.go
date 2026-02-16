package planner

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"github.com/pg-sharding/lyx/lyx"
	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/models/spqrerror"
	"github.com/pg-sharding/spqr/pkg/spqrlog"
	"github.com/pg-sharding/spqr/router/rerrors"
	"github.com/pg-sharding/spqr/router/rfqn"
	"github.com/pg-sharding/spqr/router/rmeta"
	"github.com/pg-sharding/spqr/router/virtual"
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
func analyzeFromNode(ctx context.Context, node lyx.FromClauseNode, rm *rmeta.RoutingMetadataContext) error {
	spqrlog.Zero.Debug().
		Type("node-type", node).
		Msg("analyzing from node")
	switch q := node.(type) {
	case *lyx.RangeVar:
		if err := ProcessRangeNode(ctx, rm, q); err != nil {
			return err
		}

	case *lyx.JoinExpr:
		if err := analyzeFromNode(ctx, q.Rarg, rm); err != nil {
			return err
		}
		if err := analyzeFromNode(ctx, q.Larg, rm); err != nil {
			return err
		}

		if err := analyzeWhereClause(ctx, q.JoinQual, rm); err != nil {
			return err
		}

	case *lyx.SubSelect:
		return analyzeSelectStmt(ctx, q.Arg, rm)
	default:
		// other cases to consider
		// lateral join, natural, etc
	}

	return nil
}

/* XXX: Keep this in sync with `planByQualExpr`` */
func analyzeWhereClause(ctx context.Context, expr lyx.Node, rm *rmeta.RoutingMetadataContext) error {
	if expr == nil {
		return nil
	}
	spqrlog.Zero.Debug().
		Interface("clause", expr).
		Msg("analyzing select where clause")

	switch texpr := expr.(type) {
	/* lyx.ResTarget is unexpected here */
	case *lyx.AExprIn:

		switch lft := texpr.Expr.(type) {
		case *lyx.ColumnRef:

			alias, colname := lft.TableAlias, lft.ColName

			switch q := texpr.SubLink.(type) {
			case *lyx.AExprList:
				for _, expr := range q.List {
					if err := rm.ProcessConstExpr(alias, colname, expr); err != nil {
						return err
					}
				}
			case *lyx.Select:
				/* TODO properly support subquery here */
				/* SELECT * FROM t WHERE id IN (SELECT 1, 2) */

				if err := AnalyzeQueryV1(ctx, rm, q); err != nil {
					return err
				}
			}
		}

	case *lyx.AExprOp:

		if config.RouterConfig().Qr.StrictOperators {
			if texpr.Op != "=" {
				return nil
			}
		}

		switch lft := texpr.Left.(type) {
		/* simple key-value pair in const = id form */
		case *lyx.ParamRef, *lyx.AExprSConst, *lyx.AExprIConst:
			// else  error out?

			/* simple key-value pair */
			switch right := texpr.Right.(type) {
			case *lyx.ColumnRef:

				alias, colname := right.TableAlias, right.ColName
				// TBD: postpone routing from here to root of parsing tree
				// maybe extremely inefficient. Will be fixed in SPQR-3.0/engine v2
				if err := rm.ProcessConstExpr(alias, colname, lft); err != nil {
					return err
				}
			}
		/* lyx.ResTarget is unexpected here */
		case *lyx.ColumnRef:

			alias, colname := lft.TableAlias, lft.ColName

			/* simple key-value pair */
			switch right := texpr.Right.(type) {
			case *lyx.ParamRef, *lyx.AExprSConst, *lyx.AExprIConst:
				// else  error out?

				// TBD: postpone routing from here to root of parsing tree
				// maybe extremely inefficient. Will be fixed in SPQR-3.0/engine v2
				if err := rm.ProcessConstExpr(alias, colname, right); err != nil {
					return err
				}

			case *lyx.ColumnRef:
				/* colref = colref case, skip, expect when we know exact value of ColumnRef */
				for _, v := range rm.AuxExprByColref(right) {
					if err := rm.ProcessConstExpr(alias, colname, v); err != nil {
						return err
					}
				}

			case *lyx.AExprList:
				for _, expr := range right.List {
					if err := rm.ProcessConstExpr(alias, colname, expr); err != nil {
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
							return analyzeSelectStmt(ctx, argexpr.SubSelect, rm)
						}
					}
				}

			default:
				if err := analyzeWhereClause(ctx, texpr.Left, rm); err != nil {
					return err
				}
				if err := analyzeWhereClause(ctx, texpr.Right, rm); err != nil {
					return err
				}
			}

		case *lyx.Select:
			if err := AnalyzeQueryV1(ctx, rm, lft); err != nil {
				return err
			}
		default:
			if err := analyzeWhereClause(ctx, texpr.Left, rm); err != nil {
				return err
			}
			if err := analyzeWhereClause(ctx, texpr.Right, rm); err != nil {
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
					return AnalyzeQueryV1(ctx, rm, argexpr.SubSelect)
				}
			}
		}
	case *lyx.AExprList, *lyx.AExprNot:
		/* ok */
	default:
		return fmt.Errorf("analyze where clause, unknown expr %T: %w", expr, rerrors.ErrComplexQuery)
	}
	return nil
}

func AnalyzeWithClause(ctx context.Context, rm *rmeta.RoutingMetadataContext, WithClause []*lyx.CommonTableExpr) error {
	for _, cte := range WithClause {
		rm.CteNames[cte.Name] = struct{}{}
		switch qq := cte.SubQuery.(type) {
		case *lyx.ValueClause:
			/* special case */
			for _, vv := range qq.Values {
				for i, name := range cte.NameList {
					if i < len(cte.NameList) && i < len(vv) {
						/* XXX: currently only one-tuple aux values supported */
						rm.RecordAuxExpr(cte.Name, name, vv[i])
					}
				}
			}
		default:
			if err := AnalyzeQueryV1(ctx, rm, cte.SubQuery); err != nil {
				return err
			}
		}
	}

	return nil
}

func AnalyzeQueryV1(
	ctx context.Context,
	rm *rmeta.RoutingMetadataContext, qstmt lyx.Node) error {

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
	case *lyx.ExplainStmt:
		return AnalyzeQueryV1(ctx, rm, stmt.Query)
	case *lyx.Select:

		if err := AnalyzeWithClause(ctx, rm, stmt.WithClause); err != nil {
			return err
		}

		for _, expr := range stmt.TargetList {
			actualExpr := expr
			if rt, ok := expr.(*lyx.ResTarget); ok {
				actualExpr = rt.Value
			}

			switch e := actualExpr.(type) {
			/* Special cases for SELECT current_schema(), SELECT set_config(...), and SELECT pg_is_in_recovery() */
			case *lyx.FuncApplication:
				if e.Name == virtual.VirtualCTID {
					/* we only support this if query is simple select */
					rm.Is_SPQR_CTID = true
				}
				for _, innerExp := range e.Args {
					switch iE := innerExp.(type) {
					case *lyx.Select:
						if err := AnalyzeQueryV1(ctx, rm, iE); err != nil {
							return err
						}
					}
				}
			/* Expression like SELECT 1, SELECT 'a', SELECT 1.0, SELECT true, SELECT false */
			case *lyx.AExprIConst, *lyx.AExprSConst, *lyx.AExprNConst, *lyx.AExprBConst, *lyx.ColumnRef:
				/* Ok, skip analyze */
			case *lyx.Select:
				if err := AnalyzeQueryV1(ctx, rm, e); err != nil {
					return err
				}
			}
		}

		/*
		 * analyse both recurse branches to populates the FromClause info,
		 */

		if err := AnalyzeQueryV1(ctx, rm, stmt.LArg); err != nil {
			return err
		}

		if err := AnalyzeQueryV1(ctx, rm, stmt.RArg); err != nil {
			return err
		}

		// collect table alias names, if any
		// for single-table queries, process as usual
		if err := analyzeFromClauseList(ctx, stmt.FromClause, rm); err != nil {
			return err
		}

		/* XXX: analyse where clause here, because it can contain col op subselect patterns */

		return analyzeWhereClause(ctx, stmt.Where, rm)
	case *lyx.Insert:

		if err := AnalyzeWithClause(ctx, rm, stmt.WithClause); err != nil {
			return err
		}

		if err := analyseHelper(stmt.TableRef); err != nil {
			return err
		}
		if selectStmt := stmt.SubSelect; selectStmt != nil {
			switch subS := selectStmt.(type) {
			case *lyx.Select:
				spqrlog.Zero.Debug().Msg("analyze insert stmt on select clause")
				return AnalyzeQueryV1(ctx, rm, subS)
			default:
				return nil
			}
		}

		return nil
	case *lyx.Update:

		switch q := stmt.TableRef.(type) {
		case *lyx.RangeVar:
			rqdn := rfqn.RelationFQNFromRangeRangeVar(q)
			if d, err := rm.GetRelationDistribution(ctx, rqdn); err != nil {
				return err
			} else {
				r, ok := d.TryGetRelation(rqdn)
				/* Not all distribution guarantee that
				* get relation will actually return meaningful
				* `relation`. CatalogDistribution is one example. */
				if ok {
					cols, err := r.GetDistributionKeyColumns()
					if err != nil {
						return err
					}

					for _, c := range stmt.SetClause {
						switch cc := c.(type) {
						case *lyx.ResTarget:
							if slices.Contains(cols, cc.Name) {
								rm.IsSplitUpdate = true
							}
						default:
							return rerrors.ErrComplexQuery
						}
					}
				}
			}
		default:
			return spqrerror.NewByCode(spqrerror.SPQR_NOT_IMPLEMENTED)
		}

		if err := analyzeFromNode(ctx, stmt.TableRef, rm); err != nil {
			return err
		}

		if err := AnalyzeWithClause(ctx, rm, stmt.WithClause); err != nil {
			return err
		}

		return analyzeWhereClause(ctx, stmt.Where, rm)
	case *lyx.Delete:

		if err := AnalyzeWithClause(ctx, rm, stmt.WithClause); err != nil {
			return err
		}

		if err := analyseHelper(stmt.TableRef); err != nil {
			return err
		}

		return analyzeWhereClause(ctx, stmt.Where, rm)
	}
	return nil
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
				case "now", "pg_is_in_recovery":
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
