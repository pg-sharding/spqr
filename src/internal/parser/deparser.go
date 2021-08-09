package deparse

import (

	//spqr "github.com/app/parser"
	sqlp "github.com/blastrain/vitess-sqlparser/sqlparser"
)

var SHCOLMP = map[string]struct{}{
	"w_id":     {},
	"d_w_id":   {},
	"c_w_id":   {},
	"h_c_w_id": {},
	"o_w_id":   {},
	"no_w_id":  {},
	"ol_w_id":  {},
	"s_w_id":   {},
}

var LOCALS = map[string]struct{}{
	"item1": {},
}

func matchShkey(expr sqlp.Expr) bool {

	switch texpr := expr.(type) {
	case sqlp.ValTuple:
		for _, val := range texpr {
			if matchShkey(val) {
				return true
			}
		}
	case *sqlp.ColName:
		_, ok := SHCOLMP[texpr.Name.String()]
		return ok
	default:
		//fmt.Printf("%T", texpr)
	}

	return false
}

func routeByExpr(expr sqlp.Expr) int {
	switch texpr := expr.(type) {
	case *sqlp.AndExpr:
		lft := routeByExpr(texpr.Left)
		if lft == -1 {
			return routeByExpr(texpr.Right)
		}
	case *sqlp.ComparisonExpr:
		if matchShkey(texpr.Left) {
			return routeByExpr(texpr.Right)
		}
	case *sqlp.SQLVal:
		valInt, err := strconv.Atoi(string(texpr.Val))
		if err != nil {
			return -1
		}
		return routeByIndx(valInt)
	default:
		//fmt.Printf("typ is %T\n", expr)
	}

	return -1
}

func isLocalTbl(frm sqlp.TableExprs) bool {

	for _, texpr := range frm {
		switch tbltype := texpr.(type) {
		case *sqlp.ParenTableExpr:
			//fmt.Println("parent table")
			//fmt.Println(tbltype.Exprs)
		case *sqlp.JoinTableExpr:
			//fmt.Println("join table")
			//fmt.Println(tbltype.LeftExpr)
		case *sqlp.AliasedTableExpr:
			//fmt.Println("aliased table")
			//fmt.Println(tbltype.Expr)

			switch tname := tbltype.Expr.(type) {
			case sqlp.TableName:
				//fmt.Printf("table name is %v\n", tname.Name)
				if _, ok := LOCALS[tname.Name.String()]; ok {
					return true
				}
			case *sqlp.Subquery:
				//fmt.Printf("sub table name is %v\n", tname.Select)
			default:
				//fmt.Printf("typ is %T\n", tname)
			}

		}

	}
	return false
}
