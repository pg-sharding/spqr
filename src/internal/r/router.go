package r

import ( //"fmt"
	"strconv"

	sqlp "github.com/blastrain/vitess-sqlparser/sqlparser"
	//"github.com/wal-g///tracelog"
)

const NOSHARD = -1

type R struct {
	shindx int
}

func NewR() R {
	return R{
		shindx: NOSHARD,
	}
}

func routeByIndx(i int) int {
	//tracelog.InfoLogger.Println("routing to ", i)
	if i <= 8000 {
		return 0
	}
	if i <= 16000 {
		return 1
	}
	if i <= 24000 {
		return 2
	}
	if i <= 32000 {
		return 3
	}
	if i <= 40000 {
		return 4
	}
	if i <= 48000 {
		return 5
	}
	if i <= 56000 {
		return 6
	}
	if i <= 65000 {
		return 7
	}
	if i <= 73000 {
		return 8
	}

	return NOSHARD
}

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
		//fmt.Printf("colanme is %s\n", texpr.Name.String())
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
		if lft == NOSHARD {
			//fmt.Println("go right")
			return routeByExpr(texpr.Right)
		}
		//fmt.Printf("go lft %d\n", lft)
		return lft
	case *sqlp.ComparisonExpr:
		if matchShkey(texpr.Left) {
			shindx := routeByExpr(texpr.Right)
			//fmt.Printf("shkey mathed %d\n", shindx)
			return shindx
		}
	case *sqlp.SQLVal:
		valInt, err := strconv.Atoi(string(texpr.Val))
		if err != nil {
			return NOSHARD
		}
		return routeByIndx(valInt)
	default:
		//fmt.Printf("typ is %T\n", expr)
	}

	return NOSHARD
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

func getshindx(sql string) int {

	parsedStmt, err := sqlp.Parse(sql)
	if err != nil {
		return NOSHARD
	}
	//fmt.Printf("stmt = %+v\n", parsedStmt)

	switch stmt := parsedStmt.(type) {
	case *sqlp.Select:
		//fmt.Println("select routing")
		if isLocalTbl(stmt.From) {
			return 2
		}
		if stmt.Where != nil {
			shindx := routeByExpr(stmt.Where.Expr)
			return shindx
		}
		return NOSHARD

	case *sqlp.Insert:
		//fmt.Println("insert routing")
		for i, c := range stmt.Columns {

			//fmt.Printf("stmt = %+v\n", c)
			if _, ok := SHCOLMP[c.String()]; ok {

				switch vals := stmt.Rows.(type) {
				case sqlp.Values:
					valTyp := vals[0]
					return routeByExpr(valTyp[i])
				}
			}
		}
	case *sqlp.Update:
		//fmt.Println("updater routing")
		if stmt.Where != nil {
			shindx := routeByExpr(stmt.Where.Expr)
			return shindx
		}
		return NOSHARD

	}

	return NOSHARD
}

func (r *R) Route(q string) int {
	return getshindx(q)
}
