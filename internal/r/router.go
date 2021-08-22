package r

import ( //"fmt"
	"strconv"

	sqlp "github.com/blastrain/vitess-sqlparser/sqlparser"
	//"github.com/wal-g/////tracelog"
)

const NOSHARD = -1

type KeyRange struct {
	From    int
	To      int
	ShardId int
}

type Qrouter interface {
	Route(q string) int

	AddColumn(col string) error
	AddLocalTable(tname string) error

	AddKeyRange(kr KeyRange) error
}

type R struct {
	Qrouter
	SHCOLMP map[string]struct{}

	LOCALS map[string]struct{}

	Ranges []KeyRange
}

var _ Qrouter = &R{
	SHCOLMP: map[string]struct{}{},
	LOCALS:  map[string]struct{}{},
	Ranges:  []KeyRange{},
}

func NewR() R {
	return R{
		SHCOLMP: map[string]struct{}{},
		LOCALS:  map[string]struct{}{},
		Ranges:  []KeyRange{},
	}
}

func (r *R) AddColumn(col string) error {
	r.SHCOLMP[col] = struct{}{}
	return nil
}

func (r *R) AddLocalTable(tname string) error {
	r.LOCALS[tname] = struct{}{}
	return nil
}

func (r *R) AddKeyRange(kr KeyRange) error {
	r.Ranges = append(r.Ranges, kr)

	return nil
}

func (r *R) routeByIndx(i int) int {

	for _, kr := range r.Ranges {
		if kr.From <= i && kr.To >= i {
			return kr.ShardId
		}
	}

	return NOSHARD
}

func (r *R) matchShkey(expr sqlp.Expr) bool {

	switch texpr := expr.(type) {
	case sqlp.ValTuple:
		for _, val := range texpr {
			if r.matchShkey(val) {
				return true
			}
		}
	case *sqlp.ColName:
		//fmt.Printf("colanme is %s\n", texpr.Name.String())
		_, ok := r.SHCOLMP[texpr.Name.String()]
		return ok
	default:
		//fmt.Printf("%T", texpr)
	}

	return false
}

func (r *R) routeByExpr(expr sqlp.Expr) int {
	switch texpr := expr.(type) {
	case *sqlp.AndExpr:
		lft := r.routeByExpr(texpr.Left)
		if lft == NOSHARD {
			//fmt.Println("go right")
			return r.routeByExpr(texpr.Right)
		}
		//fmt.Printf("go lft %d\n", lft)
		return lft
	case *sqlp.ComparisonExpr:
		if r.matchShkey(texpr.Left) {
			shindx := r.routeByExpr(texpr.Right)
			//fmt.Printf("shkey mathed %d\n", shindx)
			return shindx
		}
	case *sqlp.SQLVal:
		valInt, err := strconv.Atoi(string(texpr.Val))
		if err != nil {
			return NOSHARD
		}
		return r.routeByIndx(valInt)
	default:
		//fmt.Printf("typ is %T\n", expr)
	}

	return NOSHARD
}

func (r *R) isLocalTbl(frm sqlp.TableExprs) bool {

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
				if _, ok := r.LOCALS[tname.Name.String()]; ok {
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

func (r *R) getshindx(sql string) int {

	parsedStmt, err := sqlp.Parse(sql)
	if err != nil {
		return NOSHARD
	}
	//fmt.Printf("stmt = %+v\n", parsedStmt)

	switch stmt := parsedStmt.(type) {
	case *sqlp.Select:
		//fmt.Println("select routing")
		if r.isLocalTbl(stmt.From) {
			return 2
		}
		if stmt.Where != nil {
			shindx := r.routeByExpr(stmt.Where.Expr)
			return shindx
		}
		return NOSHARD

	case *sqlp.Insert:
		//fmt.Println("insert routing")
		for i, c := range stmt.Columns {

			//fmt.Printf("stmt = %+v\n", c)
			if _, ok := r.SHCOLMP[c.String()]; ok {

				switch vals := stmt.Rows.(type) {
				case sqlp.Values:
					valTyp := vals[0]
					return r.routeByExpr(valTyp[i])
				}
			}
		}
	case *sqlp.Update:
		//fmt.Println("updater routing")
		if stmt.Where != nil {
			shindx := r.routeByExpr(stmt.Where.Expr)
			return shindx
		}
		return NOSHARD

	}

	return NOSHARD
}

func (r *R) Route(q string) int {
	return r.getshindx(q)
}
