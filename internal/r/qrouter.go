package r

import (
	"github.com/wal-g/tracelog"
	"strconv"

	sqlp "github.com/blastrain/vitess-sqlparser/sqlparser" // Is it OK?
	"github.com/pg-sharding/spqr/internal/config"
	"github.com/pg-sharding/spqr/yacc/spqrparser"
)

const NOSHARD = ""

type Qrouter interface {
	Route(q string) string

	AddColumn(col string) error
	AddLocalTable(tname string) error

	AddKeyRange(kr *spqrparser.KeyRange) error
	Shards() []string
	AddShard(name string, cfg *config.ShardCfg) error
}

type R struct {
	ColumnMapping map[string]struct{}

	LocalTables map[string]struct{}

	Ranges []*spqrparser.KeyRange

	ShardCfgs map[string]*config.ShardCfg
}

func (r *R) AddShard(name string, cfg *config.ShardCfg) error {

	tracelog.InfoLogger.Printf("adding node %s", name)
	r.ShardCfgs [name] = cfg

	return nil
}

func (r *R) Shards() []string {

	var ret []string

	for name := range r.ShardCfgs {
		ret = append(ret, name)
	}

	return ret
}

var _ Qrouter = &R{
	ColumnMapping: map[string]struct{}{},
	LocalTables:   map[string]struct{}{},
	Ranges:        []*spqrparser.KeyRange{},
	ShardCfgs:     map[string]*config.ShardCfg{},
}

func NewR() *R {
	return &R{
		ColumnMapping: map[string]struct{}{},
		LocalTables:   map[string]struct{}{},
		Ranges:        []*spqrparser.KeyRange{},
	}
}

func (r *R) AddColumn(col string) error {
	r.ColumnMapping[col] = struct{}{}
	return nil
}

func (r *R) AddLocalTable(tname string) error {
	r.LocalTables[tname] = struct{}{}
	return nil
}

func (r *R) AddKeyRange(kr *spqrparser.KeyRange) error {
	r.Ranges = append(r.Ranges, kr)

	return nil
}

func (r *R) routeByIndx(i int) string {

	for _, kr := range r.Ranges {
		if kr.From <= i && kr.To >= i {
			return kr.ShardID
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
		//tracelog.InfoLogger.Println("colanme is %s\n", texpr.Name.String())
		_, ok := r.ColumnMapping[texpr.Name.String()]
		return ok
	default:
		//tracelog.InfoLogger.Println("%T", texpr)
	}

	return false
}

func (r *R) routeByExpr(expr sqlp.Expr) string {
	switch texpr := expr.(type) {
	case *sqlp.AndExpr:
		lft := r.routeByExpr(texpr.Left)
		if lft == NOSHARD {
			//tracelog.InfoLogger.Println("go right")
			return r.routeByExpr(texpr.Right)
		}
		//tracelog.InfoLogger.Println("go lft %d\n", lft)
		return lft
	case *sqlp.ComparisonExpr:
		if r.matchShkey(texpr.Left) {
			shindx := r.routeByExpr(texpr.Right)
			//tracelog.InfoLogger.Println("shkey mathed %d\n", shindx)
			return shindx
		}
	case *sqlp.SQLVal:
		valInt, err := strconv.Atoi(string(texpr.Val))
		if err != nil {
			return NOSHARD
		}
		return r.routeByIndx(valInt)
	default:
		//tracelog.InfoLogger.Println("typ is %T\n", expr)
	}

	return NOSHARD
}

func (r *R) isLocalTbl(frm sqlp.TableExprs) bool {

	for _, texpr := range frm {
		switch tbltype := texpr.(type) {
		case *sqlp.ParenTableExpr:
		case *sqlp.JoinTableExpr:
		case *sqlp.AliasedTableExpr:

			switch tname := tbltype.Expr.(type) {
			case sqlp.TableName:
				if _, ok := r.LocalTables[tname.Name.String()]; ok {
					return true
				}
			case *sqlp.Subquery:
			default:
			}
		}

	}
	return false
}

func (r *R) getshindx(sql string) string {

	parsedStmt, err := sqlp.Parse(sql)
	if err != nil {
		return NOSHARD
	}
	switch stmt := parsedStmt.(type) {
	case *sqlp.Select:
		if r.isLocalTbl(stmt.From) {
			return NOSHARD
		}
		if stmt.Where != nil {
			shindx := r.routeByExpr(stmt.Where.Expr)
			return shindx
		}
		return NOSHARD

	case *sqlp.Insert:
		for i, c := range stmt.Columns {

			if _, ok := r.ColumnMapping[c.String()]; ok {

				switch vals := stmt.Rows.(type) {
				case sqlp.Values:
					valTyp := vals[0]
					return r.routeByExpr(valTyp[i])
				}
			}
		}
	case *sqlp.Update:
		//tracelog.InfoLogger.Println("updater routing")
		if stmt.Where != nil {
			shindx := r.routeByExpr(stmt.Where.Expr)
			return shindx
		}
		return NOSHARD

	}

	return NOSHARD
}

// shard name
func (r *R) Route(q string) string {
	return r.getshindx(q)
}
