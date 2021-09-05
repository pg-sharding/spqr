package qrouter

import (
	"strconv"

	sqlp "github.com/blastrain/vitess-sqlparser/sqlparser"
	"github.com/pg-sharding/spqr/internal/config"
	"github.com/pg-sharding/spqr/yacc/spqrparser"
	"github.com/wal-g/tracelog"
)

const NOSHARD = ""

type Qrouter interface {
	Route(q string) []string

	AddColumn(col string) error
	AddLocalTable(tname string) error

	AddKeyRange(kr *spqrparser.KeyRange) error
	Shards() []string
	ShardCfg(string) *config.ShardCfg
	KeyRanges() []string

	AddShard(name string, cfg *config.ShardCfg) error
}

type QrouterImpl struct {
	ColumnMapping map[string]struct{}

	LocalTables map[string]struct{}

	Ranges []*spqrparser.KeyRange

	ShardCfgs map[string]*config.ShardCfg
}

func (r *QrouterImpl) ShardCfg(s string) *config.ShardCfg {
	return r.ShardCfgs[s]
}

func (r *QrouterImpl) AddShard(name string, cfg *config.ShardCfg) error {

	tracelog.InfoLogger.Printf("adding node %s", name)
	r.ShardCfgs[name] = cfg

	return nil
}

func (r *QrouterImpl) Shards() []string {

	var ret []string

	for name := range r.ShardCfgs {
		ret = append(ret, name)
	}

	return ret
}

func (r *QrouterImpl) KeyRanges() []string {

	var ret []string

	for _, kr := range r.Ranges {
		ret = append(ret, kr.ShardID)
	}

	return ret
}

var _ Qrouter = &QrouterImpl{}

func NewR() *QrouterImpl {
	return &QrouterImpl{
		ColumnMapping: map[string]struct{}{},
		LocalTables:   map[string]struct{}{},
		Ranges:        []*spqrparser.KeyRange{},
		ShardCfgs:     map[string]*config.ShardCfg{},
	}
}

func (r *QrouterImpl) AddColumn(col string) error {
	r.ColumnMapping[col] = struct{}{}
	return nil
}

func (r *QrouterImpl) AddLocalTable(tname string) error {
	r.LocalTables[tname] = struct{}{}
	return nil
}

func (r *QrouterImpl) AddKeyRange(kr *spqrparser.KeyRange) error {
	r.Ranges = append(r.Ranges, kr)

	return nil
}

func (r *QrouterImpl) routeByIndx(i int) string {

	for _, kr := range r.Ranges {
		if kr.From <= i && kr.To >= i {
			return kr.ShardID
		}
	}

	return NOSHARD
}

func (r *QrouterImpl) matchShkey(expr sqlp.Expr) bool {

	switch texpr := expr.(type) {
	case sqlp.ValTuple:
		for _, val := range texpr {
			if r.matchShkey(val) {
				return true
			}
		}
	case *sqlp.ColName:
		_, ok := r.ColumnMapping[texpr.Name.String()]
		return ok
	default:
	}

	return false
}

func (r *QrouterImpl) routeByExpr(expr sqlp.Expr) string {
	switch texpr := expr.(type) {
	case *sqlp.AndExpr:
		lft := r.routeByExpr(texpr.Left)
		if lft == NOSHARD {
			return r.routeByExpr(texpr.Right)
		}
		return lft
	case *sqlp.ComparisonExpr:
		if r.matchShkey(texpr.Left) {
			shindx := r.routeByExpr(texpr.Right)
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

func (r *QrouterImpl) isLocalTbl(frm sqlp.TableExprs) bool {

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

func (r *QrouterImpl) matchShards(sql string) []string {

	parsedStmt, err := sqlp.Parse(sql)
	if err != nil {
		return nil
	}

	tracelog.InfoLogger.Printf("parsed qtype %T", parsedStmt)

	switch stmt := parsedStmt.(type) {
	case *sqlp.Select:
		if r.isLocalTbl(stmt.From) {
			return nil
		}
		if stmt.Where != nil {
			shname := r.routeByExpr(stmt.Where.Expr)
			if shname == NOSHARD {
				return nil
			}
			return []string{shname}
		}
		return nil

	case *sqlp.Insert:
		for i, c := range stmt.Columns {

			if _, ok := r.ColumnMapping[c.String()]; ok {

				switch vals := stmt.Rows.(type) {
				case sqlp.Values:
					valTyp := vals[0]
					shname := r.routeByExpr(valTyp[i])
					if shname == NOSHARD {
						return nil
					}
					return []string{shname}
				}
			}
		}
	case *sqlp.Update:
		if stmt.Where != nil {
			shname := r.routeByExpr(stmt.Where.Expr)
			if shname == NOSHARD {
				return nil
			}
			return []string{shname}
		}
		return nil
	case *sqlp.CreateTable:
		tracelog.InfoLogger.Printf("ddl routing excpands to every shard")
		// route ddl to every shard
		return r.Shards()
	}

	return nil
}

func (r *QrouterImpl) Route(q string) []string {
	return r.matchShards(q)
}
