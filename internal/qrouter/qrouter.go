package qrouter

import (
	"strconv"

	sqlp "github.com/blastrain/vitess-sqlparser/sqlparser"
	"github.com/pg-sharding/spqr/internal/config"
	"github.com/pg-sharding/spqr/internal/qrouterdb"
	"github.com/pg-sharding/spqr/internal/qrouterdb/mem"
	"github.com/pg-sharding/spqr/yacc/spqrparser"
	"github.com/wal-g/tracelog"
	"golang.org/x/xerrors"
)

const NOSHARD = ""

type Qrouter interface {
	Route(q string) []qrouterdb.ShardKey

	AddColumn(col string) error
	AddLocalTable(tname string) error

	AddKeyRange(kr *spqrparser.KeyRange) error
	Shards() []string
	ShardCfg(string) *config.ShardCfg
	KeyRanges() []string

	AddShard(name string, cfg *config.ShardCfg) error

	Lock(krid string) error
	UnLock(krid string) error
	Split(req *spqrparser.SplitKeyRange) error
}

type QrouterImpl struct {
	ColumnMapping map[string]struct{}

	LocalTables map[string]struct{}

	Ranges map[string]*spqrparser.KeyRange

	ShardCfgs map[string]*config.ShardCfg

	qdb qrouterdb.QrouterDB
}

func (qr *QrouterImpl) Split(req *spqrparser.SplitKeyRange) error {
	if err := qr.qdb.Begin(); err != nil {
		return err
	}

	defer func() { _ = qr.qdb.Commit() }()

	krOld := qr.Ranges[req.KeyRangeFromID]
	krNew := &spqrparser.KeyRange{
		From:       req.Border,
		To:         krOld.To,
		KeyRangeID: req.KeyRangeID,
	}

	_ = qr.qdb.Add(krNew)
	krOld.To = req.Border
	_ = qr.qdb.Update(krOld)

	qr.Ranges[krOld.KeyRangeID] = krOld
	qr.Ranges[krNew.KeyRangeID] = krNew

	return nil
}

func (qr *QrouterImpl) Lock(krid string) error {
	var kr *spqrparser.KeyRange
	var ok bool

	if kr, ok = qr.Ranges[krid]; !ok {
		return xerrors.Errorf("key range with id %v not found", krid)
	}

	return qr.qdb.Lock(kr)
}

func (qr *QrouterImpl) UnLock(krid string) error {
	var kr *spqrparser.KeyRange
	var ok bool

	if kr, ok = qr.Ranges[krid]; !ok {
		return xerrors.Errorf("key range with id %v not found", krid)
	}

	return qr.qdb.UnLock(kr)
}

func (qr *QrouterImpl) ShardCfg(s string) *config.ShardCfg {
	return qr.ShardCfgs[s]
}

func (qr *QrouterImpl) AddShard(name string, cfg *config.ShardCfg) error {

	tracelog.InfoLogger.Printf("adding node %s", name)
	qr.ShardCfgs[name] = cfg

	return nil
}

func (qr *QrouterImpl) Shards() []string {

	var ret []string

	for name := range qr.ShardCfgs {
		ret = append(ret, name)
	}

	return ret
}

func (qr *QrouterImpl) KeyRanges() []string {

	var ret []string

	for _, kr := range qr.Ranges {
		ret = append(ret, kr.ShardID)
	}

	return ret
}

var _ Qrouter = &QrouterImpl{}

func NewQrouter() (*QrouterImpl, error) {

	// acq conn to db
	qdb, err := mem.NewQrouterDBMem()

	if err != nil {
		return nil, err
	}

	return &QrouterImpl{
		ColumnMapping: map[string]struct{}{},
		LocalTables:   map[string]struct{}{},
		Ranges:        map[string]*spqrparser.KeyRange{},
		ShardCfgs:     map[string]*config.ShardCfg{},
		qdb:           qdb,
	}, nil
}

func (qr *QrouterImpl) AddColumn(col string) error {
	qr.ColumnMapping[col] = struct{}{}
	return nil
}

func (qr *QrouterImpl) AddLocalTable(tname string) error {
	qr.LocalTables[tname] = struct{}{}
	return nil
}

func (qr *QrouterImpl) AddKeyRange(kr *spqrparser.KeyRange) error {
	if _, ok := qr.Ranges[kr.KeyRangeID]; ok {
		return xerrors.Errorf("key range with ID already defined", kr.KeyRangeID)
	}

	qr.Ranges[kr.KeyRangeID] = kr
	return nil
}

func (qr *QrouterImpl) routeByIndx(i int) string {

	for _, kr := range qr.Ranges {
		if kr.From <= i && kr.To >= i {
			return kr.ShardID
		}
	}

	return NOSHARD
}

func (qr *QrouterImpl) matchShkey(expr sqlp.Expr) bool {

	switch texpr := expr.(type) {
	case sqlp.ValTuple:
		for _, val := range texpr {
			if qr.matchShkey(val) {
				return true
			}
		}
	case *sqlp.ColName:
		_, ok := qr.ColumnMapping[texpr.Name.String()]
		return ok
	default:
	}

	return false
}

func (qr *QrouterImpl) routeByExpr(expr sqlp.Expr) qrouterdb.ShardKey {
	switch texpr := expr.(type) {
	case *sqlp.AndExpr:
		lft := qr.routeByExpr(texpr.Left)
		if lft.Name == NOSHARD {
			return qr.routeByExpr(texpr.Right)
		}
		return lft
	case *sqlp.ComparisonExpr:
		if qr.matchShkey(texpr.Left) {
			shindx := qr.routeByExpr(texpr.Right)
			return shindx
		}
	case *sqlp.SQLVal:
		valInt, err := strconv.Atoi(string(texpr.Val))
		if err != nil {
			return qrouterdb.ShardKey{Name: NOSHARD}
		}
		shname := qr.routeByIndx(valInt)
		rw := qr.qdb.Check(valInt)

		return qrouterdb.ShardKey{
			Name: shname,
			RW:   rw,
		}
	default:
		//tracelog.InfoLogger.Println("typ is %T\n", expr)
	}

	return qrouterdb.ShardKey{Name: NOSHARD}
}

func (qr *QrouterImpl) isLocalTbl(frm sqlp.TableExprs) bool {

	for _, texpr := range frm {
		switch tbltype := texpr.(type) {
		case *sqlp.ParenTableExpr:
		case *sqlp.JoinTableExpr:
		case *sqlp.AliasedTableExpr:

			switch tname := tbltype.Expr.(type) {
			case sqlp.TableName:
				if _, ok := qr.LocalTables[tname.Name.String()]; ok {
					return true
				}
			case *sqlp.Subquery:
			default:
			}
		}

	}
	return false
}

func (qr *QrouterImpl) matchShards(sql string) []qrouterdb.ShardKey {

	parsedStmt, err := sqlp.Parse(sql)
	if err != nil {
		return nil
	}

	tracelog.InfoLogger.Printf("parsed qtype %T", parsedStmt)

	switch stmt := parsedStmt.(type) {
	case *sqlp.Select:
		if qr.isLocalTbl(stmt.From) {
			return nil
		}
		if stmt.Where != nil {
			shkey := qr.routeByExpr(stmt.Where.Expr)
			if shkey.Name == NOSHARD {
				return nil
			}
			return []qrouterdb.ShardKey{shkey}
		}
		return nil

	case *sqlp.Insert:
		for i, c := range stmt.Columns {

			if _, ok := qr.ColumnMapping[c.String()]; ok {

				switch vals := stmt.Rows.(type) {
				case sqlp.Values:
					valTyp := vals[0]
					shkey := qr.routeByExpr(valTyp[i])
					if shkey.Name == NOSHARD {
						return nil
					}
					return []qrouterdb.ShardKey{shkey}
				}
			}
		}
	case *sqlp.Update:
		if stmt.Where != nil {
			shkey := qr.routeByExpr(stmt.Where.Expr)
			if shkey.Name == NOSHARD {
				return nil
			}
			return []qrouterdb.ShardKey{shkey}
		}
		return nil
	case *sqlp.CreateTable:
		tracelog.InfoLogger.Printf("ddl routing excpands to every shard")
		// route ddl to every shard
		shrds := qr.Shards()
		var ret []qrouterdb.ShardKey
		for _, sh := range shrds {
			ret = append(ret, qrouterdb.ShardKey{
				Name: sh,
				RW:   true,
			})
		}

		return ret
	}

	return nil
}

func (qr *QrouterImpl) Route(q string) []qrouterdb.ShardKey {
	return qr.matchShards(q)
}
