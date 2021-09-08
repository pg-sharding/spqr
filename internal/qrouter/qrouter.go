package qrouter

import (
	"strconv"

	sqlp "github.com/blastrain/vitess-sqlparser/sqlparser"
	"github.com/pg-sharding/spqr/internal/config"
	"github.com/pg-sharding/spqr/internal/qdb"
	"github.com/pg-sharding/spqr/internal/qdb/mem"
	"github.com/pg-sharding/spqr/yacc/spqrparser"
	"github.com/wal-g/tracelog"
	"golang.org/x/xerrors"
)

const NOSHARD = ""

type ShardRoute struct {
}

type Qrouter interface {
	Route(q string) []qdb.ShardKey

	AddShardingColumn(col string) error
	AddLocalTable(tname string) error

	AddKeyRange(kr *spqrparser.KeyRange) error
	Shards() []string
	KeyRanges() []*spqrparser.KeyRange

	AddShard(name string, cfg *config.ShardCfg) error

	Lock(krid string) error
	UnLock(krid string) error
	Split(req *spqrparser.SplitKeyRange) error
	Unite(req *spqrparser.UniteKeyRange) error
}

type LocalQrouter struct {
	shid string
}

func (l *LocalQrouter) Unite(req *spqrparser.UniteKeyRange) error {
	panic("implement me")
}

func (l *LocalQrouter) AddLocalTable(tname string) error {
	return xerrors.New("Local qrouter does not support sharding")
}

func (l *LocalQrouter) AddKeyRange(kr *spqrparser.KeyRange) error {
	return xerrors.New("Local qrouter does not support sharding")
}

func (l *LocalQrouter) Shards() []string {
	return []string{l.shid}
}

func (l *LocalQrouter) KeyRanges() []*spqrparser.KeyRange {
	return nil
}

func (l *LocalQrouter) AddShard(name string, cfg *config.ShardCfg) error {
	return xerrors.New("Local qrouter does not support sharding")
}

func (l *LocalQrouter) Lock(krid string) error {
	return xerrors.New("Local qrouter does not support sharding")
}

func (l *LocalQrouter) UnLock(krid string) error {
	return xerrors.New("Local qrouter does not support sharding")
}

func (l *LocalQrouter) Split(req *spqrparser.SplitKeyRange) error {
	return xerrors.New("Local qrouter does not support sharding")
}

var _ Qrouter = &LocalQrouter{}

func NewLocalQrouter(shid string) *LocalQrouter {
	return &LocalQrouter{
		shid: shid,
	}
}

func (l *LocalQrouter) AddShardingColumn(col string) error {
	return xerrors.New("Local qoruter does not supprort sharding")
}

func (l *LocalQrouter) Route(q string) []qdb.ShardKey {
	return []qdb.ShardKey{{
		Name: l.shid,
	}}
}

type ShardQrouter struct {
	ColumnMapping map[string]struct{}

	LocalTables map[string]struct{}

	Ranges map[string]*spqrparser.KeyRange

	ShardCfgs map[string]*config.ShardCfg

	qdb qdb.QrouterDB
}

func (qr *ShardQrouter) Unite(req *spqrparser.UniteKeyRange) error {
	panic("implement me")
}

func (qr *ShardQrouter) Split(req *spqrparser.SplitKeyRange) error {
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

func (qr *ShardQrouter) Lock(krid string) error {
	var kr *spqrparser.KeyRange
	var ok bool

	if kr, ok = qr.Ranges[krid]; !ok {
		return xerrors.Errorf("key range with id %v not found", krid)
	}

	return qr.qdb.Lock(kr)
}

func (qr *ShardQrouter) UnLock(krid string) error {
	var kr *spqrparser.KeyRange
	var ok bool

	if kr, ok = qr.Ranges[krid]; !ok {
		return xerrors.Errorf("key range with id %v not found", krid)
	}

	return qr.qdb.UnLock(kr)
}

func (qr *ShardQrouter) AddShard(name string, cfg *config.ShardCfg) error {

	tracelog.InfoLogger.Printf("adding node %s", name)
	qr.ShardCfgs[name] = cfg

	return nil
}

func (qr *ShardQrouter) Shards() []string {

	var ret []string

	for name := range qr.ShardCfgs {
		ret = append(ret, name)
	}

	return ret
}

func (qr *ShardQrouter) KeyRanges() []*spqrparser.KeyRange {

	var ret []*spqrparser.KeyRange

	for _, kr := range qr.Ranges {
		ret = append(ret, kr)
	}

	return ret
}

var _ Qrouter = &ShardQrouter{}

func NewQrouter() (*ShardQrouter, error) {

	// acq conn to db
	qdb, err := mem.NewQrouterDBMem()

	if err != nil {
		return nil, err
	}

	return &ShardQrouter{
		ColumnMapping: map[string]struct{}{},
		LocalTables:   map[string]struct{}{},
		Ranges:        map[string]*spqrparser.KeyRange{},
		ShardCfgs:     map[string]*config.ShardCfg{},
		qdb:           qdb,
	}, nil
}

func (qr *ShardQrouter) AddShardingColumn(col string) error {
	qr.ColumnMapping[col] = struct{}{}
	return nil
}

func (qr *ShardQrouter) AddLocalTable(tname string) error {
	qr.LocalTables[tname] = struct{}{}
	return nil
}

func (qr *ShardQrouter) AddKeyRange(kr *spqrparser.KeyRange) error {
	if _, ok := qr.Ranges[kr.KeyRangeID]; ok {
		return xerrors.Errorf("key range with ID already defined", kr.KeyRangeID)
	}

	qr.Ranges[kr.KeyRangeID] = kr
	return nil
}

func (qr *ShardQrouter) routeByIndx(i int) string {

	for _, kr := range qr.Ranges {
		if kr.From <= i && kr.To >= i {
			return kr.ShardID
		}
	}

	return NOSHARD
}

func (qr *ShardQrouter) matchShkey(expr sqlp.Expr) bool {

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

func (qr *ShardQrouter) routeByExpr(expr sqlp.Expr) qdb.ShardKey {
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
			return qdb.ShardKey{Name: NOSHARD}
		}
		shname := qr.routeByIndx(valInt)
		rw := qr.qdb.Check(valInt)

		return qdb.ShardKey{
			Name: shname,
			RW:   rw,
		}
	default:
		//tracelog.InfoLogger.Println("typ is %T\n", expr)
	}

	return qdb.ShardKey{Name: NOSHARD}
}

func (qr *ShardQrouter) isLocalTbl(frm sqlp.TableExprs) bool {

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

func (qr *ShardQrouter) matchShards(sql string) []qdb.ShardKey {

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
			return []qdb.ShardKey{shkey}
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
					return []qdb.ShardKey{shkey}
				}
			}
		}
	case *sqlp.Update:
		if stmt.Where != nil {
			shkey := qr.routeByExpr(stmt.Where.Expr)
			if shkey.Name == NOSHARD {
				return nil
			}
			return []qdb.ShardKey{shkey}
		}
		return nil
	case *sqlp.CreateTable:
		tracelog.InfoLogger.Printf("ddl routing excpands to every shard")
		// route ddl to every shard
		shrds := qr.Shards()
		var ret []qdb.ShardKey
		for _, sh := range shrds {
			ret = append(ret, qdb.ShardKey{
				Name: sh,
				RW:   true,
			})
		}

		return ret
	}

	return nil
}

func (qr *ShardQrouter) Route(q string) []qdb.ShardKey {
	return qr.matchShards(q)
}
