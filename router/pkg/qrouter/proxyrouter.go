package qrouter

import (
	"math/rand"

	"github.com/blastrain/vitess-sqlparser/sqlparser"
	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/qdb/qdb"
	"github.com/pg-sharding/spqr/qdb/qdb/mem"
	spqrparser "github.com/pg-sharding/spqr/yacc/console"
	"github.com/pkg/errors"
	"github.com/wal-g/tracelog"
	"golang.org/x/xerrors"
)

type ProxyRouter struct {
	ColumnMapping map[string]struct{}

	LocalTables map[string]struct{}

	Ranges map[string]*kr.KeyRange

	DataShardCfgs  map[string]*config.ShardCfg
	WorldShardCfgs map[string]*config.ShardCfg

	qdb qdb.QrouterDB
}

func (qr *ProxyRouter) AddWorldShard(name string, cfg *config.ShardCfg) error {

	tracelog.InfoLogger.Printf("adding world shard %s", name)
	qr.WorldShardCfgs[name] = cfg

	return nil
}

func (qr *ProxyRouter) DataShardsRoutes() []ShardRoute {

	var ret []ShardRoute

	for name := range qr.DataShardCfgs {
		ret = append(ret, ShardRoute{
			Shkey: kr.ShardKey{
				Name: name,
				RW:   true,
			},
		})
	}

	return ret
}

func (qr *ProxyRouter) WorldShardsRoutes() []ShardRoute {

	var ret []ShardRoute

	for name := range qr.WorldShardCfgs {
		ret = append(ret, ShardRoute{
			Shkey: kr.ShardKey{
				Name: name,
				RW:   true,
			},
		})
	}

	// a sort of round robin

	rand.Shuffle(len(ret), func(i, j int) {
		ret[i], ret[j] = ret[j], ret[i]
	})
	return ret
}

func (qr *ProxyRouter) WorldShards() []string {

	panic("implement me")
}

var _ Qrouter = &ProxyRouter{}

func NewProxyRouter() (*ProxyRouter, error) {
	db, err := mem.NewQrouterDBMem()
	if err != nil {
		return nil, err
	}

	return &ProxyRouter{
		ColumnMapping:  map[string]struct{}{},
		LocalTables:    map[string]struct{}{},
		Ranges:         map[string]*kr.KeyRange{},
		DataShardCfgs:  map[string]*config.ShardCfg{},
		WorldShardCfgs: map[string]*config.ShardCfg{},
		qdb:            db,
	}, nil
}

func (qr *ProxyRouter) Subscribe(krid string, krst *qdb.KeyRangeStatus, noitfyio chan<- interface{}) error {
	return qr.qdb.Watch(krid, krst, noitfyio)
}

func (qr *ProxyRouter) Unite(req *spqrparser.UniteKeyRange) error {
	panic("implement me")
}

func (qr *ProxyRouter) Split(req *spqrparser.SplitKeyRange) error {
	if err := qr.qdb.Begin(); err != nil {
		return err
	}

	defer func() { _ = qr.qdb.Commit() }()

	krOld := qr.Ranges[req.KeyRangeFromID]
	krNew := kr.KeyRangeFromSQL(
		&qdb.KeyRange{
			From:       req.Border,
			To:         krOld.UpperBound,
			KeyRangeID: req.KeyRangeID,
		},
	)

	_ = qr.qdb.Add(krNew.ToSQL())
	krOld.UpperBound = req.Border
	_ = qr.qdb.Update(krOld.ToSQL())

	qr.Ranges[krOld.ID] = krOld
	qr.Ranges[krNew.ID] = krNew

	return nil
}

func (qr *ProxyRouter) Lock(krid string) error {
	var keyRange *kr.KeyRange
	var ok bool

	if keyRange, ok = qr.Ranges[krid]; !ok {
		return errors.Errorf("key range with id %v not found", krid)
	}

	return qr.qdb.Lock(keyRange.ToSQL())
}

func (qr *ProxyRouter) UnLock(krid string) error {
	var keyRange *kr.KeyRange
	var ok bool

	if keyRange, ok = qr.Ranges[krid]; !ok {
		return errors.Errorf("key range with id %v not found", krid)
	}

	return qr.qdb.UnLock(keyRange.ToSQL())
}

func (qr *ProxyRouter) AddDataShard(name string, cfg *config.ShardCfg) error {

	tracelog.InfoLogger.Printf("adding node %s", name)
	qr.DataShardCfgs[name] = cfg

	return nil
}

func (qr *ProxyRouter) Shards() []string {

	var ret []string

	for name := range qr.DataShardCfgs {
		ret = append(ret, name)
	}

	return ret
}

func (qr *ProxyRouter) KeyRanges() []*kr.KeyRange {

	var ret []*kr.KeyRange

	for _, keyRange := range qr.Ranges {
		ret = append(ret, keyRange)
	}

	return ret
}

func (qr *ProxyRouter) AddShardingColumn(col string) error {
	qr.ColumnMapping[col] = struct{}{}
	return nil
}

func (qr *ProxyRouter) AddLocalTable(tname string) error {
	qr.LocalTables[tname] = struct{}{}
	return nil
}

func (qr *ProxyRouter) AddKeyRange(kr *kr.KeyRange) error {
	if _, ok := qr.Ranges[kr.ID]; ok {
		return errors.Errorf("key range with ID already defined", kr.ID)
	}

	qr.Ranges[kr.ID] = kr
	return nil
}

func (qr *ProxyRouter) routeByIndx(i []byte) *kr.KeyRange {

	for _, keyRange := range qr.Ranges {
		tracelog.InfoLogger.Printf("comparing %v with key range %v %v", i, keyRange.LowerBound, keyRange.UpperBound)
		if kr.CmpRanges(keyRange.LowerBound, i) && kr.CmpRanges(i, keyRange.UpperBound) {
			return keyRange
		}
	}

	return &kr.KeyRange{
		Shid: NOSHARD,
	}
}

func (qr *ProxyRouter) matchShkey(expr sqlparser.Expr) bool {
	switch texpr := expr.(type) {
	case sqlparser.ValTuple:
		for _, val := range texpr {
			if qr.matchShkey(val) {
				return true
			}
		}
	case *sqlparser.ColName:
		_, ok := qr.ColumnMapping[texpr.Name.String()]
		return ok
	default:
	}

	return false
}

func (qr *ProxyRouter) routeByExpr(expr sqlparser.Expr) ShardRoute {

	switch texpr := expr.(type) {
	case *sqlparser.AndExpr:
		lft := qr.routeByExpr(texpr.Left)
		if lft.Shkey.Name == NOSHARD {
			return qr.routeByExpr(texpr.Right)
		}
		return lft
	case *sqlparser.ComparisonExpr:

		if qr.matchShkey(texpr.Left) {
			tracelog.InfoLogger.Printf("go right")
			shindx := qr.routeByExpr(texpr.Right)
			return shindx
		}
		tracelog.InfoLogger.Printf("go left")
		return qr.routeByExpr(texpr.Left)
	case *sqlparser.SQLVal:

		tracelog.InfoLogger.Printf("parsed val %d", texpr.Val)
		keyRange := qr.routeByIndx(texpr.Val)
		rw := qr.qdb.Check(keyRange.ToSQL())

		return ShardRoute{
			Shkey: kr.ShardKey{
				Name: keyRange.Shid,
				RW:   rw,
			},
			Matchedkr: keyRange,
		}
	default:
	}

	return ShardRoute{
		Shkey: kr.ShardKey{
			Name: NOSHARD,
		},
	}
}

func (qr *ProxyRouter) isLocalTbl(from sqlparser.TableExprs) bool {
	for _, texpr := range from {
		switch tbltype := texpr.(type) {
		case *sqlparser.ParenTableExpr:
		case *sqlparser.JoinTableExpr:
		case *sqlparser.AliasedTableExpr:

			switch tname := tbltype.Expr.(type) {
			case sqlparser.TableName:
				if _, ok := qr.LocalTables[tname.Name.String()]; ok {
					return true
				}
			case *sqlparser.Subquery:
			default:
			}
		}

	}
	return false
}

func (qr *ProxyRouter) matchShards(qstmt sqlparser.Statement) []ShardRoute {

	tracelog.InfoLogger.Printf("parsed qtype %T", qstmt)

	switch stmt := qstmt.(type) {
	case *sqlparser.Select:
		if qr.isLocalTbl(stmt.From) {
			return nil
		}
		if stmt.Where != nil {
			shroute := qr.routeByExpr(stmt.Where.Expr)
			if shroute.Shkey.Name == NOSHARD {
				return nil
			}
			return []ShardRoute{shroute}
		}
		return nil

	case *sqlparser.Insert:
		for i, c := range stmt.Columns {

			if _, ok := qr.ColumnMapping[c.String()]; ok {

				switch vals := stmt.Rows.(type) {
				case sqlparser.Values:
					valTyp := vals[0]
					shroute := qr.routeByExpr(valTyp[i])
					if shroute.Shkey.Name == NOSHARD {
						return nil
					}
					return []ShardRoute{shroute}
				}
			}
		}
	case *sqlparser.Update:
		if stmt.Where != nil {
			shroute := qr.routeByExpr(stmt.Where.Expr)
			if shroute.Shkey.Name == NOSHARD {
				return nil
			}
			return []ShardRoute{shroute}
		}
		return nil
	case *sqlparser.CreateTable:
		tracelog.InfoLogger.Printf("ddl routing excpands to every shard")
		// route ddl to every shard
		shrds := qr.Shards()
		var ret []ShardRoute
		for _, sh := range shrds {
			ret = append(ret,
				ShardRoute{
					Shkey: kr.ShardKey{
						Name: sh,
						RW:   true,
					},
				})
		}

		return ret
	}

	return nil
}

var ParseError = xerrors.New("parsing stmt error")

func (qr *ProxyRouter) Route(q string) (RoutingState, error) {
	tracelog.InfoLogger.Printf("routing by %s", q)

	parsedStmt, err := sqlparser.Parse(q)
	if err != nil {
		return nil, ParseError
	}

	tracelog.InfoLogger.Printf("stmt type %T", parsedStmt)

	switch parsedStmt.(type) {
	case *sqlparser.DDL:
		return ShardMatchState{
			Routes: qr.DataShardsRoutes(),
		}, nil
	default:
		routes := qr.matchShards(parsedStmt)

		if routes == nil {
			return SkipRoutingState{}, nil
		}

		return ShardMatchState{
			Routes: routes,
		}, nil
	}

}
