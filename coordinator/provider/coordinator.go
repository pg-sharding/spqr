package provider

import (
	"context"
	"github.com/pg-sharding/spqr/pkg/models/shrule"
	"net"

	"github.com/jackc/pgproto3/v2"
	"github.com/pg-sharding/spqr/coordinator"
	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/conn"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/qdb/qdb"
	"github.com/pg-sharding/spqr/router/grpcclient"
	router "github.com/pg-sharding/spqr/router/pkg"
	"github.com/pg-sharding/spqr/router/pkg/rrouter"
	routerproto "github.com/pg-sharding/spqr/router/protos"
	"github.com/pg-sharding/spqr/world"
	spqrparser "github.com/pg-sharding/spqr/yacc/console"
	"github.com/wal-g/tracelog"
	"golang.org/x/xerrors"
	"google.golang.org/grpc"
)

type routerconn struct {
	routerproto.KeyRangeServiceClient
	addr string
	id   string
}

func (r *routerconn) Addr() string {
	return r.addr
}

func (r *routerconn) ID() string {
	return r.id
}

var _ router.Router = &routerconn{}

func DialRouter(r router.Router) (*grpc.ClientConn, error) {
	return grpcclient.Dial(r.Addr())
}

type qdbCoordinator struct {
	db qdb.QrouterDB
}

func (d *qdbCoordinator) KeyRanges() []*kr.KeyRange {
	panic("implement me")
}

func (d *qdbCoordinator) Lock(krid string) (*kr.KeyRange, error) {
	panic("implement me")
}

func (d *qdbCoordinator) Split(req *kr.SplitKeyRange) error {
	panic("implement me")
}

func (d *qdbCoordinator) Unite(req *kr.UniteKeyRange) error {
	panic("implement me")
}

func (d *qdbCoordinator) RegisterWorld(w world.World) error {
	panic("implement me")
}

func (d *qdbCoordinator) AddShardingRule(rule *shrule.ShardingRule) error {
	resp, err := d.db.ListRouters()
	if err != nil {
		return err
	}
	tracelog.InfoLogger.Printf("routers %v", resp)
	for _, r := range resp {
		cc, err := DialRouter(r)

		tracelog.InfoLogger.Printf("dialing router %v, err %w", r, err)
		if err != nil {
			return err
		}

		cl := routerproto.NewShardingKeyServiceClient(cc)
		resp, err := cl.AddShardingKey(context.TODO(), &routerproto.AddShardingRuleRequest{
			Rules:
		})

		if err != nil {
			return err
		}

		tracelog.InfoLogger.Printf("got resp %v", resp)
	}

	return nil
}

func (d *qdbCoordinator) AddLocalTable(tname string) error {
	panic("implement me")
}

func (d *qdbCoordinator) AddKeyRange(keyRange *kr.KeyRange) error {
	resp, err := d.db.ListRouters()
	if err != nil {
		return err
	}
	tracelog.InfoLogger.Printf("routers %v", resp)
	for _, r := range resp {
		cc, err := DialRouter(r)

		tracelog.InfoLogger.Printf("dialing router %v, err %w", r, err)
		if err != nil {
			return err
		}

		cl := routerproto.NewKeyRangeServiceClient(cc)
		resp, err := cl.AddKeyRange(context.TODO(), &routerproto.AddKeyRangeRequest{
			KeyRange: keyRange.ToProto(),
		})

		if err != nil {
			return err
		}

		tracelog.InfoLogger.Printf("got resp %v", resp)
	}

	return nil
}

func (d *qdbCoordinator) UnLock(krid string) error {
	panic("implement me")
}

var _ coordinator.Coordinator = &qdbCoordinator{}

func NewCoordinator(db qdb.QrouterDB) *qdbCoordinator {
	return &qdbCoordinator{
		db: db,
	}
}

func (d *qdbCoordinator) RegisterRouter(r *qdb.Router) error {

	tracelog.InfoLogger.Printf("register router %v %v", r.Addr(), r.ID())

	return d.db.AddRouter(r)
}

// call from grpc
func proc() {

}

func (d *qdbCoordinator) ProcClient(netconn net.Conn) error {
	cl := rrouter.NewPsqlClient(netconn)

	err := cl.Init(nil, config.SSLMODEDISABLE)

	if err != nil {
		return err
	}

	tracelog.InfoLogger.Printf("initialized client connection %s-%s\n", cl.Usr(), cl.DB())

	if err := cl.AssignRule(&config.FRRule{
		AuthRule: config.AuthRule{
			Method: config.AuthOK,
		},
	}); err != nil {
		return err
	}

	if err := cl.Auth(); err != nil {
		return err
	}
	tracelog.InfoLogger.Printf("client auth OK")

	for {
		msg, err := cl.Receive()
		if err != nil {
			tracelog.ErrorLogger.Printf("failed to received msg %w", err)
			return err
		}

		tracelog.InfoLogger.Printf("received msg %v", msg)

		switch v := msg.(type) {
		case *pgproto3.Query:

			tstmt, err := spqrparser.Parse(v.String)
			if err != nil {
				return err
			}

			tracelog.InfoLogger.Printf("parsed %T", v.String, tstmt)

			if err := func() error {
				switch stmt := tstmt.(type) {
				case *spqrparser.ShardingColumn:
					err := d.AddShardingRule(stmt.ColName)
					if err != nil {
						cl.ReplyErr(err.Error())
						tracelog.ErrorLogger.PrintError(err)
						return err
					}

					return nil
				case *spqrparser.RegisterRouter:
					err := d.RegisterRouter(qdb.NewRouter(stmt.Addr, stmt.ID))
					if err != nil {
						cl.ReplyErr(err.Error())
						tracelog.ErrorLogger.PrintError(err)
						return err
					}

					return nil
				case *spqrparser.KeyRange:
					if err := d.AddKeyRange(
						kr.KeyRangeFromSQL(stmt),
					); err != nil {
						cl.ReplyErr(err.Error())
						tracelog.ErrorLogger.PrintError(err)
						return err
					}

					return nil
				case *spqrparser.Show:

				default:
					return xerrors.New("failed to proc")
				}
				return nil
			}(); err != nil {
				_ = cl.ReplyErr(err.Error())
			} else {

				if err := func() error {
					for _, msg := range []pgproto3.BackendMessage{
						&pgproto3.RowDescription{Fields: []pgproto3.FieldDescription{
							{
								Name:                 []byte("coordinator"),
								TableOID:             0,
								TableAttributeNumber: 0,
								DataTypeOID:          25,
								DataTypeSize:         -1,
								TypeModifier:         -1,
								Format:               0,
							},
						}},
						&pgproto3.DataRow{Values: [][]byte{[]byte("query ok")}},
						&pgproto3.CommandComplete{CommandTag: []byte("SELECT 1")},
						&pgproto3.ReadyForQuery{
							TxStatus: conn.TXREL,
						},
					} {
						if err := cl.Send(msg); err != nil {
							return err
						}
					}

					return nil
				}(); err != nil {
					tracelog.ErrorLogger.PrintError(err)
				}
			}

		default:
		}
	}
}
