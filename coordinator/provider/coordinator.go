package provider

import (
	"context"
	"golang.org/x/xerrors"
	"net"

	"github.com/jackc/pgproto3/v2"
	"github.com/pg-sharding/spqr/coordinator"
	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/conn"
	"github.com/pg-sharding/spqr/qdb/qdb"
	"github.com/pg-sharding/spqr/router/grpcclient"
	router "github.com/pg-sharding/spqr/router/pkg"
	"github.com/pg-sharding/spqr/router/pkg/rrouter"
	routerproto "github.com/pg-sharding/spqr/router/protos"
	"github.com/pg-sharding/spqr/world"
	spqrparser "github.com/pg-sharding/spqr/yacc/console"
	"github.com/wal-g/tracelog"
	"google.golang.org/grpc"
)

type routerconn struct {
	routerproto.KeyRangeServiceClient
	addr string
	id string
}

func (r* routerconn) Addr() string {
	return r.addr
}

func (r*routerconn) ID() string {
	return r.id
}

var _ router.Router = &routerconn{}

func DialRouter(r router.Router) (*grpc.ClientConn, error) {
	return grpcclient.Dial(r.Addr())
}

type dcoordinator struct {
	db qdb.QrouterDB

	rmp map[string]router.Router
}

func (d *dcoordinator) RegisterWorld(w world.World) error {
	panic("implement me")
}

func (d *dcoordinator) AddShardingColumn(col string) error {
	panic("implement me")
}

func (d *dcoordinator) AddLocalTable(tname string) error {
	panic("implement me")
}

func (d *dcoordinator) AddKeyRange(kr qdb.KeyRange) error {
	panic("implement me")
}

func (d *dcoordinator) Lock(krid string) error {
	panic("implement me")
}

func (d *dcoordinator) UnLock(krid string) error {
	panic("implement me")
}

func (d *dcoordinator) Split(req *spqrparser.SplitKeyRange) error {
	panic("implement me")
}

func (d *dcoordinator) Unite(req *spqrparser.UniteKeyRange) error {
	panic("implement me")
}

var _ coordinator.Coordinator = &dcoordinator{}

func NewCoordinator(db qdb.QrouterDB) *dcoordinator {
	return &dcoordinator{
		db:  db,
		rmp: map[string]router.Router{},
	}
}

func (d *dcoordinator) RegisterRouter(r router.Router) error {
	d.rmp[r.Addr()] = r

	return nil
}

// call from grpc
func proc() {

}

func (d *dcoordinator) Serve(netconn net.Conn) error {
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
				case *spqrparser.RegisterRouter:
					tracelog.InfoLogger.Printf("register router %v %v", stmt.Addr, stmt.ID)
					d.rmp[stmt.ID] = &routerconn{
						addr: stmt.Addr,
					}
					return nil
				case *spqrparser.KeyRange:
					for _, r := range d.rmp {
						cc, err := DialRouter(r)

						tracelog.InfoLogger.Printf("dialing router %v, err %w", r.Addr(), err)
						if err != nil {
							return err
						}
						cl := routerproto.NewKeyRangeServiceClient(cc)
						_, _ = cl.AddKeyRange(context.TODO(), &routerproto.AddKeyRangeRequest{
							KeyRange: &routerproto.KeyRange{
								LowerBound: string(stmt.From),
								UpperBound: string(stmt.To),
								Krid:       stmt.KeyRangeID,
								ShardId:    stmt.ShardID,
							},
						})
					}
				default:
					return xerrors.New("failed to proc");
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
