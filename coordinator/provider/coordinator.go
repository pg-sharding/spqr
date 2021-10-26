package provider

import (
	"context"
	"net"

	"github.com/jackc/pgproto3/v2"
	"github.com/pg-sharding/spqr/coordinator"
	shhttp "github.com/pg-sharding/spqr/grpc"
	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/qdb/qdb"
	"github.com/pg-sharding/spqr/router/grpcclient"
	router "github.com/pg-sharding/spqr/router/pkg"
	"github.com/pg-sharding/spqr/router/pkg/rrouter"
	routerproto "github.com/pg-sharding/spqr/router/protos"
	shards "github.com/pg-sharding/spqr/router/protos"
	"github.com/pg-sharding/spqr/world"
	spqrparser "github.com/pg-sharding/spqr/yacc/console"
	"github.com/wal-g/tracelog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

type routerconn struct {
	routerproto.KeyRangeServiceClient
}

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
		db: db,
	}
}

func (d *dcoordinator) RegisterRouter(r router.Router) error {
	d.rmp[r.Addr()] = r

	return nil
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

			tracelog.InfoLogger.Printf("Get '%s', parsed %T", v.String, tstmt)

			switch stmt := tstmt.(type) {
			case *spqrparser.KeyRange:
				for _, r := range d.rmp {
					cc, err := DialRouter(r)
					if err != nil {
						break
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
			}

			tracelog.InfoLogger.Printf("received message %v", v.String)
			//
			//_ = cl.ReplyNotice("you are receiving message from mock world shard")
			//
			//err := func() error {
			//	for _, msg := range []pgproto3.BackendMessage{
			//		&pgproto3.RowDescription{Fields: []pgproto3.FieldDescription{
			//			{
			//				Name:                 []byte("worldmock"),
			//				TableOID:             0,
			//				TableAttributeNumber: 0,
			//				DataTypeOID:          25,
			//				DataTypeSize:         -1,
			//				TypeModifier:         -1,
			//				Format:               0,
			//			},
			//		}},
			//		&pgproto3.DataRow{Values: [][]byte{[]byte("row1")}},
			//		&pgproto3.CommandComplete{CommandTag: []byte("SELECT 1")},
			//		&pgproto3.ReadyForQuery{
			//			TxStatus: conn.TXREL,
			//		},
			//	} {
			//		if err := cl.Send(msg); err != nil {
			//			return err
			//		}
			//	}
			//
			//	return nil
			//}()
			//
			//if err != nil {
			//	tracelog.ErrorLogger.PrintError(err)
			//}

		default:
		}
	}
}

func (d *dcoordinator) Run() error {
	serv := grpc.NewServer()
	shhttp.Register(serv)
	reflection.Register(serv)

	krserv := NewKeyRangeService(d)
	rrserv := NewRoutersService(d)

	shards.RegisterKeyRangeServiceServer(serv, krserv)
	shards.RegisterRoutersServiceServer(serv, rrserv)

	httpAddr := config.Get().CoordinatorHttpAddr
	httpAddr = "localhost:7002"

	listener, err := net.Listen("tcp", httpAddr)
	if err != nil {
		return err
	}
	tracelog.InfoLogger.Printf("coordinator listening on %s", httpAddr)

	return serv.Serve(listener)
}
