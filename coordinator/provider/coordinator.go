package provider

import (
	"context"
	"net"

	"github.com/jackc/pgproto3/v2"
	"github.com/pg-sharding/spqr/coordinator"
	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/conn"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/pkg/models/shrule"
	"github.com/pg-sharding/spqr/qdb"
	"github.com/pg-sharding/spqr/router/grpcclient"
	router "github.com/pg-sharding/spqr/router/pkg"
	client2 "github.com/pg-sharding/spqr/router/pkg/client"
	routerproto "github.com/pg-sharding/spqr/router/protos"
	spqrparser "github.com/pg-sharding/spqr/yacc/console"
	"github.com/wal-g/tracelog"
	"golang.org/x/xerrors"
	"google.golang.org/grpc"
)

type routerConn struct {
	routerproto.KeyRangeServiceClient
	addr string
	id   string
}

func (r *routerConn) Addr() string {
	return r.addr
}

func (r *routerConn) ID() string {
	return r.id
}

var _ router.Router = &routerConn{}

func DialRouter(r router.Router) (*grpc.ClientConn, error) {
	return grpcclient.Dial(r.Addr())
}

type qdbCoordinator struct {
	coordinator.Coordinator

	db qdb.QrouterDB
}

func (qc *qdbCoordinator) ListShardingRules(ctx context.Context) ([]*shrule.ShardingRule, error) {
	return qc.db.ListShardingRules(ctx)
}

func (qc *qdbCoordinator) AddShardingRule(ctx context.Context, rule *shrule.ShardingRule) error {
	resp, err := qc.db.ListRouters(ctx)
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

		cl := routerproto.NewShardingRulesServiceClient(cc)
		resp, err := cl.AddShardingRules(context.TODO(), &routerproto.AddShardingRuleRequest{
			Rules: []*routerproto.ShardingRule{{Columns: rule.Columns()}},
		})

		if err != nil {
			return err
		}

		tracelog.InfoLogger.Printf("got resp %v", resp)
	}

	return nil
}

func (qc *qdbCoordinator) AddLocalTable(tname string) error {
	panic("implement me")
}

func (qc *qdbCoordinator) AddKeyRange(ctx context.Context, keyRange *kr.KeyRange) error {

	// add key range to metadb

	err := qc.db.AddKeyRange(ctx, keyRange.ToSQL())
	if err != nil {
		return err
	}

	resp, err := qc.db.ListRouters(ctx)
	if err != nil {
		return err
	}

	tracelog.InfoLogger.Printf("routers %v", func() []string {
		var strs []string

		for _, el := range resp {
			strs = append(strs, el.Addr())
		}

		return strs
	})

	// notify all routers
	for _, r := range resp {
		cc, err := DialRouter(r)

		tracelog.InfoLogger.Printf("dialing router %v, err %w", r, err)
		if err != nil {
			return err
		}

		cl := routerproto.NewKeyRangeServiceClient(cc)
		resp, err := cl.AddKeyRange(ctx, &routerproto.AddKeyRangeRequest{
			KeyRange: keyRange.ToProto(),
		})

		if err != nil {
			return err
		}

		tracelog.InfoLogger.Printf("got resp %v", resp)
	}

	return nil
}

var _ coordinator.Coordinator = &qdbCoordinator{}

func NewCoordinator(db qdb.QrouterDB) *qdbCoordinator {
	return &qdbCoordinator{
		db: db,
	}
}

func (qc *qdbCoordinator) RegisterRouter(ctx context.Context, r *qdb.Router) error {

	tracelog.InfoLogger.Printf("register router %v %v", r.Addr(), r.ID())

	return qc.db.AddRouter(ctx, r)
}

func (qc *qdbCoordinator) ProcClient(ctx context.Context, nconn net.Conn) error {
	cl := client2.NewPsqlClient(nconn)

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

			tracelog.InfoLogger.Printf("parsed %v %T", v.String, tstmt)

			if err := func() error {
				switch stmt := tstmt.(type) {
				case *spqrparser.ShardingColumn:
					err := qc.AddShardingRule(ctx, shrule.NewShardingRule([]string{stmt.ColName}))
					if err != nil {
						return err
					}

					return nil
				case *spqrparser.RegisterRouter:
					err := qc.RegisterRouter(ctx, qdb.NewRouter(stmt.Addr, stmt.ID))
					if err != nil {
						return err
					}

					return nil
				case *spqrparser.AddKeyRange:
					if err := qc.AddKeyRange(ctx, kr.KeyRangeFromSQL(stmt)); err != nil {
						return err
					}
					return nil
				case *spqrparser.Lock:
					if _, err := qc.Lock(ctx, stmt.KeyRangeID); err != nil {
						return err
					}
				case *spqrparser.Show:

					switch stmt.Cmd {

					}

				default:
					return xerrors.New("failed to proc")
				}

				return nil
			}(); err != nil {
				tracelog.ErrorLogger.PrintError(err)

				//
				_ = cl.ReplyErr(err.Error())
			} else {
				tracelog.InfoLogger.Print("processed ok\n")

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
