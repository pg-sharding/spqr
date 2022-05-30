package provider

import (
	"context"
	"fmt"
	"net"

	"github.com/pg-sharding/spqr/pkg/clientinteractor"
	"github.com/pg-sharding/spqr/pkg/spqrlog"

	"github.com/jackc/pgproto3/v2"
	"google.golang.org/grpc"

	"github.com/pg-sharding/spqr/coordinator"
	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/models/datashards"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/pkg/models/shrule"
	"github.com/pg-sharding/spqr/qdb"
	"github.com/pg-sharding/spqr/router/grpcclient"
	router "github.com/pg-sharding/spqr/router/pkg"
	psqlclient "github.com/pg-sharding/spqr/router/pkg/client"
	"github.com/pg-sharding/spqr/router/pkg/datashard"
	"github.com/pg-sharding/spqr/router/pkg/route"
	routerproto "github.com/pg-sharding/spqr/router/protos"
	spqrparser "github.com/pg-sharding/spqr/yacc/console"
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

var _ coordinator.Coordinator = &qdbCoordinator{}

func NewCoordinator(db qdb.QrouterDB) *qdbCoordinator {
	return &qdbCoordinator{
		db: db,
	}
}

func (qc *qdbCoordinator) ListShardingRules(ctx context.Context) ([]*shrule.ShardingRule, error) {
	rulesList, err := qc.db.ListShardingRules(ctx)
	if err != nil {
		return nil, err
	}

	shRules := make([]*shrule.ShardingRule, 0, len(rulesList))
	for _, rule := range rulesList {
		shRules = append(shRules, shrule.NewShardingRule(rule.Columns()))
	}

	return shRules, nil
}

func (qc *qdbCoordinator) AddShardingRule(ctx context.Context, rule *shrule.ShardingRule) error {
	// Store sharding rule to metadb.
	if err := qc.db.AddShardingRule(ctx, rule); err != nil {
		return err
	}

	resp, err := qc.db.ListRouters(ctx)
	if err != nil {
		spqrlog.Logger.PrintError(err)
		return err
	}

	spqrlog.Logger.Printf(spqrlog.DEBUG3, "routers %+v", resp)

	for _, r := range resp {
		cc, err := DialRouter(r)

		spqrlog.Logger.Printf(spqrlog.DEBUG1, "dialing router %v, err %w", r, err)
		if err != nil {
			spqrlog.Logger.PrintError(err)
			return err
		}

		cl := routerproto.NewShardingRulesServiceClient(cc)
		resp, err := cl.AddShardingRules(context.TODO(), &routerproto.AddShardingRuleRequest{
			Rules: []*routerproto.ShardingRule{{Columns: rule.Columns()}},
		})
		if err != nil {
			spqrlog.Logger.PrintError(err)
			return err
		}

		spqrlog.Logger.Printf(spqrlog.DEBUG5, "got resp %v", resp)
	}

	return nil
}

func (qc *qdbCoordinator) AddKeyRange(ctx context.Context, keyRange *kr.KeyRange) error {
	// add key range to metadb
	spqrlog.Logger.Printf(spqrlog.DEBUG5, "adding key range %+v", keyRange)

	err := qc.db.AddKeyRange(ctx, keyRange.ToSQL())
	if err != nil {
		return err
	}

	resp, err := qc.db.ListRouters(ctx)
	if err != nil {
		return err
	}

	spqrlog.Logger.Printf(spqrlog.DEBUG4, "routers %+v", func() []string {
		var strs []string

		for _, el := range resp {
			strs = append(strs, el.Addr())
		}

		return strs
	}())

	// notify all routers
	for _, r := range resp {
		cc, err := DialRouter(r)

		spqrlog.Logger.Printf(spqrlog.DEBUG4, "dialing router %v, err %w", r, err)
		if err != nil {
			return err
		}

		cl := routerproto.NewKeyRangeServiceClient(cc)
		resp, err := cl.AddKeyRange(ctx, &routerproto.AddKeyRangeRequest{
			KeyRangeInfo: keyRange.ToProto(),
		})

		if err != nil {
			return err
		}

		spqrlog.Logger.Printf(spqrlog.DEBUG4, "got resp %v", resp)
	}

	return nil
}

func (qc *qdbCoordinator) ListKeyRange(ctx context.Context) ([]*kr.KeyRange, error) {
	keyRanges, err := qc.db.ListKeyRanges(ctx)
	if err != nil {
		return nil, err
	}

	keyr := make([]*kr.KeyRange, 0, len(keyRanges))
	for _, keyRange := range keyRanges {
		keyr = append(keyr, kr.KeyRangeFromDB(keyRange))
	}

	return keyr, nil
}

func (qc *qdbCoordinator) MoveKeyRange(ctx context.Context, keyRange *kr.KeyRange) error {
	return qc.db.UpdateKeyRange(ctx, keyRange.ToSQL())
}

func (qc *qdbCoordinator) Lock(ctx context.Context, keyRangeID string) (*kr.KeyRange, error) {
	keyRangeDB, err := qc.db.Lock(ctx, keyRangeID)
	if err != nil {
		return nil, err
	}

	keyRange := kr.KeyRangeFromDB(keyRangeDB)

	return keyRange, nil
}

func (qc *qdbCoordinator) Unlock(ctx context.Context, keyRangeID string) error {
	return qc.db.Unlock(ctx, keyRangeID)
}

// Split TODO: check bounds and keyRangeID (sourceID)
func (qc *qdbCoordinator) Split(ctx context.Context, req *kr.SplitKeyRange) error {
	var krOld *qdb.KeyRange
	var err error

	spqrlog.Logger.Printf(spqrlog.DEBUG4, "Split request %#v", req)

	if krOld, err = qc.db.Lock(ctx, req.SourceID); err != nil {
		return err
	}

	defer func() {
		if err := qc.db.Unlock(ctx, req.SourceID); err != nil {
			spqrlog.Logger.PrintError(err)
		}
	}()

	krNew := kr.KeyRangeFromDB(
		&qdb.KeyRange{
			LowerBound: req.Bound,
			UpperBound: krOld.UpperBound,
			KeyRangeID: req.Krid,
			ShardID:    krOld.ShardID,
		},
	)

	spqrlog.Logger.Printf(spqrlog.DEBUG3, "New key range %#v", krNew)

	if err := qc.db.AddKeyRange(ctx, krNew.ToSQL()); err != nil {
		return fmt.Errorf("failed to add a new key range: %w", err)
	}

	krOld.UpperBound = req.Bound

	return qc.db.UpdateKeyRange(ctx, krOld)
}

func (qc *qdbCoordinator) Unite(ctx context.Context, uniteKeyRange *kr.UniteKeyRange) error {
	krLeft, err := qc.db.Lock(ctx, uniteKeyRange.KeyRangeIDLeft)
	if err != nil {
		return err
	}

	defer func() {
		if err := qc.db.Unlock(ctx, uniteKeyRange.KeyRangeIDLeft); err != nil {
			spqrlog.Logger.PrintError(err)
		}
	}()

	krRight, err := qc.db.Lock(ctx, uniteKeyRange.KeyRangeIDRight)
	if err != nil {
		return err
	}

	defer func() {
		if err := qc.db.Unlock(ctx, uniteKeyRange.KeyRangeIDRight); err != nil {
			spqrlog.Logger.PrintError(err)
		}
	}()

	krLeft.UpperBound = krRight.UpperBound

	if err := qc.db.DropKeyRange(ctx, krRight.KeyRangeID); err != nil {
		return fmt.Errorf("failed to drop an old key range: %w", err)
	}

	if err := qc.db.UpdateKeyRange(ctx, krLeft); err != nil {
		return fmt.Errorf("failed to update a new key range: %w", err)
	}

	return nil
}

func (qc *qdbCoordinator) ConfigureNewRouter(ctx context.Context, qRouter router.Router) error {
	cc, err := DialRouter(qRouter)

	spqrlog.Logger.Printf(spqrlog.DEBUG3, "dialing router %v, err %w", qRouter, err)
	if err != nil {
		return err
	}

	// Configure sharding rules.
	shardingRules, err := qc.db.ListShardingRules(ctx)
	if err != nil {
		return err
	}

	var protoShardingRules []*routerproto.ShardingRule
	shClient := routerproto.NewShardingRulesServiceClient(cc)
	krClient := routerproto.NewKeyRangeServiceClient(cc)
	for _, shRule := range shardingRules {
		protoShardingRules = append(protoShardingRules, &routerproto.ShardingRule{Columns: shRule.Columns()})
	}

	resp, err := shClient.AddShardingRules(ctx, &routerproto.AddShardingRuleRequest{
		Rules: protoShardingRules,
	})

	if err != nil {
		return err
	}

	spqrlog.Logger.Printf(spqrlog.DEBUG3, "got sharding rules response %v", resp.String())

	// Configure key ranges.
	keyRanges, err := qc.db.ListKeyRanges(ctx)
	if err != nil {
		return err
	}
	if _, err = krClient.DropAllKeyRanges(ctx, &routerproto.DropAllKeyRangesRequest{}); err != nil {
		return err
	}

	for _, keyRange := range keyRanges {
		resp, err := krClient.AddKeyRange(ctx, &routerproto.AddKeyRangeRequest{
			KeyRangeInfo: kr.KeyRangeFromDB(keyRange).ToProto(),
		})

		if err != nil {
			return err
		}

		spqrlog.Logger.Printf(spqrlog.DEBUG3, "got resp %v", resp.String())
	}

	return nil
}

func (qc *qdbCoordinator) RegisterRouter(ctx context.Context, r *qdb.Router) error {
	spqrlog.Logger.Printf(spqrlog.DEBUG3, "register router %v %v", r.Addr(), r.ID())
	return qc.db.AddRouter(ctx, r)
}

func (qc *qdbCoordinator) UnregisterRouter(ctx context.Context, rID string) error {
	spqrlog.Logger.Printf(spqrlog.DEBUG3, "unregister router %v", rID)

	return qc.db.DeleteRouter(ctx, rID)
}

var unknownCoordinatorCmd = fmt.Errorf("unknown coordinator dmd")

func (qc *qdbCoordinator) ProcClient(ctx context.Context, nconn net.Conn) error {
	cl := psqlclient.NewPsqlClient(nconn)

	err := cl.Init(nil, config.SSLMODEDISABLE)

	if err != nil {
		return err
	}

	spqrlog.Logger.Printf(spqrlog.LOG, "initialized client connection %s-%s\n", cl.Usr(), cl.DB())

	if err := cl.AssignRule(&config.FRRule{
		AuthRule: config.AuthRule{
			Method: config.AuthOK,
		},
	}); err != nil {
		return err
	}

	r := route.NewRoute(nil, nil, nil)
	r.SetParams(datashard.ParameterSet{})

	cli := clientinteractor.PSQLInteractor{}

	if err := cl.Auth(r); err != nil {
		return err
	}
	spqrlog.Logger.Printf(spqrlog.LOG, "client auth OK")

	for {
		msg, err := cl.Receive()
		if err != nil {
			spqrlog.Logger.Printf(spqrlog.ERROR, "failed to received msg %w", err)
			return err
		}

		spqrlog.Logger.Printf(spqrlog.DEBUG1, "received msg %v", msg)

		switch v := msg.(type) {
		case *pgproto3.Query:
			tstmt, err := spqrparser.Parse(v.String)
			if err != nil {
				_ = cli.ReportError(err, cl)
				continue
			}

			spqrlog.Logger.Printf(spqrlog.DEBUG5, "parsed %v %T", v.String, tstmt)

			if err := func() error {
				switch stmt := tstmt.(type) {
				case *spqrparser.ShardingColumn:
					shardingRule := shrule.NewShardingRule([]string{stmt.ColName})
					err := qc.AddShardingRule(ctx, shardingRule)
					if err != nil {
						return err
					}
					return cli.AddShardingRule(ctx, shardingRule, cl)
				case *spqrparser.RegisterRouter:
					newRouter := qdb.NewRouter(stmt.Addr, stmt.ID)

					if err := qc.ConfigureNewRouter(ctx, newRouter); err != nil {
						return err
					}

					if err := qc.RegisterRouter(ctx, newRouter); err != nil {
						return err
					}

					return cli.RegisterRouter(ctx, cl, stmt.ID, stmt.Addr)
				case *spqrparser.UnregisterRouter:
					if err := qc.UnregisterRouter(ctx, stmt.ID); err != nil {
						return err
					}
					return cli.UnregisterRouter(cl, stmt.ID)
				case *spqrparser.AddKeyRange:
					req := kr.KeyRangeFromSQL(stmt)
					if err := qc.AddKeyRange(ctx, req); err != nil {
						return err
					}
					return cli.AddKeyRange(ctx, req, cl)
				case *spqrparser.Lock:
					if _, err := qc.Lock(ctx, stmt.KeyRangeID); err != nil {
						return err
					}
					return cli.LockKeyRange(ctx, stmt.KeyRangeID, cl)
				case *spqrparser.Show:
					spqrlog.Logger.Printf(spqrlog.DEBUG4, "show %s stmt", stmt.Cmd)
					switch stmt.Cmd {
					case spqrparser.ShowKeyRangesStr:
						ranges, err := qc.db.ListKeyRanges(ctx)
						if err != nil {
							return err
						}

						var resp []*kr.KeyRange
						for _, el := range ranges {
							resp = append(resp, kr.KeyRangeFromDB(el))
						}
						return cli.KeyRanges(resp, cl)
					case spqrparser.ShowRoutersStr:
						routers, err := qc.db.ListRouters(ctx)
						if err != nil {
							return err
						}

						return cli.Routers(routers, cl)
					default:
						return cli.ReportError(unknownCoordinatorCmd, cl)
					}
				default:
					return unknownCoordinatorCmd
				}
			}(); err != nil {
				spqrlog.Logger.PrintError(err)
				_ = cli.ReportError(err, cl)
			} else {
				spqrlog.Logger.Printf(spqrlog.DEBUG1, "processed ok\n")
			}
		default:
			return cli.ReportError(fmt.Errorf("unsupported msg type %T", msg), cl)
		}
	}
}

func (qc *qdbCoordinator) AddDataShard(ctx context.Context, newShard *datashards.Shard) error {
	return qc.db.AddShard(ctx, qdb.NewShard(newShard.ID, newShard.Addr))
}

func (qc *qdbCoordinator) AddWorldShard(_ context.Context, _ *datashards.Shard) error {
	panic("implement me")
}

func (qc *qdbCoordinator) ListShards(ctx context.Context) ([]*datashards.Shard, error) {
	shardList, err := qc.db.ListShards(ctx)
	if err != nil {
		return nil, err
	}

	shards := make([]*datashards.Shard, 0, len(shardList))

	for _, shard := range shardList {
		shards = append(shards, &datashards.Shard{
			Addr: shard.Addr,
			ID:   shard.ID,
		})
	}

	return shards, nil
}

func (qc *qdbCoordinator) GetShardInfo(ctx context.Context, shardID string) (*datashards.ShardInfo, error) {
	shardInfo, err := qc.db.GetShardInfo(ctx, shardID)
	if err != nil {
		return nil, err
	}

	return &datashards.ShardInfo{
		Hosts: shardInfo.Hosts,
		Port:  shardInfo.Port,
	}, nil
}
