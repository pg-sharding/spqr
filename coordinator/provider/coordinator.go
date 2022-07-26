package provider

import (
	"context"
	"fmt"
	"net"

	"github.com/pg-sharding/spqr/pkg/meta"
	"github.com/pg-sharding/spqr/pkg/models/routers"

	"github.com/pg-sharding/spqr/qdb/ops"

	"github.com/pg-sharding/spqr/pkg/client"
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

func DialRouter(r *routers.Router) (*grpc.ClientConn, error) {
	return grpcclient.Dial(r.AdmAddr)
}

type qdbCoordinator struct {
	coordinator.Coordinator
	db qdb.QrouterDB
}

func (qc *qdbCoordinator) ListRouters(ctx context.Context) ([]*routers.Router, error) {
	//TODO implement me
	resp, err := qc.db.ListRouters(ctx)
	if err != nil {
		return nil, err
	}
	var retTouters []*routers.Router

	for _, v := range resp {
		retTouters = append(retTouters, &routers.Router{
			Id:      v.Id,
			AdmAddr: v.Address,
		})
	}

	return retTouters, nil
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
		shRules = append(shRules, shrule.NewShardingRule(rule.Id, rule.Colnames))
	}

	return shRules, nil
}

func (qc *qdbCoordinator) AddShardingRule(ctx context.Context, rule *shrule.ShardingRule) error {
	// Store sharding rule to metadb.
	if err := qc.db.AddShardingRule(ctx, &qdb.ShardingRule{
		Colnames: rule.Columns(),
		Id:       rule.ID(),
	}); err != nil {
		return err
	}

	resp, err := qc.db.ListRouters(ctx)
	if err != nil {
		spqrlog.Logger.PrintError(err)
		return err
	}

	spqrlog.Logger.Printf(spqrlog.DEBUG3, "routers %+v", resp)

	for _, r := range resp {
		cc, err := DialRouter(&routers.Router{
			Id:      r.ID(),
			AdmAddr: r.Addr(),
		})

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

	err := ops.AddKeyRangeWithChecks(ctx, qc.db, keyRange.ToSQL())
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
		cc, err := DialRouter(&routers.Router{
			Id:      r.ID(),
			AdmAddr: r.Addr(),
		})

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

func (qc *qdbCoordinator) ListKeyRanges(ctx context.Context) ([]*kr.KeyRange, error) {
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
	return ops.ModifyKeyRangeWithChecks(ctx, qc.db, keyRange.ToSQL())
}

func (qc *qdbCoordinator) LockKeyRange(ctx context.Context, keyRangeID string) (*kr.KeyRange, error) {
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

	if err := ops.AddKeyRangeWithChecks(ctx, qc.db, krNew.ToSQL()); err != nil {
		return fmt.Errorf("failed to add a new key range: %w", err)
	}

	krOld.UpperBound = req.Bound

	return ops.ModifyKeyRangeWithChecks(ctx, qc.db, krOld)
}

func (qc *qdbCoordinator) DropKeyRangeAll(ctx context.Context) ([]*kr.KeyRange, error) {

	// TODO: exclusive lock all routers

	krs, err := qc.ListKeyRanges(ctx)
	if err != nil {
		return nil, err
	}

	rtrs, err := qc.ListRouters(ctx)
	if err != nil {
		return nil, err
	}

	for _, qRouter := range rtrs {
		cc, err := DialRouter(qRouter)
		if err != nil {
			return nil, err
		}

		// Configure sharding rules.

		krClient := routerproto.NewKeyRangeServiceClient(cc)

		_, err = krClient.DropAllKeyRanges(ctx, &routerproto.DropAllKeyRangesRequest{})
		if err != nil {
			return nil, err
		}
	}

	return krs, nil
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

	if err := ops.ModifyKeyRangeWithChecks(ctx, qc.db, krLeft); err != nil {
		return fmt.Errorf("failed to update a new key range: %w", err)
	}

	return nil
}

func (qc *qdbCoordinator) ConfigureNewRouter(ctx context.Context, qRouter *routers.Router, cl client.Client) error {
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
		protoShardingRules = append(protoShardingRules,
			&routerproto.ShardingRule{Columns: shRule.Colnames, Id: shRule.Id})
	}

	resp, err := shClient.AddShardingRules(ctx, &routerproto.AddShardingRuleRequest{
		Rules: protoShardingRules,
	})

	if err != nil {
		return err
	}

	spqrlog.Logger.Printf(spqrlog.DEBUG3, "got sharding rules response %v", resp.String())
	_ = cl.ReplyNoticef("got sharding rules response %v", resp.String())

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

		spqrlog.Logger.Printf(spqrlog.DEBUG3, "got resp %v while adding kr %v", resp.String(), keyRange)
		_ = cl.ReplyNoticef("got resp %v while adding kr %v", resp.String(), keyRange)
	}

	return nil
}

func (qc *qdbCoordinator) RegisterRouter(ctx context.Context, r *routers.Router) error {
	// TODO: list routers and deduplicate
	spqrlog.Logger.Printf(spqrlog.DEBUG3, "try to register router %v %v", r.AdmAddr, r.Id)
	return qc.db.AddRouter(ctx, &qdb.Router{
		Id: r.Id,
	})
}

func (qc *qdbCoordinator) UnregisterRouter(ctx context.Context, rID string) error {
	spqrlog.Logger.Printf(spqrlog.DEBUG3, "unregister router %v", rID)

	return qc.db.DeleteRouter(ctx, rID)
}

func (qc *qdbCoordinator) Move(ctx context.Context, req *kr.MoveKeyRange) error {
	krmv, err := qc.db.Lock(ctx, req.Krid)
	if err != nil {
		return err
	}
	defer qc.db.Unlock(ctx, req.Krid)

	krmv.ShardID = req.ShardId
	return ops.ModifyKeyRangeWithChecks(ctx, qc.db, krmv)
}

func (qc *qdbCoordinator) ProcClient(ctx context.Context, nconn net.Conn) error {
	cl := psqlclient.NewPsqlClient(nconn)

	err := cl.Init(nil)

	if err != nil {
		return err
	}

	spqrlog.Logger.Printf(spqrlog.LOG, "initialized client connection %s-%s\n", cl.Usr(), cl.DB())

	if err := cl.AssignRule(&config.FrontendRule{
		AuthRule: &config.AuthCfg{
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

			if err := meta.Proc(ctx, tstmt, qc, cli, cl); err != nil {
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

func (qc *qdbCoordinator) AddDataShard(ctx context.Context, shard *datashards.DataShard) error {
	return qc.db.AddShard(ctx, qdb.NewShard(shard.ID, shard.Cfg.Hosts))
}

func (qc *qdbCoordinator) AddWorldShard(_ context.Context, _ *datashards.DataShard) error {
	panic("implement me")
}

func (qc *qdbCoordinator) ListShards(ctx context.Context) ([]*datashards.DataShard, error) {
	shardList, err := qc.db.ListShards(ctx)
	if err != nil {
		return nil, err
	}

	shards := make([]*datashards.DataShard, 0, len(shardList))

	for _, shard := range shardList {
		shards = append(shards, &datashards.DataShard{
			ID: shard.ID,
			Cfg: &config.Shard{
				Hosts: shard.Hosts,
			},
		})
	}

	return shards, nil
}

func (qc *qdbCoordinator) GetShardInfo(ctx context.Context, shardID string) (*datashards.DataShard, error) {
	panic("implement or delete me")
}
