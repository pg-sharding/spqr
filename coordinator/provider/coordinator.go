package provider

import (
	"context"
	"fmt"
	"net"
	"time"

	"github.com/pg-sharding/spqr/pkg/meta"
	"github.com/pg-sharding/spqr/pkg/models/topology"

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
	routerproto "github.com/pg-sharding/spqr/pkg/protos"
	"github.com/pg-sharding/spqr/qdb"
	router "github.com/pg-sharding/spqr/router"
	psqlclient "github.com/pg-sharding/spqr/router/client"
	"github.com/pg-sharding/spqr/router/datashard"
	"github.com/pg-sharding/spqr/router/route"
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

func DialRouter(r *topology.Router) (*grpc.ClientConn, error) {
	// TODO: add creds
	return grpc.Dial(r.Address, grpc.WithInsecure())
}

type qdbCoordinator struct {
	coordinator.Coordinator
	db qdb.QDB
}

var _ coordinator.Coordinator = &qdbCoordinator{}

// watchRouters traverse routers one check if they are opened
// for clients. If not, initialize metadata and open router
func (qc *qdbCoordinator) watchRouters(ctx context.Context) {
	for {
		spqrlog.Logger.Printf(spqrlog.DEBUG3, "start routers watch iteration")

		// TODO: lock router
		rtrs, err := qc.db.ListRouters(ctx)
		if err != nil {
			spqrlog.Logger.PrintError(err)
			time.Sleep(time.Second)
			continue
		}

		if err := func() error {
			for _, r := range rtrs {
				spqrlog.Logger.Printf(spqrlog.DEBUG3, "dialing router %v", r.ID)
				internalR := &topology.Router{
					ID:      r.ID,
					Address: r.Address,
				}

				cc, err := DialRouter(internalR)
				if err != nil {
					return err
				}

				rrClient := routerproto.NewTopologyServiceClient(cc)

				resp, err := rrClient.GetRouterStatus(ctx, &routerproto.GetRouterStatusRequest{})
				if err != nil {
					return err
				}

				spqrlog.Logger.Printf(spqrlog.DEBUG4, "router %v status %v", r.ID, resp)
				switch resp.Status {
				case routerproto.RouterStatus_CLOSED:
					if err := qc.db.LockRouter(ctx, r.ID); err != nil {
						return err
					}
					if err := qc.SyncRouterMetadata(ctx, internalR); err != nil {
						return err
					}
					if _, err := rrClient.OpenRouter(ctx, &routerproto.OpenRouterRequest{}); err != nil {
						return err
					}
				case routerproto.RouterStatus_OPENED:
					// TODO: consistency checks
				}
			}

			return nil
		}(); err != nil {
			spqrlog.Logger.Printf(spqrlog.ERROR, "router watchdog coroutine failed: %v", err)
		}

		time.Sleep(time.Second)
	}
}

// NewCoordinator side efferc: runs async goroutine that checks
// spqr router`s availability
func NewCoordinator(db qdb.QDB) *qdbCoordinator {
	cc := &qdbCoordinator{
		db: db,
	}

	go cc.watchRouters(context.TODO())
	return cc
}

// traverseRouters traverse each route and run callback for each of them
// cb receives grpc connection to router`s admin console
func (qc *qdbCoordinator) traverseRouters(ctx context.Context, cb func(cc *grpc.ClientConn) error) error {
	spqrlog.Logger.Printf(spqrlog.DEBUG4, "qdb coordinator traverse")

	rtrs, err := qc.db.ListRouters(ctx)
	if err != nil {
		return err
	}

	for _, rtr := range rtrs {
		// TODO: run cb`s async
		cc, err := DialRouter(&topology.Router{
			ID:      rtr.ID,
			Address: rtr.Addr(),
		})

		spqrlog.Logger.Printf(spqrlog.DEBUG1, "dialing router %v, err %w", rtr.ID, err)
		if err != nil {
			return err
		}

		if err := cb(cc); err != nil {
			return err
		}
	}

	return nil
}

func (qc *qdbCoordinator) ListRouters(ctx context.Context) ([]*topology.Router, error) {
	resp, err := qc.db.ListRouters(ctx)
	if err != nil {
		return nil, err
	}
	var retRouters []*topology.Router

	for _, v := range resp {
		retRouters = append(retRouters, &topology.Router{
			ID:      v.ID,
			Address: v.Address,
		})
	}

	return retRouters, nil
}

func (qc *qdbCoordinator) AddRouter(ctx context.Context, router *topology.Router) error {
	return qc.db.AddRouter(ctx, topology.RouterToDB(router))
}

func (qc *qdbCoordinator) ListShardingRules(ctx context.Context) ([]*shrule.ShardingRule, error) {
	rulesList, err := qc.db.ListShardingRules(ctx)
	if err != nil {
		return nil, err
	}

	shRules := make([]*shrule.ShardingRule, 0, len(rulesList))
	for _, rule := range rulesList {
		shRules = append(shRules, shrule.ShardingRuleFromDB(rule))
	}

	return shRules, nil
}

func (qc *qdbCoordinator) AddShardingRule(ctx context.Context, rule *shrule.ShardingRule) error {
	// Store sharding rule to metadb.
	if err := qc.db.AddShardingRule(ctx, shrule.ShardingRuleToDB(rule)); err != nil {
		return err
	}

	return qc.traverseRouters(ctx, func(cc *grpc.ClientConn) error {
		cl := routerproto.NewShardingRulesServiceClient(cc)
		resp, err := cl.AddShardingRules(context.TODO(), &routerproto.AddShardingRuleRequest{
			Rules: []*routerproto.ShardingRule{shrule.ShardingRuleToProto(rule)},
		})
		if err != nil {
			spqrlog.Logger.PrintError(err)
			return err
		}

		spqrlog.Logger.Printf(spqrlog.DEBUG5, "got resp %v", resp)
		return nil
	})
}

func (qc *qdbCoordinator) DropShardingRuleAll(ctx context.Context) ([]*shrule.ShardingRule, error) {
	spqrlog.Logger.Printf(spqrlog.DEBUG4, "qdb coordinator dropping all sharding keys")

	if err := qc.traverseRouters(ctx, func(cc *grpc.ClientConn) error {
		cl := routerproto.NewShardingRulesServiceClient(cc)
		// TODO: support drop sharding rules all in grpc somehow
		resp, err := cl.ListShardingRules(context.TODO(), &routerproto.ListShardingRuleRequest{})
		if err != nil {
			spqrlog.Logger.PrintError(err)
			return err
		}

		var ids []string
		for _, v := range resp.Rules {
			ids = append(ids, v.Id)
		}

		spqrlog.Logger.Printf(spqrlog.DEBUG5, "got list sharding rules resp %v", resp)
		dropResp, err := cl.DropShardingRules(ctx, &routerproto.DropShardingRuleRequest{
			Id: ids,
		})
		spqrlog.Logger.Printf(spqrlog.DEBUG4, "drop sharding rules responce %v", dropResp)

		return err
	}); err != nil {
		return nil, err
	}

	// Drop sharding rules from qdb.
	rules, err := qc.db.DropShardingRuleAll(ctx)
	if err != nil {
		return nil, err
	}

	var ret []*shrule.ShardingRule

	for _, v := range rules {
		ret = append(ret, shrule.ShardingRuleFromDB(v))
	}

	return ret, nil
}

func (qc *qdbCoordinator) AddKeyRange(ctx context.Context, keyRange *kr.KeyRange) error {
	// add key range to metadb
	spqrlog.Logger.Printf(spqrlog.DEBUG5, "adding key range %+v", keyRange)

	err := ops.AddKeyRangeWithChecks(ctx, qc.db, keyRange)
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
		cc, err := DialRouter(&topology.Router{
			ID:      r.ID,
			Address: r.Addr(),
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
	return ops.ModifyKeyRangeWithChecks(ctx, qc.db, keyRange)
}

func (qc *qdbCoordinator) LockKeyRange(ctx context.Context, keyRangeID string) (*kr.KeyRange, error) {
	keyRangeDB, err := qc.db.LockKeyRange(ctx, keyRangeID)
	if err != nil {
		return nil, err
	}

	keyRange := kr.KeyRangeFromDB(keyRangeDB)

	return keyRange, nil
}

func (qc *qdbCoordinator) Unlock(ctx context.Context, keyRangeID string) error {
	return qc.db.UnlockKeyRange(ctx, keyRangeID)
}

// Split TODO: check bounds and keyRangeID (sourceID)
func (qc *qdbCoordinator) Split(ctx context.Context, req *kr.SplitKeyRange) error {
	var krOld *qdb.KeyRange
	var err error

	spqrlog.Logger.Printf(spqrlog.DEBUG4, "Split request %#v", req)

	if krOld, err = qc.db.LockKeyRange(ctx, req.SourceID); err != nil {
		return err
	}

	defer func() {
		if err := qc.db.UnlockKeyRange(ctx, req.SourceID); err != nil {
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

	if err := ops.AddKeyRangeWithChecks(ctx, qc.db, krNew); err != nil {
		return fmt.Errorf("failed to add a new key range: %w", err)
	}

	krOld.UpperBound = req.Bound

	return ops.ModifyKeyRangeWithChecks(ctx, qc.db, kr.KeyRangeFromDB(krOld))
}

func (qc *qdbCoordinator) DropKeyRangeAll(ctx context.Context) error {
	// TODO: exclusive lock all routers
	spqrlog.Logger.Printf(spqrlog.DEBUG4, "qdb coordinator dropping all key ranges")

	if err := qc.traverseRouters(ctx, func(cc *grpc.ClientConn) error {
		cl := routerproto.NewKeyRangeServiceClient(cc)
		dropResp, err := cl.DropAllKeyRanges(ctx, &routerproto.DropAllKeyRangesRequest{})
		spqrlog.Logger.Printf(spqrlog.DEBUG4, "drop key range response %v", dropResp)
		return err
	}); err != nil {
		return err
	}

	return qc.db.DropKeyRangeAll(ctx)
}

func (qc *qdbCoordinator) DropKeyRange(ctx context.Context, id string) error {
	// TODO: exclusive lock all routers
	spqrlog.Logger.Printf(spqrlog.DEBUG4, "qdb coordinator dropping all sharding keys")

	if err := qc.traverseRouters(ctx, func(cc *grpc.ClientConn) error {
		cl := routerproto.NewKeyRangeServiceClient(cc)
		dropResp, err := cl.DropKeyRange(ctx, &routerproto.DropKeyRangeRequest{
			Id: []string{id},
		})
		spqrlog.Logger.Printf(spqrlog.DEBUG4, "drop key ranges response %v", dropResp)
		return err
	}); err != nil {
		return err
	}

	// Drop key range from qdb.
	return qc.db.DropKeyRange(ctx, id)
}

func (qc *qdbCoordinator) DropShardingRule(ctx context.Context, id string) error {
	// TODO: exclusive lock all routers
	spqrlog.Logger.Printf(spqrlog.DEBUG4, "qdb coordinator dropping all sharding keys")

	if err := qc.traverseRouters(ctx, func(cc *grpc.ClientConn) error {
		cl := routerproto.NewShardingRulesServiceClient(cc)
		dropResp, err := cl.DropShardingRules(ctx, &routerproto.DropShardingRuleRequest{
			Id: []string{id},
		})
		spqrlog.Logger.Printf(spqrlog.DEBUG4, "drop sharding rules response %v", dropResp)
		return err
	}); err != nil {
		return err
	}

	// Drop key range from qdb.
	return qc.db.DropShardingRule(ctx, id)
}

func (qc *qdbCoordinator) Unite(ctx context.Context, uniteKeyRange *kr.UniteKeyRange) error {
	krLeft, err := qc.db.LockKeyRange(ctx, uniteKeyRange.KeyRangeIDLeft)
	if err != nil {
		return err
	}

	defer func() {
		if err := qc.db.UnlockKeyRange(ctx, uniteKeyRange.KeyRangeIDLeft); err != nil {
			spqrlog.Logger.PrintError(err)
		}
	}()

	krRight, err := qc.db.LockKeyRange(ctx, uniteKeyRange.KeyRangeIDRight)
	if err != nil {
		return err
	}

	defer func() {
		if err := qc.db.UnlockKeyRange(ctx, uniteKeyRange.KeyRangeIDRight); err != nil {
			spqrlog.Logger.PrintError(err)
		}
	}()

	krLeft.UpperBound = krRight.UpperBound

	if err := qc.db.DropKeyRange(ctx, krRight.KeyRangeID); err != nil {
		return fmt.Errorf("failed to drop an old key range: %w", err)
	}

	if err := ops.ModifyKeyRangeWithChecks(ctx, qc.db, kr.KeyRangeFromDB(krLeft)); err != nil {
		return fmt.Errorf("failed to update a new key range: %w", err)
	}

	return nil
}

// Move key range from one logical shard to another
func (qc *qdbCoordinator) Move(ctx context.Context, req *kr.MoveKeyRange) error {
	spqrlog.Logger.Printf(spqrlog.DEBUG4, "qdb coordinator dropping all sharding keys")
	if err := qc.traverseRouters(ctx, func(cc *grpc.ClientConn) error {
		cl := routerproto.NewKeyRangeServiceClient(cc)
		lockResp, err := cl.LockKeyRange(ctx, &routerproto.LockKeyRangeRequest{
			Id: []string{req.Krid},
		})
		spqrlog.Logger.Printf(spqrlog.DEBUG4, "lock sharding rules response %v", lockResp)
		return err
	}); err != nil {
		return err
	}

	defer func() {
		if err := qc.traverseRouters(ctx, func(cc *grpc.ClientConn) error {
			cl := routerproto.NewKeyRangeServiceClient(cc)
			lockResp, err := cl.UnlockKeyRange(ctx, &routerproto.UnlockKeyRangeRequest{
				Id: []string{req.Krid},
			})
			spqrlog.Logger.Printf(spqrlog.DEBUG4, "unlock sharding rules response %v", lockResp)
			return err
		}); err != nil {
			spqrlog.Logger.PrintError(err)
			return
		}
	}()

	krmv, err := qc.db.LockKeyRange(ctx, req.Krid)
	if err != nil {
		return err
	}
	defer qc.db.UnlockKeyRange(ctx, req.Krid)

	krmv.ShardID = req.ShardId
	if err := ops.ModifyKeyRangeWithChecks(ctx, qc.db, kr.KeyRangeFromDB(krmv)); err != nil {
		// TODO: check if unlock here is ok
		return err
	}

	if err := qc.traverseRouters(ctx, func(cc *grpc.ClientConn) error {
		cl := routerproto.NewKeyRangeServiceClient(cc)
		lockResp, err := cl.LockKeyRange(ctx, &routerproto.LockKeyRangeRequest{
			Id: []string{req.Krid},
		})
		if err != nil {
			return err
		}
		spqrlog.Logger.Printf(spqrlog.DEBUG4, "lock key range response %v", lockResp)

		defer func() {
			unlockResp, err := cl.UnlockKeyRange(ctx, &routerproto.UnlockKeyRangeRequest{
				Id: []string{req.Krid},
			})
			if err != nil {
				return
			}
			spqrlog.Logger.Printf(spqrlog.DEBUG4, "unlock key range response %v", unlockResp)
		}()

		moveResp, err := cl.MoveKeyRange(ctx, &routerproto.MoveKeyRangeRequest{
			KeyRange: kr.KeyRangeFromDB(krmv).ToProto(),
		})
		spqrlog.Logger.Printf(spqrlog.DEBUG4, "lock key range response %v", moveResp)
		return err
	}); err != nil {
		return err
	}
	// do phyc keys move
	return nil
}

func (qc *qdbCoordinator) SyncRouterMetadata(ctx context.Context, qRouter *topology.Router) error {
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
			shrule.ShardingRuleToProto(shrule.ShardingRuleFromDB(shRule)))
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

		spqrlog.Logger.Printf(spqrlog.DEBUG3, "got resp %v while adding kr %v", resp.String(), keyRange)
	}

	rCl := routerproto.NewTopologyServiceClient(cc)
	if resp, err := rCl.OpenRouter(ctx, &routerproto.OpenRouterRequest{}); err != nil {
		return err
	} else {
		spqrlog.Logger.Printf(spqrlog.DEBUG4, "open router response %v", resp)
	}

	return nil
}

func (qc *qdbCoordinator) RegisterRouter(ctx context.Context, r *topology.Router) error {
	// TODO: list routers and deduplicate
	spqrlog.Logger.Printf(spqrlog.DEBUG3, "try to register router %v %v", r.Address, r.ID)
	return qc.db.AddRouter(ctx, qdb.NewRouter(r.Address, r.ID, qdb.CLOSED))
}

func (qc *qdbCoordinator) UnregisterRouter(ctx context.Context, rID string) error {
	spqrlog.Logger.Printf(spqrlog.DEBUG3, "unregister router %v", rID)
	return qc.db.DeleteRouter(ctx, rID)
}

func (qc *qdbCoordinator) PrepareClient(nconn net.Conn) (client.Client, error) {
	cl := psqlclient.NewPsqlClient(nconn)

	if err := cl.Init(nil); err != nil {
		return nil, err
	}

	spqrlog.Logger.Printf(spqrlog.LOG, "initialized client connection %s-%s\n", cl.Usr(), cl.DB())

	if err := cl.AssignRule(&config.FrontendRule{
		AuthRule: &config.AuthCfg{
			Method: config.AuthOK,
		},
	}); err != nil {
		return nil, err
	}

	r := route.NewRoute(nil, nil, nil)
	r.SetParams(datashard.ParameterSet{})
	if err := cl.Auth(r); err != nil {
		return nil, err
	}
	spqrlog.Logger.Printf(spqrlog.LOG, "client auth OK")

	return cl, nil
}

func (qc *qdbCoordinator) ProcClient(ctx context.Context, nconn net.Conn) error {
	cl, err := qc.PrepareClient(nconn)
	if err != nil {
		spqrlog.Logger.PrintError(err)
		return err
	}

	cli := clientinteractor.NewPSQLInteractor(cl)
	for {
		// TODO: check leader status
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
				_ = cli.ReportError(err)
				continue
			}

			spqrlog.Logger.Printf(spqrlog.DEBUG5, "parsed %v %T", v.String, tstmt)

			if err := meta.Proc(ctx, tstmt, qc, cli); err != nil {
				spqrlog.Logger.PrintError(err)
				_ = cli.ReportError(err)
			} else {
				spqrlog.Logger.Printf(spqrlog.DEBUG1, "processed ok\n")
			}
		default:
			return cli.ReportError(fmt.Errorf("unsupported msg type %T", msg))
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
