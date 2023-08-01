package provider

import (
	"context"
	"fmt"
	"net"
	"time"

	"github.com/pg-sharding/spqr/pkg/datatransfers"
	"github.com/pg-sharding/spqr/pkg/meta"
	"github.com/pg-sharding/spqr/pkg/models/topology"
	"github.com/pg-sharding/spqr/pkg/shard"

	"github.com/pg-sharding/spqr/qdb/ops"

	"github.com/pg-sharding/spqr/pkg/client"
	"github.com/pg-sharding/spqr/pkg/clientinteractor"
	"github.com/pg-sharding/spqr/pkg/spqrlog"

	"github.com/jackc/pgx/v5/pgproto3"
	"google.golang.org/grpc"

	"github.com/pg-sharding/spqr/coordinator"
	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/connectiterator"
	"github.com/pg-sharding/spqr/pkg/models/datashards"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/pkg/models/shrule"
	"github.com/pg-sharding/spqr/pkg/pool"
	routerproto "github.com/pg-sharding/spqr/pkg/protos"
	"github.com/pg-sharding/spqr/qdb"
	router "github.com/pg-sharding/spqr/router"
	psqlclient "github.com/pg-sharding/spqr/router/client"
	"github.com/pg-sharding/spqr/router/route"
	spqrparser "github.com/pg-sharding/spqr/yacc/console"
)

type grpcConnectionIterator struct {
	*qdbCoordinator
}

func (ci grpcConnectionIterator) IterRouter(cb func(cc *grpc.ClientConn, addr string) error) error {
	ctx := context.TODO()
	rtrs, err := ci.qdbCoordinator.db.ListRouters(ctx)

	spqrlog.Zero.Log().Int("router counts", len(rtrs))

	if err != nil {
		return err
	}

	for _, r := range rtrs {
		internalR := &topology.Router{
			ID:      r.ID,
			Address: r.Address,
		}

		cc, err := DialRouter(internalR)
		if err != nil {
			return err
		}
		if err := cb(cc, r.Address); err != nil {
			return err
		}
	}
	return nil
}

func (ci grpcConnectionIterator) ClientPoolForeach(cb func(client client.ClientInfo) error) error {
	return ci.IterRouter(func(cc *grpc.ClientConn, addr string) error {
		ctx := context.TODO()
		rrClient := routerproto.NewClientInfoServiceClient(cc)

		spqrlog.Zero.Debug().Msg("fetch clients with grpc")
		resp, err := rrClient.ListClients(ctx, &routerproto.ListClientsRequest{})
		if err != nil {
			spqrlog.Zero.Error().Msg("error fetching clients with grpc")
			return err
		}

		for _, client := range resp.Clients {
			err = cb(psqlclient.NewMockClient(client, addr))
			if err != nil {
				return err
			}
		}
		return nil
	})
}

func (ci grpcConnectionIterator) Put(client client.Client) error {
	return fmt.Errorf("not implemented")
}

func (ci grpcConnectionIterator) Pop(id string) (bool, error) {
	return true, fmt.Errorf("not implemented")
}

func (ci grpcConnectionIterator) Shutdown() error {
	return fmt.Errorf("not implemented")
}

func (ci grpcConnectionIterator) ForEach(cb func(sh shard.Shardinfo) error) error {
	return ci.IterRouter(func(cc *grpc.ClientConn, addr string) error {
		ctx := context.TODO()
		rrBackConn := routerproto.NewBackendConnectionsServiceClient(cc)

		spqrlog.Zero.Debug().Msg("fetch clients with grpc")
		resp, err := rrBackConn.ListBackendConnections(ctx, &routerproto.ListBackendConnectionsRequest{})
		if err != nil {
			spqrlog.Zero.Error().Msg("error fetching clients with grpc")
			return err
		}

		for _, conn := range resp.Conns {
			err = cb(NewCoordShardInfo(conn))
			if err != nil {
				return err
			}
		}
		return nil
	})
}

func (ci grpcConnectionIterator) ForEachPool(cb func(p pool.Pool) error) error {
	return fmt.Errorf("not implemented")
}

var _ connectiterator.ConnectIterator = &grpcConnectionIterator{}

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
	spqrlog.Zero.Debug().
		Str("router-id", r.ID).
		Msg("dialing router")
	// TODO: add creds
	return grpc.Dial(r.Address, grpc.WithInsecure()) //nolint:all
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
		spqrlog.Zero.Debug().Msg("start routers watch iteration")

		// TODO: lock router
		rtrs, err := qc.db.ListRouters(ctx)
		if err != nil {
			spqrlog.Zero.Error().Err(err).Msg("")
			time.Sleep(time.Second)
			continue
		}

		if err := func() error {
			for _, r := range rtrs {
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

				switch resp.Status {
				case routerproto.RouterStatus_CLOSED:
					spqrlog.Zero.Debug().Msg("router is closed")
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
					spqrlog.Zero.Debug().Msg("router is opened")
					// TODO: consistency checks
				}
			}

			return nil
		}(); err != nil {
			spqrlog.Zero.Error().
				Err(err).
				Msg("router watchdog coroutine failed")
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

	ranges, err := db.ListKeyRanges(context.TODO())
	if err != nil {
		spqrlog.Zero.Error().
			Err(err).
			Msg("faild to list key ranges")
	}

	for _, r := range ranges {
		tx, err := db.GetTransferTx(context.TODO(), r.KeyRangeID)
		if tx == nil || err != nil || tx.ToStatus == "" {
			continue
		}
		if tx.ToStatus == "commit" {
			datatransfers.ResolvePreparedTransaction(context.TODO(), tx.FromShardId, tx.FromTxName, true)
			tem := kr.MoveKeyRange{
				ShardId: tx.ToShardId,
				Krid:    r.KeyRangeID,
			}
			err = cc.Move(context.TODO(), &tem)
			if err != nil {
				spqrlog.Zero.Error().Err(err).Msg("failed to move key range")
			}
		} else {
			datatransfers.ResolvePreparedTransaction(context.TODO(), tx.ToShardId, tx.ToTxName, false)
			datatransfers.ResolvePreparedTransaction(context.TODO(), tx.FromShardId, tx.FromTxName, false)
		}
		err = db.RemoveTransferTx(context.TODO(), r.KeyRangeID)
		if err != nil {
			spqrlog.Zero.Error().Err(err).Msg("error removing from qdb")
		}
	}

	go cc.watchRouters(context.TODO())
	return cc
}

// traverseRouters traverse each route and run callback for each of them
// cb receives grpc connection to router`s admin console
func (qc *qdbCoordinator) traverseRouters(ctx context.Context, cb func(cc *grpc.ClientConn) error) error {
	spqrlog.Zero.Debug().Msg("qdb coordinator traverse")

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
			return err
		}

		spqrlog.Zero.Debug().
			Interface("response", resp).
			Msg("add sharding rules response")
		return nil
	})
}

func (qc *qdbCoordinator) DropShardingRuleAll(ctx context.Context) ([]*shrule.ShardingRule, error) {
	spqrlog.Zero.Debug().Msg("qdb coordinator dropping all sharding keys")

	if err := qc.traverseRouters(ctx, func(cc *grpc.ClientConn) error {
		cl := routerproto.NewShardingRulesServiceClient(cc)
		// TODO: support drop sharding rules all in grpc somehow
		listResp, err := cl.ListShardingRules(context.TODO(), &routerproto.ListShardingRuleRequest{})
		if err != nil {
			return err
		}

		var ids []string
		for _, v := range listResp.Rules {
			ids = append(ids, v.Id)
		}

		spqrlog.Zero.Debug().
			Interface("response", listResp).
			Msg("list sharding rules response")

		dropResp, err := cl.DropShardingRules(ctx, &routerproto.DropShardingRuleRequest{
			Id: ids,
		})

		spqrlog.Zero.Debug().
			Interface("response", dropResp).
			Msg("drop sharding rules response")

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
	spqrlog.Zero.Debug().
		Bytes("lower-bound", keyRange.LowerBound).
		Bytes("upper-bound", keyRange.UpperBound).
		Str("shard-id", keyRange.ShardID).
		Str("key-range-id", keyRange.ID).
		Msg("etcdqdb: add key range")

	err := ops.AddKeyRangeWithChecks(ctx, qc.db, keyRange)
	if err != nil {
		return err
	}

	resp, err := qc.db.ListRouters(ctx)
	if err != nil {
		return err
	}

	// notify all routers
	for _, r := range resp {
		cc, err := DialRouter(&topology.Router{
			ID:      r.ID,
			Address: r.Addr(),
		})
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

		spqrlog.Zero.Debug().
			Interface("response", resp).
			Msg("add key range response")
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

	return keyRange, qc.traverseRouters(ctx, func(cc *grpc.ClientConn) error {
		cl := routerproto.NewKeyRangeServiceClient(cc)
		resp, err := cl.LockKeyRange(ctx, &routerproto.LockKeyRangeRequest{
			Id: []string{keyRangeID},
		})
		if err != nil {
			return err
		}

		spqrlog.Zero.Debug().
			Interface("response", resp).
			Msg("lock key range response")
		return nil
	})
}

func (qc *qdbCoordinator) Unlock(ctx context.Context, keyRangeID string) error {
	if err := qc.db.UnlockKeyRange(ctx, keyRangeID); err != nil {
		return err
	}
	return qc.traverseRouters(ctx, func(cc *grpc.ClientConn) error {
		cl := routerproto.NewKeyRangeServiceClient(cc)
		resp, err := cl.UnlockKeyRange(ctx, &routerproto.UnlockKeyRangeRequest{
			Id: []string{keyRangeID},
		})
		if err != nil {
			return err
		}

		spqrlog.Zero.Debug().
			Interface("response", resp).
			Msg("lock key range response")
		return nil
	})
}

// Split TODO: check bounds and keyRangeID (sourceID)
func (qc *qdbCoordinator) Split(ctx context.Context, req *kr.SplitKeyRange) error {
	var krOld *qdb.KeyRange
	var err error

	spqrlog.Zero.Debug().
		Str("krid", req.Krid).
		Interface("bound", req.Bound).
		Str("source-id", req.SourceID).
		Msg("split request is")

	if krOld, err = qc.db.LockKeyRange(ctx, req.SourceID); err != nil {
		return err
	}

	defer func() {
		if err := qc.db.UnlockKeyRange(ctx, req.SourceID); err != nil {
			spqrlog.Zero.Error().Err(err).Msg("")
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

	// splitting by X makes X the point of intersection
	// increase lower bound of new key range by 1
	//krNew.LowerBound = make([]byte, len(req.Bound))
	//copy(krNew.LowerBound, req.Bound)
	//krNew.LowerBound[len(krNew.LowerBound)-1]++

	spqrlog.Zero.Debug().
		Bytes("lower-bound", krNew.LowerBound).
		Bytes("upper-bound", krNew.UpperBound).
		Str("shard-id", krNew.ShardID).
		Str("id", krNew.ID).
		Msg("new key range")

	krOld.UpperBound = req.Bound
	if err := ops.ModifyKeyRangeWithChecks(ctx, qc.db, kr.KeyRangeFromDB(krOld)); err != nil {
		return err
	}

	if err := ops.AddKeyRangeWithChecks(ctx, qc.db, krNew); err != nil {
		return fmt.Errorf("failed to add a new key range: %w", err)
	}

	if err := qc.traverseRouters(ctx, func(cc *grpc.ClientConn) error {
		cl := routerproto.NewKeyRangeServiceClient(cc)
		resp, err := cl.SplitKeyRange(ctx, &routerproto.SplitKeyRangeRequest{
			Bound:        req.Bound,
			SourceId:     req.SourceID,
			KeyRangeInfo: krNew.ToProto(),
		})
		spqrlog.Zero.Debug().
			Interface("response", resp).
			Msg("drop key range response")
		return err
	}); err != nil {
		return err
	}

	return nil
}

func (qc *qdbCoordinator) DropKeyRangeAll(ctx context.Context) error {
	// TODO: exclusive lock all routers
	spqrlog.Zero.Debug().Msg("qdb coordinator dropping all key ranges")

	if err := qc.traverseRouters(ctx, func(cc *grpc.ClientConn) error {
		cl := routerproto.NewKeyRangeServiceClient(cc)
		resp, err := cl.DropAllKeyRanges(ctx, &routerproto.DropAllKeyRangesRequest{})
		spqrlog.Zero.Debug().
			Interface("response", resp).
			Msg("drop key range response")
		return err
	}); err != nil {
		return err
	}

	return qc.db.DropKeyRangeAll(ctx)
}

func (qc *qdbCoordinator) DropKeyRange(ctx context.Context, id string) error {
	// TODO: exclusive lock all routers
	spqrlog.Zero.Debug().Msg("qdb coordinator dropping all sharding keys")

	if err := qc.traverseRouters(ctx, func(cc *grpc.ClientConn) error {
		cl := routerproto.NewKeyRangeServiceClient(cc)
		resp, err := cl.DropKeyRange(ctx, &routerproto.DropKeyRangeRequest{
			Id: []string{id},
		})
		spqrlog.Zero.Debug().
			Interface("response", resp).
			Msg("drop key range response")
		return err
	}); err != nil {
		return err
	}

	// Drop key range from qdb.
	return qc.db.DropKeyRange(ctx, id)
}

func (qc *qdbCoordinator) DropShardingRule(ctx context.Context, id string) error {
	// TODO: exclusive lock all routers
	spqrlog.Zero.Debug().Msg("qdb coordinator dropping all sharding keys")

	if err := qc.traverseRouters(ctx, func(cc *grpc.ClientConn) error {
		cl := routerproto.NewShardingRulesServiceClient(cc)
		dropResp, err := cl.DropShardingRules(ctx, &routerproto.DropShardingRuleRequest{
			Id: []string{id},
		})

		spqrlog.Zero.Debug().
			Interface("response", dropResp).
			Msg("drop sharding rules response")
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
			spqrlog.Zero.Error().Err(err).Msg("")
		}
	}()

	krRight, err := qc.db.LockKeyRange(ctx, uniteKeyRange.KeyRangeIDRight)
	if err != nil {
		return err
	}

	defer func() {
		if err := qc.db.UnlockKeyRange(ctx, uniteKeyRange.KeyRangeIDRight); err != nil {
			spqrlog.Zero.Error().Err(err).Msg("")
		}
	}()

	krLeft.UpperBound = krRight.UpperBound

	if err := qc.db.DropKeyRange(ctx, krRight.KeyRangeID); err != nil {
		return fmt.Errorf("failed to drop an old key range: %w", err)
	}

	if err := ops.ModifyKeyRangeWithChecks(ctx, qc.db, kr.KeyRangeFromDB(krLeft)); err != nil {
		return fmt.Errorf("failed to update a new key range: %w", err)
	}

	if err := qc.traverseRouters(ctx, func(cc *grpc.ClientConn) error {
		cl := routerproto.NewKeyRangeServiceClient(cc)
		resp, err := cl.MergeKeyRange(ctx, &routerproto.MergeKeyRangeRequest{
			Bound: krRight.LowerBound,
		})

		spqrlog.Zero.Debug().
			Interface("response", resp).
			Msg("lock sharding rules response")
		return err
	}); err != nil {
		return err
	}

	return nil
}

// Move key range from one logical shard to another
func (qc *qdbCoordinator) Move(ctx context.Context, req *kr.MoveKeyRange) error {
	spqrlog.Zero.Debug().
		Str("key-range", req.Krid).
		Str("shard-id", req.ShardId).
		Msg("qdb coordinator move key range")

	//move i qdb
	if err := qc.traverseRouters(ctx, func(cc *grpc.ClientConn) error {
		cl := routerproto.NewKeyRangeServiceClient(cc)
		lockResp, err := cl.LockKeyRange(ctx, &routerproto.LockKeyRangeRequest{
			Id: []string{req.Krid},
		})

		spqrlog.Zero.Debug().
			Interface("response", lockResp).
			Msg("lock sharding rules response")
		return err
	}); err != nil {
		return err
	}

	defer func() {
		if err := qc.traverseRouters(ctx, func(cc *grpc.ClientConn) error {
			cl := routerproto.NewKeyRangeServiceClient(cc)
			unlockResp, err := cl.UnlockKeyRange(ctx, &routerproto.UnlockKeyRangeRequest{
				Id: []string{req.Krid},
			})

			spqrlog.Zero.Debug().
				Interface("response", unlockResp).
				Msg("unlock sharding rules response")
			return err
		}); err != nil {
			spqrlog.Zero.Error().Err(err).Msg("")
			return
		}
	}()

	krmv, err := qc.db.LockKeyRange(ctx, req.Krid)
	if err != nil {
		return err
	}
	defer func() {
		if err := qc.db.UnlockKeyRange(ctx, req.Krid); err != nil {
			spqrlog.Zero.Error().Err(err).Msg("")
		}
	}()

	//move between shards
	keyRange, _ := qc.db.GetKeyRange(ctx, req.Krid)
	shardingRules, _ := qc.ListShardingRules(ctx)
	err = datatransfers.MoveKeys(ctx, keyRange.ShardID, req.ShardId, *keyRange, shardingRules, &qc.db)
	if err != nil {
		spqrlog.Zero.Error().Msg("failed to move rows")
		return err
	}

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
		spqrlog.Zero.Debug().
			Interface("response", lockResp).
			Msg("lock key range response")

		defer func() {
			unlockResp, err := cl.UnlockKeyRange(ctx, &routerproto.UnlockKeyRangeRequest{
				Id: []string{req.Krid},
			})
			if err != nil {
				return
			}
			spqrlog.Zero.Debug().
				Interface("response", unlockResp).
				Msg("unlock key range response")
		}()
		moveResp, err := cl.MoveKeyRange(ctx, &routerproto.MoveKeyRangeRequest{
			KeyRange: kr.KeyRangeFromDB(krmv).ToProto(),
		})
		spqrlog.Zero.Debug().
			Interface("response", moveResp).
			Msg("move key range response")
		return err
	}); err != nil {
		return err
	}
	return nil
}

func (qc *qdbCoordinator) SyncRouterMetadata(ctx context.Context, qRouter *topology.Router) error {
	cc, err := DialRouter(qRouter)
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

	spqrlog.Zero.Debug().
		Interface("response", resp).
		Msg("add sharding rules response")

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

		spqrlog.Zero.Debug().
			Interface("response", resp).
			Msg("got response while adding key range")
	}

	rCl := routerproto.NewTopologyServiceClient(cc)
	if resp, err := rCl.OpenRouter(ctx, &routerproto.OpenRouterRequest{}); err != nil {
		return err
	} else {
		spqrlog.Zero.Debug().
			Interface("response", resp).
			Msg("open router response")
	}

	return nil
}

func (qc *qdbCoordinator) RegisterRouter(ctx context.Context, r *topology.Router) error {
	// TODO: list routers and deduplicate
	spqrlog.Zero.Debug().
		Str("address", r.Address).
		Str("router", r.ID).
		Msg("unregister router")
	return qc.db.AddRouter(ctx, qdb.NewRouter(r.Address, r.ID, qdb.CLOSED))
}

func (qc *qdbCoordinator) UnregisterRouter(ctx context.Context, rID string) error {
	spqrlog.Zero.Debug().
		Str("router", rID).
		Msg("unregister router")
	return qc.db.DeleteRouter(ctx, rID)
}

func (qc *qdbCoordinator) PrepareClient(nconn net.Conn) (client.Client, error) {
	cl := psqlclient.NewPsqlClient(nconn)

	if err := cl.Init(nil); err != nil {
		return nil, err
	}

	spqrlog.Zero.Info().
		Str("user", cl.Usr()).
		Str("db", cl.DB()).
		Msg("initialized client connection")

	if err := cl.AssignRule(&config.FrontendRule{
		AuthRule: &config.AuthCfg{
			Method: config.AuthOK,
		},
	}); err != nil {
		return nil, err
	}

	r := route.NewRoute(nil, nil, nil)
	r.SetParams(cl.Params())
	if err := cl.Auth(r); err != nil {
		return nil, err
	}
	spqrlog.Zero.Info().Msg("client auth OK")

	return cl, nil
}

func (qc *qdbCoordinator) ProcClient(ctx context.Context, nconn net.Conn) error {
	cl, err := qc.PrepareClient(nconn)
	if err != nil {
		spqrlog.Zero.Error().Err(err).Msg("")
		return err
	}

	ci := grpcConnectionIterator{qdbCoordinator: qc}
	cli := clientinteractor.NewPSQLInteractor(cl)
	for {
		// TODO: check leader status
		msg, err := cl.Receive()
		if err != nil {
			spqrlog.Zero.Error().
				Err(err).
				Msg("failed to receive message")
			return err
		}
		spqrlog.Zero.Debug().
			Interface("message", msg).
			Msg("received message")

		switch v := msg.(type) {
		case *pgproto3.Query:
			tstmt, err := spqrparser.Parse(v.String)
			if err != nil {
				_ = cli.ReportError(err)
				continue
			}

			spqrlog.Zero.Info().
				Str("query", v.String).
				Type("type", tstmt).
				Msg("parsed statement is")

			if err := meta.Proc(ctx, tstmt, qc, ci, cli); err != nil {
				spqrlog.Zero.Error().Err(err).Msg("")
				_ = cli.ReportError(err)
			} else {
				spqrlog.Zero.Debug().Msg("processed OK")
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
