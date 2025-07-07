package coord

import (
	"context"
	"crypto/tls"
	"encoding/binary"
	"fmt"
	"math"
	"net"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/pg-sharding/spqr/coordinator"
	"github.com/pg-sharding/spqr/coordinator/provider"
	"github.com/pg-sharding/spqr/coordinator/statistics"
	"github.com/pg-sharding/spqr/pkg/client"
	"github.com/pg-sharding/spqr/pkg/clientinteractor"
	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/connmgr"
	"github.com/pg-sharding/spqr/pkg/datatransfers"
	"github.com/pg-sharding/spqr/pkg/meta"
	"github.com/pg-sharding/spqr/pkg/models/distributions"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/pkg/models/rrelation"
	"github.com/pg-sharding/spqr/pkg/models/spqrerror"
	"github.com/pg-sharding/spqr/pkg/models/tasks"
	"github.com/pg-sharding/spqr/pkg/models/topology"
	"github.com/pg-sharding/spqr/pkg/pool"
	routerproto "github.com/pg-sharding/spqr/pkg/protos"
	"github.com/pg-sharding/spqr/pkg/rulemgr"
	"github.com/pg-sharding/spqr/pkg/shard"
	"github.com/pg-sharding/spqr/pkg/spqrlog"
	"github.com/pg-sharding/spqr/pkg/tsa"
	"github.com/pg-sharding/spqr/qdb"
	"github.com/pg-sharding/spqr/qdb/ops"
	"github.com/pg-sharding/spqr/router/cache"
	rclient "github.com/pg-sharding/spqr/router/client"
	"github.com/pg-sharding/spqr/router/port"
	"github.com/pg-sharding/spqr/router/route"
	spqrparser "github.com/pg-sharding/spqr/yacc/console"
	"google.golang.org/grpc"
)

type grpcConnMgr struct {
	*ClusteredCoordinator
}

// InstanceHealthChecks implements connmgr.ConnectionStatsMgr.
func (ci grpcConnMgr) InstanceHealthChecks() map[config.Host]tsa.TimedCheckResult {
	return map[config.Host]tsa.TimedCheckResult{}
}

// TODO implement it
// ActiveTcpCount implements connectiterator.ConnectIterator.
func (ci grpcConnMgr) ActiveTcpCount() int64 {
	return 0
}

// TODO implement it
// TotalCancelCount implements connectiterator.ConnectIterator.
func (ci grpcConnMgr) TotalCancelCount() int64 {
	return 0
}

// TODO implement it
// TotalTcpCount implements connectiterator.ConnectIterator.
func (ci grpcConnMgr) TotalTcpCount() int64 {
	return 0
}

// TODO : unit tests
func (ci grpcConnMgr) IterRouter(cb func(cc *grpc.ClientConn, addr string) error) error {
	ctx := context.TODO()
	rtrs, err := ci.ClusteredCoordinator.QDB().ListRouters(ctx)

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
		defer func() {
			if err := cc.Close(); err != nil {
				spqrlog.Zero.Debug().Err(err).Msg("failed to close connection")
			}
		}()

		if err := cb(cc, r.Address); err != nil {
			return err
		}
		if err := cc.Close(); err != nil {
			return err
		}
	}
	return nil
}

// TODO : unit tests
func (ci grpcConnMgr) ClientPoolForeach(cb func(client client.ClientInfo) error) error {
	return ci.IterRouter(func(cc *grpc.ClientConn, addr string) error {
		ctx := context.TODO()
		rrClient := routerproto.NewClientInfoServiceClient(cc)

		spqrlog.Zero.Debug().Msg("fetch clients with grpc")
		resp, err := rrClient.ListClients(ctx, nil)
		if err != nil {
			spqrlog.Zero.Error().Msg("error fetching clients with grpc")
			return err
		}

		for _, client := range resp.Clients {
			err = cb(rclient.NewNoopClient(client, addr))
			if err != nil {
				return err
			}
		}
		return nil
	})
}

// TODO : implement
// TODO : unit tests
func (ci grpcConnMgr) Put(client client.Client) error {
	return spqrerror.New(spqrerror.SPQR_NOT_IMPLEMENTED, "grpcConnectionIterator put not implemented")
}

// TODO : implement
// TODO : unit tests
func (ci grpcConnMgr) Pop(id uint) (bool, error) {
	return true, spqrerror.New(spqrerror.SPQR_NOT_IMPLEMENTED, "grpcConnectionIterator pop not implemented")
}

// TODO : implement
// TODO : unit tests
func (ci grpcConnMgr) Shutdown() error {
	return spqrerror.New(spqrerror.SPQR_NOT_IMPLEMENTED, "grpcConnectionIterator shutdown not implemented")
}

// TODO : unit tests
func (ci grpcConnMgr) ForEach(cb func(sh shard.ShardHostInfo) error) error {
	return ci.IterRouter(func(cc *grpc.ClientConn, addr string) error {
		ctx := context.TODO()
		rrBackConn := routerproto.NewBackendConnectionsServiceClient(cc)

		spqrlog.Zero.Debug().Msg("fetch clients with grpc")
		resp, err := rrBackConn.ListBackendConnections(ctx, nil)
		if err != nil {
			spqrlog.Zero.Error().Msg("error fetching clients with grpc")
			return err
		}

		for _, conn := range resp.Conns {
			err = cb(provider.NewCoordShardInfo(conn, addr))
			if err != nil {
				return err
			}
		}
		return nil
	})
}

// TODO : unit tests
func (ci grpcConnMgr) ForEachPool(cb func(p pool.Pool) error) error {
	return ci.IterRouter(func(cc *grpc.ClientConn, addr string) error {
		ctx := context.TODO()
		rrBackConn := routerproto.NewPoolServiceClient(cc)

		spqrlog.Zero.Debug().Msg("fetch pools with grpc")
		resp, err := rrBackConn.ListPools(ctx, nil)
		if err != nil {
			spqrlog.Zero.Error().Msg("error fetching pools with grpc")
			return err
		}

		for _, p := range resp.Pools {
			err = cb(pool.NewPoolView(p))
			if err != nil {
				return err
			}
		}
		return nil
	})
}

var _ connmgr.ConnectionStatsMgr = &grpcConnMgr{}

func DialRouter(r *topology.Router) (*grpc.ClientConn, error) {
	spqrlog.Zero.Debug().
		Str("router-id", r.ID).
		Msg("dialing router")
	// TODO: add creds
	return grpc.NewClient(r.Address, grpc.WithInsecure()) //nolint:all
}

const defaultWatchRouterTimeout = time.Second
const defaultLockCoordinatorTimeout = time.Second

/*
* This is the global coordinator, which manages the spqr cluster as a whole.
* Its method calls result in cluster-wide changes.
 */
type ClusteredCoordinator struct {
	Coordinator
	rmgr         rulemgr.RulesMgr
	tlsconfig    *tls.Config
	db           qdb.XQDB
	cache        *cache.SchemaCache
	acquiredLock bool
}

func (qc *ClusteredCoordinator) QDB() qdb.QDB {
	return qc.db
}

func (qc *ClusteredCoordinator) Cache() *cache.SchemaCache {
	return qc.cache
}

var _ coordinator.Coordinator = &ClusteredCoordinator{}

// watchRouters traverse routers one check if they are opened
// for clients. If not, initialize metadata and open router
// TODO : unit tests
func (qc *ClusteredCoordinator) watchRouters(ctx context.Context) {
	spqrlog.Zero.Debug().Msg("start routers watch iteration")
	for {
		// TODO check we are still coordinator

		// TODO: lock router
		rtrs, err := qc.db.ListRouters(ctx)
		if err != nil {
			spqrlog.Zero.Error().Err(err).Msg("")
			time.Sleep(time.Second)
			continue
		}

		// TODO we have to rewrite this code
		// instead of opening new connections to each router
		// we have to open it ones, keep and update before the iteration
		for _, r := range rtrs {
			if err := func() error {
				internalR := &topology.Router{
					ID:      r.ID,
					Address: r.Address,
				}

				cc, err := DialRouter(internalR)
				if err != nil {
					return err
				}

				defer func() {
					if err := cc.Close(); err != nil {
						spqrlog.Zero.Debug().Err(err).Msg("failed to close connection")
					}
				}()

				rrClient := routerproto.NewTopologyServiceClient(cc)

				resp, err := rrClient.GetRouterStatus(ctx, nil)
				if err != nil {
					return err
				}

				switch resp.Status {
				case routerproto.RouterStatus_CLOSED:
					spqrlog.Zero.Debug().Msg("router is closed")
					if err := qc.SyncRouterCoordinatorAddress(ctx, internalR); err != nil {
						return err
					}

					/* Mark router as opened in qdb */
					err := qc.db.CloseRouter(ctx, internalR.ID)
					if err != nil {
						return err
					}

				case routerproto.RouterStatus_OPENED:
					spqrlog.Zero.Debug().Msg("router is opened")

					/* TODO: check router metadata consistency */
					if err := qc.SyncRouterCoordinatorAddress(ctx, internalR); err != nil {
						return err
					}

					/* Mark router as opened in qdb */
					err := qc.db.OpenRouter(ctx, internalR.ID)
					if err != nil {
						return err
					}
				}
				return nil
			}(); err != nil {
				spqrlog.Zero.Error().
					Str("router id", r.ID).
					Err(err).
					Msg("watch routers iteration failed on router")
			}
		}

		time.Sleep(config.ValueOrDefaultDuration(config.CoordinatorConfig().IterationTimeout, defaultWatchRouterTimeout))
	}
}

func NewClusteredCoordinator(tlsconfig *tls.Config, db qdb.XQDB) (*ClusteredCoordinator, error) {
	if config.CoordinatorConfig().ShardDataCfg != "" {
		shards, err := config.LoadShardDataCfg(config.CoordinatorConfig().ShardDataCfg)
		if err != nil {
			return nil, err
		}

		if shards != nil {
			for id, cfg := range shards.ShardsData {
				if err := db.AddShard(context.TODO(), qdb.NewShard(id, cfg.Hosts)); err != nil {
					return nil, err
				}
			}
		}
	}

	return &ClusteredCoordinator{
		Coordinator:  NewCoordinator(db),
		db:           db,
		tlsconfig:    tlsconfig,
		rmgr:         rulemgr.NewMgr(config.CoordinatorConfig().FrontendRules, []*config.BackendRule{}),
		acquiredLock: false,
	}, nil
}

// TODO : unit tests
func (qc *ClusteredCoordinator) lockCoordinator(ctx context.Context, initialRouter bool) error {
	updateCoordinator := func() error {
		if !initialRouter {
			return nil
		}
		routerHost, err := config.GetHostOrHostname(config.RouterConfig().Host)
		if err != nil {
			return err
		}
		router := &topology.Router{
			ID:      uuid.NewString(),
			Address: net.JoinHostPort(routerHost, config.RouterConfig().GrpcApiPort),
			State:   qdb.OPENED,
		}
		if err := qc.RegisterRouter(ctx, router); err != nil {
			spqrlog.Zero.Error().Err(err).Msg("register router when locking coordinator")
		}

		host, err := config.GetHostOrHostname(config.CoordinatorConfig().Host)
		if err != nil {
			return err
		}
		coordAddr := net.JoinHostPort(host, config.CoordinatorConfig().GrpcApiPort)
		if err := qc.UpdateCoordinator(ctx, coordAddr); err != nil {
			return err
		}
		return nil
	}
	defer func() {
		qc.acquiredLock = true
	}()

	if qc.db.TryCoordinatorLock(context.TODO()) != nil {
		for {
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(time.Second):
				if err := qc.db.TryCoordinatorLock(context.TODO()); err == nil {
					return updateCoordinator()
				} else {
					spqrlog.Zero.Error().Err(err).Msg("qdb already taken, waiting for connection")
					/* retry lock attempt */
				}
			}
		}
	}

	return updateCoordinator()
}

// RunCoordinator side effect: it runs an asynchronous goroutine
// that checks the availability of the SPQR router
//
// TODO: unit tests
func (qc *ClusteredCoordinator) RunCoordinator(ctx context.Context, initialRouter bool) {
	for err := qc.lockCoordinator(ctx, initialRouter); err != nil; {
		spqrlog.Zero.Error().Err(err).Msg("error getting qdb lock, retrying")

		time.Sleep(config.ValueOrDefaultDuration(config.CoordinatorConfig().LockIterationTimeout, defaultLockCoordinatorTimeout))
		continue
	}

	if err := qc.finishRedistributeTasksInProgress(ctx); err != nil {
		spqrlog.Zero.Error().Err(err).Msg("unable to finish redistribution tasks in progress")
	}

	if err := qc.finishMoveTasksInProgress(ctx); err != nil {
		spqrlog.Zero.Error().Err(err).Msg("unable to finish move tasks in progress")
	}

	ranges, err := qc.db.ListAllKeyRanges(context.TODO())
	if err != nil {
		spqrlog.Zero.Error().
			Err(err).
			Msg("failed to list key ranges")
	}

	// Finish any key range move or data transfer transaction in progress
	for _, r := range ranges {
		move, err := qc.GetKeyRangeMove(context.TODO(), r.KeyRangeID)
		if err != nil {
			spqrlog.Zero.Error().Err(err).Msg("error getting key range move from qdb")
		}

		tx, err := qc.db.GetTransferTx(context.TODO(), r.KeyRangeID)
		if err != nil {
			spqrlog.Zero.Error().Err(err).Msg("error getting data transfer transaction from qdb")
		}

		var krm *kr.MoveKeyRange
		if move != nil {
			krm = &kr.MoveKeyRange{
				Krid:    move.KeyRangeID,
				ShardId: move.ShardId,
			}
		} else if tx != nil {
			krm = &kr.MoveKeyRange{
				Krid:    r.KeyRangeID,
				ShardId: tx.ToShardId,
			}
		}

		if krm != nil {
			err = qc.Move(context.TODO(), krm)
			if err != nil {
				spqrlog.Zero.Error().Err(err).Msg("error moving key range")
			}
		}
	}

	go qc.watchRouters(context.TODO())
}

// TODO : unit tests
// traverseRouters traverse each route and run callback for each of them
// cb receives grpc connection to router`s admin console
func (qc *ClusteredCoordinator) traverseRouters(ctx context.Context, cb func(cc *grpc.ClientConn) error) error {
	spqrlog.Zero.Debug().Msg("qdb coordinator traverse")
	t := time.Now()

	rtrs, err := qc.db.ListRouters(ctx)
	if err != nil {
		return err
	}

	for _, rtr := range rtrs {
		if err := func() error {
			if rtr.State != qdb.OPENED {
				return spqrerror.New(spqrerror.SPQR_ROUTER_ERROR, "router is closed")
			}

			// TODO: run cb`s async
			cc, err := DialRouter(&topology.Router{
				ID:      rtr.ID,
				Address: rtr.Addr(),
			})
			if err != nil {
				return err
			}
			defer func() {
				if err := cc.Close(); err != nil {
					spqrlog.Zero.Debug().Err(err).Msg("failed to close connection")
				}
			}()

			if err := cb(cc); err != nil {
				spqrlog.Zero.Debug().Err(err).Str("router id", rtr.ID).Msg("traverse routers")
				return err
			}
			return nil
		}(); err != nil {
			return err
		}
	}

	statistics.RecordRouterOperation(time.Since(t))
	return nil
}

// TODO : unit tests
func (qc *ClusteredCoordinator) AddRouter(ctx context.Context, router *topology.Router) error {
	return qc.db.AddRouter(ctx, topology.RouterToDB(router))
}

// TODO : unit tests
func (qc *ClusteredCoordinator) CreateKeyRange(ctx context.Context, keyRange *kr.KeyRange) error {
	// add key range to metadb
	spqrlog.Zero.Debug().
		Bytes("lower-bound", keyRange.Raw()[0]).
		Str("shard-id", keyRange.ShardID).
		Str("key-range-id", keyRange.ID).
		Msg("add key range")

	if err := qc.Coordinator.CreateKeyRange(ctx, keyRange); err != nil {
		return err
	}

	return qc.traverseRouters(ctx, func(cc *grpc.ClientConn) error {
		cl := routerproto.NewKeyRangeServiceClient(cc)
		resp, err := cl.CreateKeyRange(ctx, &routerproto.CreateKeyRangeRequest{
			KeyRangeInfo: keyRange.ToProto(),
		})
		spqrlog.Zero.Debug().Err(err).
			Interface("response", resp).
			Msg("add key range response")
		return err
	})
}

// TODO : unit tests
func (qc *ClusteredCoordinator) MoveKeyRange(ctx context.Context, keyRange *kr.KeyRange) error {
	return ops.ModifyKeyRangeWithChecks(ctx, qc.db, keyRange)
}

// TODO : unit tests
func (qc *ClusteredCoordinator) LockKeyRange(ctx context.Context, keyRangeID string) (*kr.KeyRange, error) {
	keyRange, err := qc.Coordinator.LockKeyRange(ctx, keyRangeID)
	if err != nil {
		return nil, err
	}

	return keyRange, qc.traverseRouters(ctx, func(cc *grpc.ClientConn) error {
		cl := routerproto.NewKeyRangeServiceClient(cc)
		resp, err := cl.LockKeyRange(ctx, &routerproto.LockKeyRangeRequest{
			Id: []string{keyRangeID},
		})

		spqrlog.Zero.Debug().Err(err).
			Interface("response", resp).
			Msg("lock key range response")
		return err
	})
}

// TODO : unit tests
func (qc *ClusteredCoordinator) UnlockKeyRange(ctx context.Context, keyRangeID string) error {
	if err := qc.Coordinator.UnlockKeyRange(ctx, keyRangeID); err != nil {
		return err
	}
	return qc.traverseRouters(ctx, func(cc *grpc.ClientConn) error {
		cl := routerproto.NewKeyRangeServiceClient(cc)
		resp, err := cl.UnlockKeyRange(ctx, &routerproto.UnlockKeyRangeRequest{
			Id: []string{keyRangeID},
		})

		spqrlog.Zero.Debug().Err(err).
			Interface("response", resp).
			Msg("unlock key range response")

		return err
	})
}

// Split splits key range by req.bound
// TODO : unit tests
func (qc *ClusteredCoordinator) Split(ctx context.Context, req *kr.SplitKeyRange) error {
	if err := qc.Coordinator.Split(ctx, req); err != nil {
		return err
	}

	if err := qc.traverseRouters(ctx, func(cc *grpc.ClientConn) error {
		cl := routerproto.NewKeyRangeServiceClient(cc)
		resp, err := cl.SplitKeyRange(ctx, &routerproto.SplitKeyRangeRequest{
			Bound:     req.Bound[0], // fix multidim case
			SourceId:  req.SourceID,
			NewId:     req.Krid,
			SplitLeft: req.SplitLeft,
		})
		spqrlog.Zero.Debug().Err(err).
			Interface("response", resp).
			Msg("split key range response")
		return err
	}); err != nil {
		return err
	}

	return nil
}

// TODO : unit tests
func (qc *ClusteredCoordinator) DropKeyRangeAll(ctx context.Context) error {
	// TODO: exclusive lock all routers
	spqrlog.Zero.Debug().Msg("qdb coordinator dropping all key ranges")

	if err := qc.traverseRouters(ctx, func(cc *grpc.ClientConn) error {
		cl := routerproto.NewKeyRangeServiceClient(cc)
		resp, err := cl.DropAllKeyRanges(ctx, nil)
		spqrlog.Zero.Debug().Err(err).
			Interface("response", resp).
			Msg("drop key range response")
		return err
	}); err != nil {
		return err
	}

	return qc.Coordinator.DropKeyRangeAll(ctx)
}

// TODO : unit tests
func (qc *ClusteredCoordinator) DropKeyRange(ctx context.Context, id string) error {
	// TODO: exclusive lock all routers
	spqrlog.Zero.Debug().Msg("qdb coordinator dropping all sharding keys")

	if err := qc.traverseRouters(ctx, func(cc *grpc.ClientConn) error {
		cl := routerproto.NewKeyRangeServiceClient(cc)
		resp, err := cl.DropKeyRange(ctx, &routerproto.DropKeyRangeRequest{
			Id: []string{id},
		})
		spqrlog.Zero.Debug().Err(err).
			Interface("response", resp).
			Msg("drop key range response")
		return err
	}); err != nil {
		return err
	}

	// Drop key range from qdb.
	return qc.Coordinator.DropKeyRange(ctx, id)
}

// TODO : unit tests
func (qc *ClusteredCoordinator) Unite(ctx context.Context, uniteKeyRange *kr.UniteKeyRange) error {
	if err := qc.Coordinator.Unite(ctx, uniteKeyRange); err != nil {
		return err
	}

	if err := qc.traverseRouters(ctx, func(cc *grpc.ClientConn) error {
		cl := routerproto.NewKeyRangeServiceClient(cc)
		resp, err := cl.MergeKeyRange(ctx, &routerproto.MergeKeyRangeRequest{
			BaseId:      uniteKeyRange.BaseKeyRangeId,
			AppendageId: uniteKeyRange.AppendageKeyRangeId,
		})

		spqrlog.Zero.Debug().Err(err).
			Interface("response", resp).
			Msg("merge key range response")
		return err
	}); err != nil {
		return err
	}

	return nil
}

// TODO : unit tests
func (qc *ClusteredCoordinator) RecordKeyRangeMove(ctx context.Context, m *qdb.MoveKeyRange) (string, error) {
	ls, err := qc.db.ListKeyRangeMoves(ctx)
	if err != nil {
		return "", err
	}
	for _, krm := range ls {
		// after the coordinator restarts, it will continue the move that was previously initiated.
		// key range move already exist for this key range
		// complete it first
		if krm.KeyRangeID == m.KeyRangeID && krm.Status != qdb.MoveKeyRangeComplete {
			return krm.KeyRangeID, nil
		}
	}

	/* record new planned key range move */
	if err := qc.db.RecordKeyRangeMove(ctx, m); err != nil {
		return "", err
	}

	return m.MoveId, nil
}

func (qc *ClusteredCoordinator) GetKeyRangeMove(ctx context.Context, krId string) (*qdb.MoveKeyRange, error) {
	ls, err := qc.db.ListKeyRangeMoves(ctx)
	if err != nil {
		return nil, err
	}

	for _, krm := range ls {
		// after the coordinator restarts, it will continue the move that was previously initiated.
		// key range move already exist for this key range
		// complete it first
		if krm.KeyRangeID == krId {
			return krm, nil
		}
	}

	return nil, nil
}

// Move key range from one logical shard to another
// This function re-shards data by locking a portion of it,
// making it unavailable for read and write access during the process.
// TODO : unit tests
func (qc *ClusteredCoordinator) Move(ctx context.Context, req *kr.MoveKeyRange) error {
	// First, we create a record in the qdb to track the data movement.
	// If the coordinator crashes during the process, we need to rerun this function.

	spqrlog.Zero.Debug().
		Str("key-range", req.Krid).
		Str("shard-id", req.ShardId).
		Msg("qdb coordinator move key range")

	keyRange, err := qc.GetKeyRange(ctx, req.Krid)
	if err != nil {
		return err
	}

	// no need to move data to the same shard
	if keyRange.ShardID == req.ShardId {
		return nil
	}

	move, err := qc.GetKeyRangeMove(ctx, req.Krid)
	if err != nil {
		return err
	}

	if move == nil {
		// No key range moves in progress
		move = &qdb.MoveKeyRange{
			MoveId:     uuid.NewString(),
			ShardId:    req.ShardId,
			KeyRangeID: req.Krid,
			Status:     qdb.MoveKeyRangePlanned,
		}
		_, err = qc.RecordKeyRangeMove(ctx, move)
		if err != nil {
			return err
		}
	}

	for move != nil {
		switch move.Status {
		case qdb.MoveKeyRangePlanned:
			// lock the key range
			_, err = qc.LockKeyRange(ctx, req.Krid)
			if err != nil {
				return err
			}
			if err = qc.db.UpdateKeyRangeMoveStatus(ctx, move.MoveId, qdb.MoveKeyRangeStarted); err != nil {
				return err
			}
			move.Status = qdb.MoveKeyRangeStarted
		case qdb.MoveKeyRangeStarted:
			// move the data
			ds, err := qc.GetDistribution(ctx, keyRange.Distribution)
			if err != nil {
				return err
			}

			t := time.Now()
			err = datatransfers.MoveKeys(ctx, keyRange.ShardID, req.ShardId, keyRange, ds, qc.db, qc)
			if err != nil {
				spqrlog.Zero.Error().Err(err).Msg("failed to move rows")
				return err
			}
			statistics.RecordShardOperation(time.Since(t))

			// update key range
			krg, err := qc.GetKeyRange(ctx, req.Krid)
			if err != nil {
				return err
			}
			krg.ShardID = req.ShardId
			if err := ops.ModifyKeyRangeWithChecks(ctx, qc.db, krg); err != nil {
				// TODO: check if unlock here is ok
				return err
			}

			// Notify all routers about scheme changes.
			if err := qc.traverseRouters(ctx, func(cc *grpc.ClientConn) error {
				cl := routerproto.NewKeyRangeServiceClient(cc)
				moveResp, err := cl.MoveKeyRange(ctx, &routerproto.MoveKeyRangeRequest{
					Id:        krg.ID,
					ToShardId: krg.ShardID,
				})
				spqrlog.Zero.Debug().Err(err).
					Interface("response", moveResp).
					Msg("move key range response")
				return err
			}); err != nil {
				return err
			}

			if err := qc.db.UpdateKeyRangeMoveStatus(ctx, move.MoveId, qdb.MoveKeyRangeComplete); err != nil {
				spqrlog.Zero.Error().Err(err).Msg("")
			}
			move.Status = qdb.MoveKeyRangeComplete
		case qdb.MoveKeyRangeComplete:
			// unlock key range
			if err := qc.UnlockKeyRange(ctx, req.Krid); err != nil {
				spqrlog.Zero.Error().Err(err).Msg("")
			}
			if err := qc.db.DeleteKeyRangeMove(ctx, move.MoveId); err != nil {
				return err
			}
			move = nil
		default:
			return fmt.Errorf("unknown key range move status: \"%s\"", move.Status)
		}
	}
	return nil
}

// checkKeyRangeMove checks the ability to execute given kr.BatchMoveKeyRange request.
//
// Parameters:
//   - ctx (context.Context): The context for QDB operations.
//   - req (*kr.BatchMoveKeyRange): The move request to check.
//
// Returns:
//   - error: An error if any occurred.
func (qc *ClusteredCoordinator) checkKeyRangeMove(ctx context.Context, req *kr.BatchMoveKeyRange) error {
	keyRange, err := qc.GetKeyRange(ctx, req.KrId)
	if err != nil {
		return err
	}
	if keyRange.ShardID == req.ShardId {
		return nil
	}
	if _, err = qc.GetKeyRange(ctx, req.DestKrId); err == nil {
		return spqrerror.Newf(spqrerror.SPQR_TRANSFER_ERROR, "key range \"%s\" already exists", req.DestKrId)
	}
	conns, err := config.LoadShardDataCfg(config.CoordinatorConfig().ShardDataCfg)
	if err != nil {
		return err
	}
	destShardConn, ok := conns.ShardsData[req.ShardId]
	if !ok {
		return spqrerror.New(spqrerror.SPQR_METADATA_CORRUPTION, fmt.Sprintf("destination shard of key range '%s' does not exist in shard data config", keyRange.ID))
	}
	destConn, err := datatransfers.GetMasterConnection(ctx, destShardConn)
	if err != nil {
		return err
	}

	sourceShardConn, ok := conns.ShardsData[keyRange.ShardID]
	if !ok {
		return spqrerror.New(spqrerror.SPQR_METADATA_CORRUPTION, fmt.Sprintf("shard of key range '%s' does not exist in shard data config", keyRange.ID))
	}
	sourceConn, err := datatransfers.GetMasterConnection(ctx, sourceShardConn)
	if err != nil {
		return err
	}

	ds, err := qc.GetDistribution(ctx, keyRange.Distribution)
	if err != nil {
		return err
	}

	schemas := make(map[string]struct{})
	for _, rel := range ds.Relations {
		schemas[rel.GetSchema()] = struct{}{}
		relName := strings.ToLower(rel.Name)
		sourceTable, err := datatransfers.CheckTableExists(ctx, sourceConn, relName, rel.GetSchema())
		if err != nil {
			return err
		}
		if !sourceTable {
			spqrlog.Zero.Info().Str("rel", rel.GetFullName()).Msg("source table does not exist")
			return spqrerror.Newf(spqrerror.SPQR_TRANSFER_ERROR, "relation \"%s\" does not exist on the source shard, possible misconfiguration of schema names", rel.GetFullName())
		}
		for _, col := range rel.DistributionKey {
			exists, err := datatransfers.CheckColumnExists(ctx, sourceConn, relName, rel.GetSchema(), col.Column)
			if err != nil {
				return err
			}
			if !exists {
				return spqrerror.Newf(spqrerror.SPQR_TRANSFER_ERROR, "distribution key column \"%s\" not found in relation \"%s\" on source shard", col.Column, rel.GetFullName())
			}
		}
		destTable, err := datatransfers.CheckTableExists(ctx, destConn, relName, rel.GetSchema())
		if err != nil {
			return err
		}
		if !destTable {
			return spqrerror.Newf(spqrerror.SPQR_TRANSFER_ERROR, "relation \"%s\" does not exist on the destination shard", rel.GetFullName())
		}
		// TODO check whole table schema for compatibility
		for _, col := range rel.DistributionKey {
			exists, err := datatransfers.CheckColumnExists(ctx, destConn, relName, rel.GetSchema(), col.Column)
			if err != nil {
				return err
			}
			if !exists {
				return spqrerror.Newf(spqrerror.SPQR_TRANSFER_ERROR, "distribution key column \"%s\" not found in relation \"%s\" on destination shard", col.Column, rel.GetFullName())
			}
		}
	}

	return datatransfers.SetupFDW(ctx, sourceConn, destConn, keyRange.ShardID, req.ShardId, schemas)
}

// TODO : unit tests

// BatchMoveKeyRange moves specified amount of keys from a key range to another shard.
//
// Parameters:
//   - ctx (context.Context): The context of the operation.
//   - req (*kr.BatchMoveKeyRange): The request with args for the operation.
//
// Returns:
//   - error: Any error occurred during transfer.
func (qc *ClusteredCoordinator) BatchMoveKeyRange(ctx context.Context, req *kr.BatchMoveKeyRange) error {
	if err := statistics.RecordMoveStart(time.Now()); err != nil {
		spqrlog.Zero.Error().Err(err).Msg("failed to record key range move start in statistics")
	}
	keyRange, err := qc.GetKeyRange(ctx, req.KrId)
	if err != nil {
		return err
	}
	if keyRange.ShardID == req.ShardId {
		return nil
	}
	if _, err = qc.GetKeyRange(ctx, req.DestKrId); err == nil {
		return spqrerror.Newf(spqrerror.SPQR_TRANSFER_ERROR, "key range \"%s\" already exists", req.DestKrId)
	}
	ds, err := qc.GetDistribution(ctx, keyRange.Distribution)
	if err != nil {
		return err
	}
	nextRange, err := qc.getNextKeyRange(ctx, keyRange)
	var nextBound kr.KeyRangeBound
	if nextRange != nil {
		nextBound = nextRange.LowerBound
	}
	if err != nil {
		return err
	}

	// Get connection to source shard's master
	conns, err := config.LoadShardDataCfg(config.CoordinatorConfig().ShardDataCfg)
	if err != nil {
		return err
	}
	if _, ok := conns.ShardsData[keyRange.ShardID]; !ok {
		return spqrerror.New(spqrerror.SPQR_METADATA_CORRUPTION, fmt.Sprintf("shard of key range '%s' does not exist in shard data config", keyRange.ID))
	}
	sourceShardConn := conns.ShardsData[keyRange.ShardID]
	sourceConn, err := datatransfers.GetMasterConnection(ctx, sourceShardConn)
	if err != nil {
		return err
	}

	totalCount, relCount, err := qc.getKeyStats(ctx, sourceConn, ds.Relations, keyRange, nextBound)
	if err != nil {
		return err
	}
	var taskGroup *tasks.MoveTaskGroup
	if totalCount != 0 {
		biggestRelName, coeff := qc.getBiggestRelation(relCount, totalCount)
		biggestRel := ds.Relations[biggestRelName]
		cond, err := kr.GetKRCondition(biggestRel, keyRange, nextBound, "")
		if err != nil {
			return err
		}
		taskGroup, err = qc.getMoveTasks(ctx, sourceConn, req, biggestRel, cond, coeff, ds)
		if err != nil {
			return err
		}
	} else {
		// If no keys, move the whole key range
		taskGroup = &tasks.MoveTaskGroup{
			KrIdFrom:  req.KrId,
			KrIdTo:    req.DestKrId,
			ShardToId: req.ShardId,
			Type:      req.Type,
			Tasks: []*tasks.MoveTask{
				{
					KrIdTemp: req.DestKrId,
					State:    tasks.TaskPlanned,
					Bound:    nil,
				},
			},
		}
	}

	if err := qc.WriteMoveTaskGroup(ctx, taskGroup); err != nil {
		return err
	}

	execCtx := context.TODO()
	ch := make(chan error)
	go func() {
		ch <- qc.executeMoveTasks(execCtx, taskGroup)
	}()

	for {
		select {
		case <-ctx.Done():
			return spqrerror.NewByCode(spqrerror.SPQR_TRANSFER_ERROR)
		case err := <-ch:
			if statErr := statistics.RecordMoveFinish(time.Now()); statErr != nil {
				spqrlog.Zero.Error().Err(err).Msg("failed to record key range move finish in statistics")
			}
			return err
		}
	}
}

// TODO : unit tests

// getKeyStats gets information about the amount of keys belonging to a key range in the specified relations.
//
// Parameters:
//   - ctx (context.Context): The context for queries to the database.
//   - conn (*pgx.Conn): The connection to the database.
//   - relations (map[string]*distributions.DistributedRelation): The relations to collect the information about.
//   - keyRange (*kr.KeyRange): The key range to collect the information about.
//   - nextBound (kr.KeyRangeBound): THe bound of the next key range. If there's no next key range, must be nil.
//
// Returns:
//   - totalCount (int64): The total amount of keys in all relations.
//   - relationCount (map[string]int64): The amount of keys by relation name.
//   - err: An error if any occurred.
func (*ClusteredCoordinator) getKeyStats(
	ctx context.Context,
	conn *pgx.Conn,
	relations map[string]*distributions.DistributedRelation,
	keyRange *kr.KeyRange,
	nextBound kr.KeyRangeBound,
) (totalCount int64, relationCount map[string]int64, err error) {
	t := time.Now()
	relationCount = make(map[string]int64)
	for _, rel := range relations {
		relExists, err := datatransfers.CheckTableExists(ctx, conn, strings.ToLower(rel.Name), rel.GetSchema())
		if err != nil {
			return 0, nil, err
		}
		if !relExists {
			continue
		}

		cond, err := kr.GetKRCondition(rel, keyRange, nextBound, "")
		if err != nil {
			return 0, nil, err
		}
		query := fmt.Sprintf(`
				SELECT count(*) 
				FROM %s as t
				WHERE %s;
`, rel.GetFullName(), cond)
		row := conn.QueryRow(ctx, query)
		var count int64
		if err = row.Scan(&count); err != nil {
			return 0, nil, err
		}
		relationCount[rel.Name] = count
		totalCount += count
	}
	statistics.RecordShardOperation(time.Since(t))
	return
}

// TODO: unit tests

// getBiggestRelation finds the relatively biggest relation and the proportion of it to the total number of keys.
//
// Parameters:
//   - relCount (map[string]int64): The amount of keys by relation name.
//   - totalCount (int64): The total amount of keys.
//
// Returns:
//   - string: The relation with the largest number of keys.
//   - float64: The ratio of keys between the biggest relation and the total amount.
func (*ClusteredCoordinator) getBiggestRelation(relCount map[string]int64, totalCount int64) (string, float64) {
	maxCount := 0.0
	maxRel := ""
	for rel, count := range relCount {
		if float64(count) > maxCount {
			maxCount = float64(count)
			maxRel = rel
		}
	}
	return maxRel, maxCount / float64(totalCount)
}

// TODO: unit tests

// getMoveTasks forms the move task group. It gets bounds for intermediate data moves from the database, and returns the resulting MoveTaskGroup.
// Bounds are determined by querying the relation with the biggest amount of keys in the key range.
//
// Parameters:
//   - ctx (context.Context): The context for requests to the database.
//   - conn (*pgx.Conn): The connection to the database.
//   - req (*kr.BatchMoveKeyRange): The request, containing transfer information.
//   - rel (*distributions.DistributedRelation): The relation with the most keys.
//   - condition (sting): The condition for the key range to move data from.
//   - coeff (float64): The ratio between the amount of keys in the biggest relation, and the total amount of keys in the key range.
//   - ds (*distributions.Distribution): The distribution the key range belongs to.
//
// Returns:
//   - *tasks.MoveTaskGroup: The group of data move tasks.
//   - error: An error if any occurred.
func (qc *ClusteredCoordinator) getMoveTasks(ctx context.Context, conn *pgx.Conn, req *kr.BatchMoveKeyRange, rel *distributions.DistributedRelation, condition string, coeff float64, ds *distributions.Distribution) (*tasks.MoveTaskGroup, error) {
	taskList := make([]*tasks.MoveTask, 0)
	step := int64(math.Ceil(float64(req.BatchSize)*coeff - 1e-3))

	limit := func() int64 {
		if req.Limit < 0 {
			return math.MaxInt64
		}
		return req.Limit
	}()

	colsArr, err := rel.GetDistributionKeyColumns()
	if err != nil {
		return nil, err
	}
	columns := strings.Join(colsArr, ", ")
	selectAsColumnsElems := make([]string, len(colsArr))
	subColumnsElems := make([]string, len(colsArr))
	for i := range selectAsColumnsElems {
		selectAsColumnsElems[i] = fmt.Sprintf("%s as col%d", colsArr[i], i)
		subColumnsElems[i] = fmt.Sprintf("sub.col%d", i)
	}
	selectAsColumns := strings.Join(selectAsColumnsElems, ", ")
	subColumns := strings.Join(subColumnsElems, ", ")
	sort := func() string {
		switch req.Type {
		case tasks.SplitLeft:
			return "ASC"
		case tasks.SplitRight:
			fallthrough
		default:
			return "DESC"
		}
	}()
	orderByClause := columns + " " + sort
	query := fmt.Sprintf(`
WITH 
sub as (
    SELECT %s, row_number() OVER(ORDER BY %s) as row_n
    FROM (
        SELECT * FROM %s
        WHERE %s
		ORDER BY %s
        LIMIT %d
        OFFSET %d
    ) AS t
),
constants AS (
    SELECT %d as row_count, %d as batch_size
),
max_row AS (
    SELECT count(1) as row_n
    FROM sub
),
total_rows AS (
	SELECT count(1)
	FROM %s
	WHERE %s
)
SELECT DISTINCT ON (%s) sub.*, total_rows.count <= constants.row_count
FROM sub JOIN max_row ON true JOIN constants ON true JOIN total_rows ON true
WHERE (sub.row_n %% constants.batch_size = 0 AND sub.row_n < constants.row_count)
   OR (sub.row_n = constants.row_count)
   OR (max_row.row_n < constants.row_count AND sub.row_n = max_row.row_n)
ORDER BY (%s) %s;
`,
		selectAsColumns,
		orderByClause,
		rel.GetFullName(),
		condition,
		orderByClause,
		limit,
		func() int {
			switch req.Type {
			case tasks.SplitLeft:
				return 1
			case tasks.SplitRight:
				fallthrough
			default:
				return 0
			}
		}(),
		limit,
		step,
		rel.GetFullName(),
		condition,
		subColumns,
		subColumns,
		sort,
	)
	spqrlog.Zero.Debug().Str("query", query).Msg("get split bound")
	t := time.Now()
	rows, err := conn.Query(ctx, query)
	if err != nil {
		return nil, err
	}
	statistics.RecordShardOperation(time.Since(t))
	var moveWhole bool
	for rows.Next() {
		values := make([]string, len(rel.DistributionKey)+1)
		links := make([]interface{}, len(values)+1)

		for i := range values {
			links[i] = &values[i]
		}
		links[len(links)-1] = &moveWhole
		if err = rows.Scan(links...); err != nil {
			spqrlog.Zero.Error().Err(err).Str("rel", rel.Name).Msg("error getting move tasks")
			return nil, err
		}
		for i, value := range values[:len(values)-1] {
			spqrlog.Zero.Debug().Str("value", value).Int("index", i).Msg("got split bound")
		}
		bound := make([][]byte, len(colsArr))
		for i, t := range ds.ColTypes {
			switch t {
			case qdb.ColumnTypeVarcharDeprecated:
				fallthrough
			case qdb.ColumnTypeVarchar:
				bound[i] = []byte(values[i])
			case qdb.ColumnTypeVarcharHashed:
				fallthrough
			case qdb.ColumnTypeUinteger:
				number, err := strconv.ParseUint(values[i], 10, 64)
				if err != nil {
					return nil, err
				}
				bound[i] = make([]byte, binary.MaxVarintLen64)
				binary.PutUvarint(bound[i], number)
			case qdb.ColumnTypeInteger:
				number, err := strconv.ParseInt(values[i], 10, 64)
				if err != nil {
					return nil, err
				}
				bound[i] = make([]byte, binary.MaxVarintLen64)
				binary.PutVarint(bound[i], number)
			}
		}
		taskList = append(taskList, &tasks.MoveTask{ID: uuid.NewString(), KrIdTemp: uuid.NewString(), State: tasks.TaskPlanned, Bound: bound})
	}
	taskList[0].KrIdTemp = req.DestKrId

	moveWhole = moveWhole || req.Limit < 0

	if len(taskList) <= 1 && moveWhole {
		taskList = []*tasks.MoveTask{
			{
				ID:       uuid.NewString(),
				KrIdTemp: req.DestKrId,
				State:    tasks.TaskPlanned,
				Bound:    nil,
			},
		}
	} else if moveWhole {
		// Avoid splitting key range by its own bound when moving the whole range
		taskList[len(taskList)-1] = &tasks.MoveTask{ID: uuid.NewString(), KrIdTemp: req.KrId, Bound: nil, State: tasks.TaskSplit}
	}

	return &tasks.MoveTaskGroup{
		Tasks:     taskList,
		KrIdFrom:  req.KrId,
		KrIdTo:    req.DestKrId,
		ShardToId: req.ShardId,
		Type:      req.Type,
	}, nil
}

// TODO : unit tests

// getNextKeyRange lists key ranges in the distribution and finds the key range next to the given key range.
// If there's no next key range, nil is returned.
//
// Parameters:
//   - ctx (context.Context): The context for QDB operations.
//   - keyRange (*kr.KeyRange): The key range to find next one to.
//
// Returns:
//   - *kr.KeyRange: The key range next to the given.
//   - error: An error if any occurred.
func (qc *ClusteredCoordinator) getNextKeyRange(ctx context.Context, keyRange *kr.KeyRange) (*kr.KeyRange, error) {
	krs, err := qc.ListKeyRanges(ctx, keyRange.Distribution)
	if err != nil {
		return nil, err
	}
	sort.Slice(krs, func(i, j int) bool {
		return kr.CmpRangesLessEqual(krs[i].LowerBound, krs[j].LowerBound, keyRange.ColumnTypes)
	})

	ind := slices.IndexFunc(krs, func(other *kr.KeyRange) bool {
		return other.ID == keyRange.ID
	})
	if ind < len(krs)-1 {
		return krs[ind+1], nil
	}
	return nil, nil
}

// TODO : unit tests

// executeMoveTasks executes the given MoveTaskGroup.
// All intermediary states of the task group are synced with the QDB for reliability.
//
// Parameters:
//   - ctx (context.Context): The context for QDB operations.
//   - taskGroup (*tasks.MoveTaskGroup): Move tasks to execute.
//
// Returns:
//   - error: An error if any occurred.
func (qc *ClusteredCoordinator) executeMoveTasks(ctx context.Context, taskGroup *tasks.MoveTaskGroup) error {
	for taskGroup.CurrentTaskIndex < len(taskGroup.Tasks) {
		task := taskGroup.Tasks[taskGroup.CurrentTaskIndex]
		switch task.State {
		case tasks.TaskPlanned:
			if task.Bound == nil && taskGroup.KrIdTo == task.KrIdTemp {
				if err := qc.RenameKeyRange(ctx, taskGroup.KrIdFrom, task.KrIdTemp); err != nil {
					return err
				}
				task.State = tasks.TaskSplit
				if err := qc.UpdateMoveTask(ctx, task); err != nil {
					return err
				}
				break
			}
			if err := qc.Split(ctx, &kr.SplitKeyRange{
				Bound:    task.Bound,
				SourceID: taskGroup.KrIdFrom,
				Krid:     task.KrIdTemp,
				SplitLeft: func() bool {
					switch taskGroup.Type {
					case tasks.SplitLeft:
						return true
					case tasks.SplitRight:
						fallthrough
					default:
						return false
					}
				}()}); err != nil {
				return err
			}
			task.State = tasks.TaskSplit
			if err := qc.UpdateMoveTask(ctx, task); err != nil {
				return err
			}
		case tasks.TaskSplit:
			if err := qc.Move(ctx, &kr.MoveKeyRange{Krid: task.KrIdTemp, ShardId: taskGroup.ShardToId}); err != nil {
				return err
			}
			task.State = tasks.TaskMoved
			if err := qc.UpdateMoveTask(ctx, task); err != nil {
				return err
			}
		case tasks.TaskMoved:
			if task.KrIdTemp != taskGroup.KrIdTo {
				if err := qc.Unite(ctx, &kr.UniteKeyRange{BaseKeyRangeId: taskGroup.KrIdTo, AppendageKeyRangeId: task.KrIdTemp}); err != nil {
					return err
				}
			}
			taskGroup.CurrentTaskIndex++
			if err := qc.qdb.UpdateMoveTaskGroupSetCurrentTask(ctx, taskGroup.CurrentTaskIndex); err != nil {
				return err
			}
			if err := qc.qdb.RemoveMoveTask(ctx, task.ID); err != nil {
				return err
			}
		}
	}
	return qc.RemoveMoveTaskGroup(ctx)
}

func (qc *ClusteredCoordinator) RetryMoveTaskGroup(ctx context.Context) error {
	taskGroup, err := qc.GetMoveTaskGroup(ctx)
	if err != nil {
		return spqrerror.Newf(spqrerror.SPQR_TRANSFER_ERROR, "failed to get move task group: %s", err)
	}
	return qc.executeMoveTasks(ctx, taskGroup)
}

// TODO : unit tests

// RedistributeKeyRange moves the whole key range to another shard in batches
//
// Parameters:
//   - ctx (context.Context): The context of the operation.
//   - req (*kr.RedistributeKeyRange): The request with args for the operation.
//
// Returns:
//   - error: An error if any occurred during transfer.
func (qc *ClusteredCoordinator) RedistributeKeyRange(ctx context.Context, req *kr.RedistributeKeyRange) error {
	keyRange, err := qc.GetKeyRange(ctx, req.KrId)
	if err != nil {
		return spqrerror.Newf(spqrerror.SPQR_INVALID_REQUEST, "key range \"%s\" not found", req.KrId)
	}
	if _, err = qc.GetShard(ctx, req.ShardId); err != nil {
		return spqrerror.Newf(spqrerror.SPQR_TRANSFER_ERROR, "error getting destination shard: %s", err.Error())
	}
	if keyRange.ShardID == req.ShardId {
		return nil
	}
	if req.BatchSize <= 0 {
		return spqrerror.Newf(spqrerror.SPQR_INVALID_REQUEST, "incorrect batch size %d", req.BatchSize)
	}
	if req.Check {
		if err := qc.checkKeyRangeMove(ctx, &kr.BatchMoveKeyRange{
			KrId:      req.KrId,
			ShardId:   req.ShardId,
			BatchSize: req.BatchSize,
			Limit:     -1,
			DestKrId:  uuid.NewString(),
			Type:      tasks.SplitRight,
		}); err != nil {
			return err
		}
	}
	if !req.Apply {
		return nil
	}
	ch := make(chan error)
	execCtx := context.TODO()
	go func() {
		ch <- qc.executeRedistributeTask(execCtx, &tasks.RedistributeTask{
			KrId:      req.KrId,
			ShardId:   req.ShardId,
			BatchSize: req.BatchSize,
			TempKrId:  uuid.NewString(),
			State:     tasks.RedistributeTaskPlanned,
		})
	}()

	for {
		select {
		case err := <-ch:
			return err
		case <-ctx.Done():
			return spqrerror.NewByCode(spqrerror.SPQR_TRANSFER_ERROR)
		}
	}
}

// TODO : unit tests

// executeRedistributeTask executes the given RedistributeTask.
// All intermediary states of the task are synced with the QDB for reliability.
//
// Parameters:
//   - ctx (context.Context): The context for QDB operations.
//   - task (*tasks.RedistributeTask): The redistribute task to execute.
//
// Returns:
//   - error: An error if any occurred.
func (qc *ClusteredCoordinator) executeRedistributeTask(ctx context.Context, task *tasks.RedistributeTask) error {
	for {
		switch task.State {
		case tasks.RedistributeTaskPlanned:
			if err := qc.BatchMoveKeyRange(ctx, &kr.BatchMoveKeyRange{
				KrId:      task.KrId,
				ShardId:   task.ShardId,
				BatchSize: task.BatchSize,
				Limit:     -1,
				DestKrId:  task.TempKrId,
				Type:      tasks.SplitRight,
			}); err != nil {
				return err
			}
			task.State = tasks.RedistributeTaskMoved
			if err := qc.db.WriteRedistributeTask(ctx, tasks.RedistributeTaskToDB(task)); err != nil {
				return err
			}
		case tasks.RedistributeTaskMoved:
			if err := qc.RenameKeyRange(ctx, task.TempKrId, task.KrId); err != nil {
				return err
			}
			if err := qc.db.RemoveRedistributeTask(ctx); err != nil {
				return err
			}
			return nil
		default:
			return spqrerror.New(spqrerror.SPQR_METADATA_CORRUPTION, "invalid redistribute task state")
		}
	}
}

// TODO : unit tests

// RenameKeyRange renames a key range.
//
// Parameters:
//   - ctx (context.Context): The context for the request.
//   - krId (string): The ID of the key range to be renamed.
//   - krIdNew (string): The new ID for the specified key range.
//
// Returns:
// - error: An error if renaming key range was unsuccessful.
func (qc *ClusteredCoordinator) RenameKeyRange(ctx context.Context, krId, krIdNew string) error {
	if err := qc.Coordinator.RenameKeyRange(ctx, krId, krIdNew); err != nil {
		return err
	}
	return qc.traverseRouters(ctx, func(cc *grpc.ClientConn) error {
		cl := routerproto.NewKeyRangeServiceClient(cc)
		_, err := cl.RenameKeyRange(ctx, &routerproto.RenameKeyRangeRequest{KeyRangeId: krId, NewKeyRangeId: krIdNew})

		return err
	})
}

// TODO : unit tests
func (qc *ClusteredCoordinator) SyncRouterMetadata(ctx context.Context, qRouter *topology.Router) error {
	spqrlog.Zero.Debug().
		Str("address", qRouter.Address).
		Msg("qdb coordinator: sync router metadata")

	cc, err := DialRouter(qRouter)
	if err != nil {
		return err
	}
	defer func() {
		if err := cc.Close(); err != nil {
			spqrlog.Zero.Debug().Err(err).Msg("failed to close connection")
		}
	}()

	// Configure distributions
	dsCl := routerproto.NewDistributionServiceClient(cc)
	spqrlog.Zero.Debug().Msg("qdb coordinator: configure distributions")
	dss, err := qc.ListDistributions(ctx)
	if err != nil {
		return err
	}
	resp, err := dsCl.ListDistributions(ctx, nil)
	if err != nil {
		return err
	}
	if _, err = dsCl.DropDistribution(ctx, &routerproto.DropDistributionRequest{
		Ids: func() []string {
			res := make([]string, len(resp.Distributions))
			for i, ds := range resp.Distributions {
				res[i] = ds.Id
			}
			return res
		}(),
	}); err != nil {
		return err
	}
	if _, err = dsCl.CreateDistribution(ctx, &routerproto.CreateDistributionRequest{
		Distributions: func() []*routerproto.Distribution {
			res := make([]*routerproto.Distribution, len(dss))
			for i, ds := range dss {
				res[i] = distributions.DistributionToProto(ds)
			}
			return res
		}(),
	}); err != nil {
		return err
	}

	// Configure key ranges.
	krClient := routerproto.NewKeyRangeServiceClient(cc)
	spqrlog.Zero.Debug().Msg("qdb coordinator: configure key ranges")
	if _, err = krClient.DropAllKeyRanges(ctx, nil); err != nil {
		return err
	}

	for _, ds := range dss {
		krs, err := qc.db.ListKeyRanges(ctx, ds.Id)
		if err != nil {
			return err
		}

		sort.Slice(krs, func(i, j int) bool {
			l := kr.KeyRangeFromDB(krs[i], ds.ColTypes)
			r := kr.KeyRangeFromDB(krs[j], ds.ColTypes)
			return !kr.CmpRangesLess(l.LowerBound, r.LowerBound, ds.ColTypes)
		})

		for _, keyrange := range krs {
			resp, err := krClient.CreateKeyRange(ctx, &routerproto.CreateKeyRangeRequest{
				KeyRangeInfo: kr.KeyRangeFromDB(keyrange, ds.ColTypes).ToProto(),
			})

			if err != nil {
				return err
			}

			spqrlog.Zero.Debug().
				Interface("response", resp).
				Msg("got response while adding key range")
		}
	}

	spqrlog.Zero.Debug().Msg("successfully add all key ranges")

	host, err := config.GetHostOrHostname(config.CoordinatorConfig().Host)
	if err != nil {
		return err
	}
	rCl := routerproto.NewTopologyServiceClient(cc)
	if _, err := rCl.UpdateCoordinator(ctx, &routerproto.UpdateCoordinatorRequest{
		Address: net.JoinHostPort(host, config.CoordinatorConfig().GrpcApiPort),
	}); err != nil {
		return err
	}

	if resp, err := rCl.OpenRouter(ctx, nil); err != nil {
		return err
	} else {
		spqrlog.Zero.Debug().
			Interface("response", resp).
			Msg("open router response")
	}

	return nil
}

// TODO : unit tests
func (qc *ClusteredCoordinator) SyncRouterCoordinatorAddress(ctx context.Context, qRouter *topology.Router) error {
	spqrlog.Zero.Debug().
		Str("address", qRouter.Address).
		Msg("qdb coordinator: sync coordinator address")

	cc, err := DialRouter(qRouter)
	if err != nil {
		return err
	}
	defer func() {
		if err := cc.Close(); err != nil {
			spqrlog.Zero.Debug().Err(err).Msg("failed to close connection")
		}
	}()

	/* Update current coordinator address. */
	/* Todo: check that router metadata is in sync. */

	host, err := config.GetHostOrHostname(config.CoordinatorConfig().Host)
	if err != nil {
		return err
	}
	rCl := routerproto.NewTopologyServiceClient(cc)
	if _, err := rCl.UpdateCoordinator(ctx, &routerproto.UpdateCoordinatorRequest{
		Address: net.JoinHostPort(host, config.CoordinatorConfig().GrpcApiPort),
	}); err != nil {
		return err
	}

	if resp, err := rCl.OpenRouter(ctx, nil); err != nil {
		return err
	} else {
		spqrlog.Zero.Debug().
			Interface("response", resp).
			Msg("open router response")
	}

	return nil
}

// TODO : unit tests
func (qc *ClusteredCoordinator) RegisterRouter(ctx context.Context, r *topology.Router) error {
	// TODO: list routers and deduplicate
	spqrlog.Zero.Debug().
		Str("address", r.Address).
		Str("router", r.ID).
		Msg("register router")

	// ping router
	conn, err := DialRouter(r)
	if err != nil {
		return spqrerror.Newf(spqrerror.SPQR_CONNECTION_ERROR, "failed to ping router: %s", err)
	}
	defer func() {
		if err := conn.Close(); err != nil {
			spqrlog.Zero.Debug().Err(err).Msg("failed to close connection")
		}
	}()
	cl := routerproto.NewTopologyServiceClient(conn)
	_, err = cl.GetRouterStatus(ctx, nil)
	if err != nil {
		return spqrerror.Newf(spqrerror.SPQR_CONNECTION_ERROR, "failed to ping router: %s", err)
	}

	return qc.Coordinator.RegisterRouter(ctx, r)
}

// TODO : unit tests
func (qc *ClusteredCoordinator) PrepareClient(nconn net.Conn, pt port.RouterPortType) (rclient.RouterClient, error) {
	cl := rclient.NewPsqlClient(nconn, pt, "", false, "")

	tlsconfig := qc.tlsconfig
	if pt == port.UnixSocketPortType {
		tlsconfig = nil
	}

	spqrlog.Zero.Info().
		Str("user", cl.Usr()).
		Str("db", cl.DB()).
		Bool("ssl", tlsconfig != nil).
		Msg("init client connection...")

	if err := cl.Init(tlsconfig); err != nil {
		return nil, err
	}

	if cl.CancelMsg() != nil {
		return cl, nil
	}

	spqrlog.Zero.Info().
		Str("user", cl.Usr()).
		Str("db", cl.DB()).
		Bool("ssl", tlsconfig != nil).
		Msg("init client connection OK")

	// match client to frontend rule
	key := *route.NewRouteKey(cl.Usr(), cl.DB())
	frRule, err := qc.rmgr.MatchKeyFrontend(key)
	if err != nil {
		for _, msg := range []pgproto3.BackendMessage{
			&pgproto3.ErrorResponse{
				Severity: "ERROR",
				Message:  err.Error(),
			},
		} {
			if err := cl.Send(msg); err != nil {
				return nil, fmt.Errorf("failed to make route failure response: %w", err)
			}
		}
		return nil, err
	}

	if err := cl.AssignRule(frRule); err != nil {
		return nil, err
	}

	r := route.NewRoute(nil, nil, nil)

	params := map[string]string{
		"client_encoding": "UTF8",
		"DateStyle":       "ISO",
	}
	for k, v := range params {
		cl.SetParam(k, v)
	}

	r.SetParams(cl.Params())
	if err := cl.Auth(r); err != nil {
		return nil, err
	}
	spqrlog.Zero.Info().Msg("client auth OK")

	return cl, nil
}

// TODO : unit tests
func (qc *ClusteredCoordinator) ProcClient(ctx context.Context, nconn net.Conn, pt port.RouterPortType) error {
	cl, err := qc.PrepareClient(nconn, pt)
	if err != nil {
		spqrlog.Zero.Error().Err(err).Msg("")
		return err
	}

	if cl.CancelMsg() != nil {
		// TODO: cancel client here
		return nil
	}

	ci := grpcConnMgr{ClusteredCoordinator: qc}
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
		case *pgproto3.Terminate:
			return nil
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

			if err := meta.ProcMetadataCommand(ctx, tstmt, qc, ci, cl, nil, qc.IsReadOnly()); err != nil {
				spqrlog.Zero.Error().Err(err).Msg("")
				_ = cli.ReportError(err)
			} else {
				spqrlog.Zero.Debug().Msg("processed OK")
			}
		default:
			return spqrerror.Newf(spqrerror.SPQR_COMPLEX_QUERY, "unsupported msg type %T", msg)
		}
	}
}

// TODO : unit tests
func (qc *ClusteredCoordinator) AddDataShard(ctx context.Context, shard *topology.DataShard) error {
	return qc.db.AddShard(ctx, qdb.NewShard(shard.ID, shard.Cfg.RawHosts))
}

func (qc *ClusteredCoordinator) AddWorldShard(_ context.Context, _ *topology.DataShard) error {
	panic("ClusteredCoordinator.AddWorldShard not implemented")
}

// TODO : unit tests
func (qc *ClusteredCoordinator) UpdateCoordinator(ctx context.Context, address string) error {
	return qc.traverseRouters(ctx, func(cc *grpc.ClientConn) error {
		c := routerproto.NewTopologyServiceClient(cc)
		spqrlog.Zero.Debug().Str("address", address).Msg("updating coordinator address")
		_, err := c.UpdateCoordinator(ctx, &routerproto.UpdateCoordinatorRequest{
			Address: address,
		})
		return err
	})
}

// CreateDistribution creates distribution in QDB
// TODO: unit tests
func (qc *ClusteredCoordinator) CreateReferenceRelation(ctx context.Context,
	r *rrelation.ReferenceRelation, entry []*rrelation.AutoIncrementEntry) error {
	if err := qc.Coordinator.CreateReferenceRelation(ctx, r, entry); err != nil {
		return err
	}

	return qc.traverseRouters(ctx, func(cc *grpc.ClientConn) error {
		cl := routerproto.NewReferenceRelationsServiceClient(cc)
		resp, err := cl.CreateReferenceRelations(context.TODO(),
			&routerproto.CreateReferenceRelationsRequest{
				Relation: rrelation.RefRelationToProto(r),
				Entries:  rrelation.AutoIncrementEntriesToProto(entry),
			})
		if err != nil {
			return err
		}

		spqrlog.Zero.Debug().
			Interface("response", resp).
			Msg("create reference relation response")
		return nil
	})
}

// TODO: unit tests
func (qc *ClusteredCoordinator) DropReferenceRelation(ctx context.Context,
	id string) error {
	if err := qc.Coordinator.DropReferenceRelation(ctx, id); err != nil {
		return err
	}

	return qc.traverseRouters(ctx, func(cc *grpc.ClientConn) error {
		cl := routerproto.NewReferenceRelationsServiceClient(cc)
		resp, err := cl.DropReferenceRelations(context.TODO(),
			&routerproto.DropReferenceRelationsRequest{
				Ids: []string{id},
			})
		if err != nil {
			return err
		}

		spqrlog.Zero.Debug().
			Interface("response", resp).
			Msg("create reference relation response")
		return nil
	})
}

// CreateDistribution creates distribution in QDB
// TODO: unit tests
func (qc *ClusteredCoordinator) CreateDistribution(ctx context.Context, ds *distributions.Distribution) error {
	if err := qc.Coordinator.CreateDistribution(ctx, ds); err != nil {
		return err
	}

	return qc.traverseRouters(ctx, func(cc *grpc.ClientConn) error {
		cl := routerproto.NewDistributionServiceClient(cc)
		resp, err := cl.CreateDistribution(context.TODO(), &routerproto.CreateDistributionRequest{
			Distributions: []*routerproto.Distribution{distributions.DistributionToProto(ds)},
		})
		if err != nil {
			return err
		}

		spqrlog.Zero.Debug().
			Interface("response", resp).
			Msg("create distribution response")
		return nil
	})
}

// DropDistribution deletes distribution from QDB
// TODO: unit tests
func (qc *ClusteredCoordinator) DropDistribution(ctx context.Context, id string) error {
	if err := qc.Coordinator.DropDistribution(ctx, id); err != nil {
		return err
	}

	return qc.traverseRouters(ctx, func(cc *grpc.ClientConn) error {
		cl := routerproto.NewDistributionServiceClient(cc)
		resp, err := cl.DropDistribution(context.TODO(), &routerproto.DropDistributionRequest{
			Ids: []string{id},
		})
		if err != nil {
			return err
		}

		spqrlog.Zero.Debug().
			Interface("response", resp).
			Msg("drop distribution response")
		return nil
	})
}

// AlterDistributionAttach attaches relation to distribution
// TODO: unit tests
func (qc *ClusteredCoordinator) AlterDistributionAttach(ctx context.Context, id string, rels []*distributions.DistributedRelation) error {
	if err := qc.Coordinator.AlterDistributionAttach(ctx, id, rels); err != nil {
		return err
	}

	return qc.traverseRouters(ctx, func(cc *grpc.ClientConn) error {
		cl := routerproto.NewDistributionServiceClient(cc)
		resp, err := cl.AlterDistributionAttach(context.TODO(), &routerproto.AlterDistributionAttachRequest{
			Id: id,
			Relations: func() []*routerproto.DistributedRelation {
				res := make([]*routerproto.DistributedRelation, len(rels))
				for i, rel := range rels {
					res[i] = distributions.DistributedRelationToProto(rel)
				}
				return res
			}(),
		})
		if err != nil {
			return err
		}

		spqrlog.Zero.Debug().
			Interface("response", resp).
			Msg("attach relation response")
		return nil
	})
}

// AlterDistributedRelation changes relation attached to a distribution
// TODO: unit tests
func (qc *ClusteredCoordinator) AlterDistributedRelation(ctx context.Context, id string, rel *distributions.DistributedRelation) error {
	if err := qc.Coordinator.AlterDistributedRelation(ctx, id, rel); err != nil {
		return err
	}

	return qc.traverseRouters(ctx, func(cc *grpc.ClientConn) error {
		cl := routerproto.NewDistributionServiceClient(cc)
		resp, err := cl.AlterDistributedRelation(context.TODO(), &routerproto.AlterDistributedRelationRequest{
			Id:       id,
			Relation: distributions.DistributedRelationToProto(rel),
		})
		if err != nil {
			return err
		}

		spqrlog.Zero.Debug().
			Interface("response", resp).
			Msg("alter relation response")
		return nil
	})
}

func (qc *ClusteredCoordinator) DropSequence(ctx context.Context, seqName string) error {
	if err := qc.Coordinator.DropSequence(ctx, seqName); err != nil {
		return err
	}
	return qc.traverseRouters(ctx, func(cc *grpc.ClientConn) error {
		cl := routerproto.NewDistributionServiceClient(cc)
		resp, err := cl.DropSequence(context.TODO(), &routerproto.DropSequenceRequest{
			Name: seqName,
		})
		if err != nil {
			return err
		}

		spqrlog.Zero.Debug().
			Interface("response", resp).
			Msg("drop sequence response")
		return nil
	})
}

// AlterDistributionDetach detaches relation from distribution
// TODO: unit tests
func (qc *ClusteredCoordinator) AlterDistributionDetach(ctx context.Context, id string, relName *spqrparser.QualifiedName) error {
	/* Do what needs to be done in metadata */
	if err := qc.Coordinator.AlterDistributionDetach(ctx, id, relName); err != nil {
		return err
	}

	return qc.traverseRouters(ctx, func(cc *grpc.ClientConn) error {
		cl := routerproto.NewDistributionServiceClient(cc)
		resp, err := cl.AlterDistributionDetach(context.TODO(), &routerproto.AlterDistributionDetachRequest{
			Id:       id,
			RelNames: []string{relName.Name},
		})
		if err != nil {
			return err
		}

		spqrlog.Zero.Debug().
			Interface("response", resp).
			Msg("detach relation response")
		return nil
	})
}

func (qc *ClusteredCoordinator) finishRedistributeTasksInProgress(ctx context.Context) error {
	task, err := qc.db.GetRedistributeTask(ctx)
	if err != nil {
		return err
	}
	if task == nil {
		return nil
	}
	return qc.executeRedistributeTask(ctx, tasks.RedistributeTaskFromDB(task))
}

func (qc *ClusteredCoordinator) finishMoveTasksInProgress(ctx context.Context) error {
	taskGroup, err := qc.GetMoveTaskGroup(ctx)
	if err != nil {
		return err
	}
	if taskGroup == nil {
		return nil
	}
	balancerTask, err := qc.GetBalancerTask(ctx)
	if err != nil {
		return err
	}
	if balancerTask != nil {
		// If there is currently a balancer task running, we need to advance its state after moving the data
		if err = qc.executeMoveTasks(ctx, taskGroup); err != nil {
			return err
		}
		balancerTask.State = tasks.BalancerTaskMoved
		return qc.WriteBalancerTask(ctx, balancerTask)
	}
	return qc.executeMoveTasks(ctx, taskGroup)
}

func (qc *ClusteredCoordinator) IsReadOnly() bool {
	return !qc.acquiredLock
}
