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
	"sync"
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
	"github.com/pg-sharding/spqr/pkg/coord"
	"github.com/pg-sharding/spqr/pkg/datatransfers"
	"github.com/pg-sharding/spqr/pkg/meta"
	"github.com/pg-sharding/spqr/pkg/models/distributions"
	"github.com/pg-sharding/spqr/pkg/models/hashfunction"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/pkg/models/rrelation"
	"github.com/pg-sharding/spqr/pkg/models/spqrerror"
	"github.com/pg-sharding/spqr/pkg/models/tasks"
	"github.com/pg-sharding/spqr/pkg/models/topology"
	mtran "github.com/pg-sharding/spqr/pkg/models/transaction"
	"github.com/pg-sharding/spqr/pkg/pool"
	proto "github.com/pg-sharding/spqr/pkg/protos"
	"github.com/pg-sharding/spqr/pkg/rulemgr"
	"github.com/pg-sharding/spqr/pkg/shard"
	"github.com/pg-sharding/spqr/pkg/spqrlog"
	"github.com/pg-sharding/spqr/pkg/tsa"
	"github.com/pg-sharding/spqr/qdb"
	"github.com/pg-sharding/spqr/qdb/ops"
	"github.com/pg-sharding/spqr/router/cache"
	rclient "github.com/pg-sharding/spqr/router/client"
	"github.com/pg-sharding/spqr/router/port"
	"github.com/pg-sharding/spqr/router/rfqn"
	"github.com/pg-sharding/spqr/router/route"
	spqrparser "github.com/pg-sharding/spqr/yacc/console"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
)

type grpcConnMgr struct {
	*ClusteredCoordinator
}

// InstanceHealthChecks implements connmgr.ConnectionStatsMgr.
func (ci grpcConnMgr) InstanceHealthChecks() map[string]tsa.CachedCheckResult {
	return map[string]tsa.CachedCheckResult{}
}

// TsaCacheEntries implements connmgr.ConnectionStatsMgr.
func (ci grpcConnMgr) TsaCacheEntries() map[pool.TsaKey]pool.CachedEntry {
	return map[pool.TsaKey]pool.CachedEntry{}
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
	routers, err := ci.ClusteredCoordinator.QDB().ListRouters(ctx)

	spqrlog.Zero.Log().Int("router counts", len(routers))

	if err != nil {
		return err
	}

	for _, r := range routers {
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
		rrClient := proto.NewClientInfoServiceClient(cc)

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
func (ci grpcConnMgr) ForEach(cb func(sh shard.ShardHostCtl) error) error {
	return ci.IterRouter(func(cc *grpc.ClientConn, addr string) error {
		ctx := context.TODO()
		rrBackConn := proto.NewBackendConnectionsServiceClient(cc)

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
		rrBackConn := proto.NewPoolServiceClient(cc)

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

var _ connmgr.ConnectionMgr = &grpcConnMgr{}

func DialRouter(r *topology.Router) (*grpc.ClientConn, error) {
	spqrlog.Zero.Debug().
		Str("router-id", r.ID).
		Msg("dialing router")

	// Configure keepalive to prevent connection closure during long idle periods
	// Network intermediaries (load balancers, firewalls) typically close idle connections after 60s-5min
	// Default: 30s keepalive ensures connections survive even aggressive timeouts (e.g., AWS ELB 60s default)
	keepaliveTime := config.ValueOrDefaultDuration(config.CoordinatorConfig().RouterKeepaliveTime, 30*time.Second)
	keepaliveTimeout := config.ValueOrDefaultDuration(config.CoordinatorConfig().RouterKeepaliveTimeout, 20*time.Second)

	keepaliveParams := keepalive.ClientParameters{
		Time:                keepaliveTime,    // Send keepalive ping interval
		Timeout:             keepaliveTimeout, // Wait for ping ack before considering connection dead
		PermitWithoutStream: true,             // Allow pings even when no active RPCs
	}

	return grpc.NewClient(r.Address,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithKeepaliveParams(keepaliveParams))
}

const defaultWatchRouterTimeout = time.Second
const defaultLockCoordinatorTimeout = time.Second

/*
* This is the global coordinator, which manages the spqr cluster as a whole.
* Its method calls result in cluster-wide changes.
 */
type ClusteredCoordinator struct {
	coord.Coordinator
	rmgr         rulemgr.RulesMgr
	tlsconfig    *tls.Config
	db           qdb.XQDB
	cache        *cache.SchemaCache
	acquiredLock bool

	bounds [][][]byte
	index  int

	routerConnCache map[string]*grpc.ClientConn
	routerConnMutex sync.RWMutex
}

func (qc *ClusteredCoordinator) QDB() qdb.QDB {
	return qc.db
}

func (qc *ClusteredCoordinator) Cache() *cache.SchemaCache {
	return qc.cache
}

var _ coordinator.Coordinator = &ClusteredCoordinator{}

// getOrCreateRouterConn returns a cached connection or creates a new one.
func (qc *ClusteredCoordinator) getOrCreateRouterConn(r *topology.Router) (*grpc.ClientConn, error) {
	qc.routerConnMutex.Lock()
	defer qc.routerConnMutex.Unlock()

	conn, exists := qc.routerConnCache[r.ID]

	if exists {
		// Check if connection is still valid
		state := conn.GetState()
		if state == connectivity.Ready || state == connectivity.Idle {
			return conn, nil
		}
		// Connection is not healthy, close and remove it
		_ = conn.Close()
		delete(qc.routerConnCache, r.ID)
	}

	// Create new connection
	conn, err := DialRouter(r)
	if err != nil {
		return nil, err
	}

	qc.routerConnCache[r.ID] = conn
	return conn, nil
}

// closeRouterConn closes and removes a router connection from cache
func (qc *ClusteredCoordinator) closeRouterConn(routerID string) {
	qc.routerConnMutex.Lock()
	defer qc.routerConnMutex.Unlock()

	if conn, exists := qc.routerConnCache[routerID]; exists {
		_ = conn.Close()
		delete(qc.routerConnCache, routerID)
	}
}

// watchRouters traverse routers one check if they are opened
// for clients. If not, initialize metadata and open router
// TODO : unit tests
func (qc *ClusteredCoordinator) watchRouters(ctx context.Context) {
	spqrlog.Zero.Debug().Msg("start routers watch iteration")
	for {
		// TODO check we are still coordinator
		if !qc.acquiredLock {
			time.Sleep(500 * time.Millisecond)
			continue
		}

		// TODO: lock router
		routers, err := qc.db.ListRouters(ctx)
		if err != nil {
			spqrlog.Zero.Error().Err(err).Msg("")
			time.Sleep(time.Second)
			continue
		}

		// Build set of current router IDs for cleanup
		currentRouterIDs := make(map[string]bool, len(routers))
		for _, r := range routers {
			currentRouterIDs[r.ID] = true
		}

		for _, r := range routers {
			if err := func() error {
				// Create bounded context for this router's operations
				routerCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
				defer cancel()

				internalR := &topology.Router{
					ID:      r.ID,
					Address: r.Address,
				}

				cc, err := qc.getOrCreateRouterConn(internalR)
				if err != nil {
					return err
				}

				rrClient := proto.NewTopologyServiceClient(cc)

				resp, err := rrClient.GetRouterStatus(routerCtx, nil)
				if err != nil {
					// Connection error - close and remove from cache so we reconnect next time
					qc.closeRouterConn(internalR.ID)
					return err
				}

				switch resp.Status {
				case proto.RouterStatus_CLOSED:
					spqrlog.Zero.Debug().Msg("router is closed")
					if err := qc.SyncRouterCoordinatorAddress(routerCtx, internalR); err != nil {
						return err
					}

					/* Mark router as opened in qdb */
					err := qc.db.CloseRouter(routerCtx, internalR.ID)
					if err != nil {
						return err
					}

				case proto.RouterStatus_OPENED:
					spqrlog.Zero.Debug().Msg("router is opened")

					/* TODO: check router metadata consistency */
					if err := qc.SyncRouterCoordinatorAddress(routerCtx, internalR); err != nil {
						return err
					}

					/* Mark router as opened in qdb */
					err := qc.db.OpenRouter(routerCtx, internalR.ID)
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

		// Clean up connections for routers that no longer exist
		qc.routerConnMutex.RLock()
		var staleConnIDs []string
		for routerID := range qc.routerConnCache {
			if !currentRouterIDs[routerID] {
				staleConnIDs = append(staleConnIDs, routerID)
			}
		}
		qc.routerConnMutex.RUnlock()

		for _, routerID := range staleConnIDs {
			spqrlog.Zero.Debug().Str("router-id", routerID).Msg("cleaning up connection for removed router")
			qc.closeRouterConn(routerID)
		}

		time.Sleep(config.ValueOrDefaultDuration(config.CoordinatorConfig().IterationTimeout, defaultWatchRouterTimeout))
	}
}

func NewClusteredCoordinator(tlsconfig *tls.Config, db qdb.XQDB) (*ClusteredCoordinator, error) {
	return &ClusteredCoordinator{
		Coordinator:     coord.NewCoordinator(db),
		db:              db,
		tlsconfig:       tlsconfig,
		rmgr:            rulemgr.NewMgr(config.CoordinatorConfig().FrontendRules, []*config.BackendRule{}),
		acquiredLock:    false,
		bounds:          make([][][]byte, 0),
		index:           0,
		routerConnCache: make(map[string]*grpc.ClientConn),
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

	lock := func(ctx context.Context) error {
		currentCoord, _ := qc.GetCoordinator(ctx)
		host, err := config.GetHostOrHostname(config.CoordinatorConfig().Host)
		if err != nil {
			return err
		}
		addr := net.JoinHostPort(host, config.CoordinatorConfig().GrpcApiPort)
		if currentCoord == addr {
			return nil
		}

		if err := qc.db.TryCoordinatorLock(ctx, addr); err != nil {
			return err
		}

		return updateCoordinator()
	}

	err := lock(ctx)
	qc.acquiredLock = err == nil
	if err != nil {
		return err
	}
	go func() {
		tryLockTimeout := config.ValueOrDefaultDuration(config.CoordinatorConfig().LockIterationTimeout, defaultLockCoordinatorTimeout)
		t := time.NewTicker(time.Second)
		for {
			<-t.C

			ctx, cancel := context.WithTimeout(context.Background(), tryLockTimeout)
			err := lock(ctx)
			if err != nil {
				spqrlog.Zero.Debug().Err(err).Msg("failed to retake coordinator lock")
			}
			qc.acquiredLock = err == nil
			cancel()
		}
	}()
	return nil
}

// RunCoordinator side effect: it runs an asynchronous goroutine
// that checks the availability of the SPQR router
//
// TODO: unit tests
func (qc *ClusteredCoordinator) RunCoordinator(ctx context.Context, initialRouter bool) {
	for {
		err := qc.lockCoordinator(ctx, initialRouter)
		if err == nil {
			break
		}
		spqrlog.Zero.Error().Err(err).Msg("error getting qdb lock, retrying")

		time.Sleep(config.ValueOrDefaultDuration(config.CoordinatorConfig().LockIterationTimeout, defaultLockCoordinatorTimeout))
	}

	// sync shards
	if config.CoordinatorConfig().ShardDataCfg != "" {
		shards, err := config.LoadShardDataCfg(config.CoordinatorConfig().ShardDataCfg)
		if err != nil {
			spqrlog.Zero.Error().
				Err(err).
				Msg("failed to load shard data config")
		}

		if shards != nil {
			for id, cfg := range shards.ShardsData {
				if _, err := qc.db.GetShard(context.TODO(), id); err == nil {
					spqrlog.Zero.Debug().
						Str("shard", id).
						Msg("already exists. creating shard skipped")
					continue
				}
				if err := qc.db.AddShard(context.TODO(), qdb.NewShard(id, cfg.Hosts)); err != nil {
					spqrlog.Zero.Error().
						Err(err).
						Msg("failed to add shard")
				}
			}
		}
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

	routers, err := qc.db.ListRouters(ctx)
	if err != nil {
		return err
	}

	for _, rtr := range routers {
		if err := func() error {
			if rtr.State != qdb.OPENED {
				return spqrerror.New(spqrerror.SPQR_ROUTER_ERROR, "router is closed")
			}

			// TODO: run cb`s async
			cc, err := qc.getOrCreateRouterConn(&topology.Router{
				ID:      rtr.ID,
				Address: rtr.Addr(),
			})
			if err != nil {
				return err
			}

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
		cl := proto.NewKeyRangeServiceClient(cc)
		resp, err := cl.CreateKeyRange(ctx, &proto.CreateKeyRangeRequest{
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
		cl := proto.NewKeyRangeServiceClient(cc)
		resp, err := cl.LockKeyRange(ctx, &proto.LockKeyRangeRequest{
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
		cl := proto.NewKeyRangeServiceClient(cc)
		resp, err := cl.UnlockKeyRange(ctx, &proto.UnlockKeyRangeRequest{
			Id: []string{keyRangeID},
		})

		spqrlog.Zero.Debug().Err(err).
			Interface("response", resp).
			Str("key range ID", keyRangeID).
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
		cl := proto.NewKeyRangeServiceClient(cc)
		resp, err := cl.SplitKeyRange(ctx, &proto.SplitKeyRangeRequest{
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
		cl := proto.NewKeyRangeServiceClient(cc)
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
		cl := proto.NewKeyRangeServiceClient(cc)
		resp, err := cl.DropKeyRange(ctx, &proto.DropKeyRangeRequest{
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
		cl := proto.NewKeyRangeServiceClient(cc)
		resp, err := cl.MergeKeyRange(ctx, &proto.MergeKeyRangeRequest{
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

			err = datatransfers.MoveKeys(ctx, keyRange.ShardID, req.ShardId, keyRange, ds, qc.db, qc)
			if err != nil {
				spqrlog.Zero.Error().Err(err).Msg("failed to move rows")
				return err
			}

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
				cl := proto.NewKeyRangeServiceClient(cc)
				moveResp, err := cl.MoveKeyRange(ctx, &proto.MoveKeyRangeRequest{
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
				spqrlog.Zero.Error().Err(err).Msg("failed to unlock key range")
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
	rels := make([]string, 0, len(ds.Relations))
	for _, rel := range ds.Relations {
		schemas[rel.GetSchema()] = struct{}{}
		relName := strings.ToLower(rel.Name)
		rels = append(rels, rel.GetFullName())
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

	exists, err := qc.QDB().CheckDistribution(ctx, distributions.REPLICATED)
	if err != nil {
		return fmt.Errorf("error checking for replicated distribution: %s", err)
	}
	replRels := []string{}
	if exists {
		replDs, err := qc.GetDistribution(ctx, distributions.REPLICATED)
		if err != nil {
			return fmt.Errorf("error getting replicated distribution: %s", err)
		}
		replRels = make([]string, 0, len(replDs.Relations))
		for _, r := range replDs.Relations {
			relExists, err := datatransfers.CheckTableExists(ctx, sourceConn, r.Name, r.GetSchema())
			if err != nil {
				return fmt.Errorf("failed to check for relation \"%s\" existence on source shard: %s", r.GetFullName(), err)
			}
			if relExists {
				destRelExists, err := datatransfers.CheckTableExists(ctx, destConn, r.Name, r.GetSchema())
				if err != nil {
					return fmt.Errorf("failed to check for relation \"%s\" existence on destination shard: %s", r.GetFullName(), err)
				}
				if !destRelExists {
					return fmt.Errorf("replicated relation \"%s\" exists on source shard, but not on destination shard", r.GetFullName())
				}
				replRels = append(replRels, r.GetFullName())
			}
		}
	}

	deferrable, constraintName, err := datatransfers.CheckConstraints(ctx, sourceConn, rels, replRels)
	if err != nil {
		return spqrerror.Newf(spqrerror.SPQR_TRANSFER_ERROR, "error checking table constraints on source shard: %s", err)
	}
	if !deferrable {
		return spqrerror.Newf(spqrerror.SPQR_TRANSFER_ERROR, "found non-deferrable constraint or constraint referencing non-distributed table on source shard: \"%s\"", constraintName)
	}

	deferrable, constraintName, err = datatransfers.CheckConstraints(ctx, destConn, rels, replRels)
	if err != nil {
		return spqrerror.Newf(spqrerror.SPQR_TRANSFER_ERROR, "error checking table constraints on destination shard: %s", err)
	}
	if !deferrable {
		return spqrerror.Newf(spqrerror.SPQR_TRANSFER_ERROR, "found non-deferrable constraint or constraint referencing non-distributed table on destination shard: \"%s\"", constraintName)
	}

	hasSpqrHash, err := datatransfers.CheckHashExtension(ctx, sourceConn)
	if err != nil {
		return spqrerror.Newf(spqrerror.SPQR_TRANSFER_ERROR, "error checking for spqrhash extension on source shard: %s", err)
	}
	if !hasSpqrHash {
		return spqrerror.New(spqrerror.SPQR_TRANSFER_ERROR, "extension \"spqrhash\" not installed on source shard")
	}

	hasSpqrHash, err = datatransfers.CheckHashExtension(ctx, destConn)
	if err != nil {
		return spqrerror.Newf(spqrerror.SPQR_TRANSFER_ERROR, "error checking for spqrhash extension on destination shard: %s", err)
	}
	if !hasSpqrHash {
		return spqrerror.New(spqrerror.SPQR_TRANSFER_ERROR, "extension \"spqrhash\" not installed on destination shard")
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
		taskGroup = &tasks.MoveTaskGroup{
			ID:        uuid.NewString(),
			KrIdFrom:  req.KrId,
			KrIdTo:    req.DestKrId,
			ShardToId: req.ShardId,
			Type:      req.Type,
			BoundRel:  biggestRelName,
			Coeff:     coeff,
			BatchSize: int64(req.BatchSize),
			Limit:     req.Limit,
		}
	} else {
		tgID := uuid.NewString()
		taskGroup = &tasks.MoveTaskGroup{
			ID:        tgID,
			KrIdFrom:  req.KrId,
			KrIdTo:    req.DestKrId,
			ShardToId: req.ShardId,
			Type:      req.Type,
			CurrentTask: &tasks.MoveTask{
				ID:          uuid.NewString(),
				TaskGroupID: tgID,
				KrIdTemp:    req.DestKrId,
				State:       tasks.TaskPlanned,
				Bound:       nil,
			},
		}
	}
	spqrlog.Zero.Debug().Str("taskGroup", fmt.Sprintf("%#v", taskGroup)).Msg("got task group")

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
	statistics.RecordShardOperation("getKeyStats", time.Since(t))
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

func (qc *ClusteredCoordinator) getNextBound(ctx context.Context, conn *pgx.Conn, taskGroup *tasks.MoveTaskGroup, rel *distributions.DistributedRelation, ds *distributions.Distribution) ([][]byte, error) {
	if qc.bounds != nil && qc.index < len(qc.bounds) {
		qc.index++
		return qc.bounds[qc.index-1], nil
	}

	spqrlog.Zero.Debug().Msg("generating new bounds batch")
	keyRange, err := qc.GetKeyRange(ctx, taskGroup.KrIdFrom)
	krFound := true
	if et, ok := err.(*spqrerror.SpqrError); ok && et.ErrorCode == spqrerror.SPQR_KEYRANGE_ERROR {
		krFound = false
		spqrlog.Zero.Debug().Str("key range", taskGroup.KrIdFrom).Msg("key range already moved")
	}
	if krFound && err != nil {
		return nil, err
	}
	if !krFound {
		return nil, nil
	}
	nextRange, err := qc.getNextKeyRange(ctx, keyRange)
	var nextBound kr.KeyRangeBound
	if nextRange != nil {
		nextBound = nextRange.LowerBound
	}
	if err != nil {
		return nil, err
	}
	condition, err := kr.GetKRCondition(rel, keyRange, nextBound, "")
	if err != nil {
		return nil, err
	}

	boundList := make([][][]byte, 0)
	step := int64(math.Ceil(float64(taskGroup.BatchSize)*taskGroup.Coeff - 1e-3))

	cacheSize := config.CoordinatorConfig().DataMoveBoundBatchSize

	limit := func() int64 {
		if taskGroup.Limit < 0 {
			return cacheSize * taskGroup.BatchSize
		}
		return min(taskGroup.Limit-taskGroup.TotalKeys, cacheSize*taskGroup.BatchSize)
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
		switch taskGroup.Type {
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
			switch taskGroup.Type {
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
	statistics.RecordShardOperation("getSplitBounds", time.Since(t))
	var moveWhole bool
	for rows.Next() {
		values := make([]string, len(rel.DistributionKey)+1)
		links := make([]any, len(values)+1)

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
			case qdb.ColumnTypeUUID:
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
				bound[i] = hashfunction.EncodeUInt64(number)
			case qdb.ColumnTypeInteger:
				number, err := strconv.ParseInt(values[i], 10, 64)
				if err != nil {
					return nil, err
				}
				bound[i] = make([]byte, binary.MaxVarintLen64)
				binary.PutVarint(bound[i], number)
			default:
				return nil, fmt.Errorf("unknown column type: %s", t)
			}
		}
		boundList = append(boundList, bound)
	}
	if len(boundList) == 0 {
		return nil, nil
	}
	if len(boundList) <= 1 && moveWhole {
		boundList = [][][]byte{keyRange.Raw()}
	} else if moveWhole {
		// Avoid splitting key range by its own bound when moving the whole range
		boundList[len(boundList)-1] = keyRange.Raw()
	}
	qc.bounds = boundList
	qc.index = 1
	return qc.bounds[0], nil
}

func (qc *ClusteredCoordinator) getNextMoveTask(ctx context.Context, conn *pgx.Conn, taskGroup *tasks.MoveTaskGroup, rel *distributions.DistributedRelation, ds *distributions.Distribution) (*tasks.MoveTask, error) {
	if taskGroup.Limit > 0 && taskGroup.TotalKeys >= taskGroup.Limit {
		return nil, nil
	}
	stop, err := qc.QDB().CheckMoveTaskGroupStopFlag(ctx, taskGroup.ID)
	if err != nil {
		return nil, fmt.Errorf("failed to check for stop flag: %s", err)
	}
	// TODO create special error type here, use it to stop redistribute/balancer tasks
	if stop {
		spqrlog.Zero.Info().Msg("got stop flag, gracefully stopping move task group")
		return nil, spqrerror.Newf(spqrerror.SPQR_STOP_MOVE_TASK_GROUP, "move task stopped by STOP MOVE TASK GROUP command")
	}
	keyRange, err := qc.GetKeyRange(ctx, taskGroup.KrIdFrom)
	krFound := true
	if et, ok := err.(*spqrerror.SpqrError); ok && et.ErrorCode == spqrerror.SPQR_KEYRANGE_ERROR {
		krFound = false
		spqrlog.Zero.Debug().Str("key range", taskGroup.KrIdFrom).Msg("key range already moved")
	}
	if krFound && err != nil {
		return nil, err
	}
	if !krFound {
		return nil, nil
	}
	bound, err := qc.getNextBound(ctx, conn, taskGroup, rel, ds)
	if err != nil {
		return nil, err
	}
	boundKR, err := kr.KeyRangeFromBytes(bound, keyRange.ColumnTypes)
	if err != nil {
		return nil, err
	}
	if kr.CmpRangesEqual(boundKR.LowerBound, keyRange.LowerBound, keyRange.ColumnTypes) {
		// move whole key range
		return &tasks.MoveTask{
			ID: uuid.NewString(),
			KrIdTemp: func() string {
				if taskGroup.TotalKeys > 0 {
					return taskGroup.KrIdFrom
				}
				return taskGroup.KrIdTo
			}(),
			State:       tasks.TaskPlanned,
			Bound:       nil,
			TaskGroupID: taskGroup.ID,
		}, nil
	}
	task := &tasks.MoveTask{ID: uuid.NewString(), KrIdTemp: uuid.NewString(), State: tasks.TaskPlanned, Bound: bound, TaskGroupID: taskGroup.ID}
	if taskGroup.TotalKeys == 0 {
		task.KrIdTemp = taskGroup.KrIdTo
	}
	return task, nil
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
	if taskGroup == nil {
		return nil
	}
	keyRange, err := qc.GetKeyRange(ctx, taskGroup.KrIdFrom)
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

	ds, err := qc.GetDistribution(ctx, keyRange.Distribution)
	if err != nil {
		return err
	}

	var rel *distributions.DistributedRelation
	if taskGroup.BoundRel != "" {
		ok := false
		rel, ok = ds.Relations[taskGroup.BoundRel]
		if !ok {
			return spqrerror.Newf(spqrerror.SPQR_TRANSFER_ERROR, "relation \"%s\" not found in distribution \"%s\"", taskGroup.BoundRel, ds.Id)
		}
	}

	var delayedError error
	for {
		if taskGroup.CurrentTask == nil {
			if taskGroup.BoundRel == "" {
				break
			}
			if err := sourceConn.Ping(ctx); err != nil {
				sourceConn, err = datatransfers.GetMasterConnection(ctx, sourceShardConn)
				if err != nil {
					return fmt.Errorf("failed to re-setup connection with source shard: %s", err)
				}
			}

			newTask, err := qc.getNextMoveTask(ctx, sourceConn, taskGroup, rel, ds)
			if err != nil {
				if te, ok := err.(*spqrerror.SpqrError); ok && te.ErrorCode == spqrerror.SPQR_STOP_MOVE_TASK_GROUP {
					delayedError = te
				} else {
					return fmt.Errorf("failed to get new move task: %s", err)
				}
			}
			if newTask != nil {
				taskGroup.CurrentTask = newTask
				if err := qc.QDB().WriteMoveTask(ctx, tasks.MoveTaskToDb(newTask)); err != nil {
					return fmt.Errorf("failed to save move task: %s", err)
				}
			} else {
				break
			}
		}
		task := taskGroup.CurrentTask
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
			if task.Bound != nil {
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
			taskGroup.CurrentTask = nil
			// TODO: get exact key count here
			taskGroup.TotalKeys += taskGroup.BatchSize
			// TODO: wrap in transaction inside etcd
			if err := qc.QDB().UpdateMoveTaskGroupTotalKeys(ctx, taskGroup.ID, taskGroup.TotalKeys); err != nil {
				return err
			}
			if err := qc.QDB().RemoveMoveTask(ctx, task.ID); err != nil {
				return err
			}
		}
	}
	if err := qc.RemoveMoveTaskGroup(ctx, taskGroup.ID); err != nil {
		return err
	}
	return delayedError
}

// RetryMoveTaskGroup re-launches the current move task group.
// If no move task group is currently being executed, then nothing is done.
//
// Parameters:
// - ctx (context.Context): The context for the request.
//
// Returns:
// - error: An error if the operation fails, otherwise nil.
func (qc *ClusteredCoordinator) RetryMoveTaskGroup(ctx context.Context, id string) error {
	taskGroup, err := qc.GetMoveTaskGroup(ctx, id)
	if err != nil {
		return spqrerror.Newf(spqrerror.SPQR_TRANSFER_ERROR, "failed to get move task group: %s", err)
	}
	return qc.executeMoveTasks(ctx, taskGroup)
}

// StopMoveTaskGroup gracefully stops the execution of current move task group.
// When current move task is completed, move task group will be finished.
//
// Parameters:
// - ctx (context.Context): The context for the request.
//
// Returns:
// - error: An error if the operation fails, otherwise nil.
func (qc *ClusteredCoordinator) StopMoveTaskGroup(ctx context.Context, id string) error {
	return qc.QDB().AddMoveTaskGroupStopFlag(ctx, id)
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
				if te, ok := err.(*spqrerror.SpqrError); ok && te.ErrorCode == spqrerror.SPQR_STOP_MOVE_TASK_GROUP {
					spqrlog.Zero.Error().Msg("finishing redistribute task due to task group stop")
					if err2 := qc.db.RemoveRedistributeTask(ctx); err2 != nil {
						return err2
					}
					return err
				}
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
			return qc.db.RemoveRedistributeTask(ctx)
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
		cl := proto.NewKeyRangeServiceClient(cc)
		_, err := cl.RenameKeyRange(ctx, &proto.RenameKeyRangeRequest{KeyRangeId: krId, NewKeyRangeId: krIdNew})

		return err
	})
}

// TODO : unit tests
func (qc *ClusteredCoordinator) SyncRouterMetadata(ctx context.Context, qRouter *topology.Router) error {
	spqrlog.Zero.Debug().
		Str("address", qRouter.Address).
		Msg("qdb coordinator: sync router metadata")

	cc, err := qc.getOrCreateRouterConn(qRouter)
	if err != nil {
		return err
	}

	// Configure distributions
	dsCl := proto.NewDistributionServiceClient(cc)
	spqrlog.Zero.Debug().Msg("qdb coordinator: configure distributions")
	dss, err := qc.ListDistributions(ctx)
	if err != nil {
		return err
	}
	resp, err := dsCl.ListDistributions(ctx, nil)
	if err != nil {
		return err
	}
	if _, err = dsCl.DropDistribution(ctx, &proto.DropDistributionRequest{
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
	if _, err = dsCl.CreateDistribution(ctx, &proto.CreateDistributionRequest{
		Distributions: func() []*proto.Distribution {
			res := make([]*proto.Distribution, len(dss))
			for i, ds := range dss {
				res[i] = distributions.DistributionToProto(ds)
			}
			return res
		}(),
	}); err != nil {
		return err
	}

	// Configure key ranges.
	krClient := proto.NewKeyRangeServiceClient(cc)
	spqrlog.Zero.Debug().Msg("qdb coordinator: configure key ranges")
	if _, err = krClient.DropAllKeyRanges(ctx, nil); err != nil {
		return err
	}

	for _, ds := range dss {
		krs, err := qc.db.ListKeyRanges(ctx, ds.Id)
		if err != nil {
			return err
		}
		krsInt := make([]*kr.KeyRange, len(krs))
		for i, kRange := range krs {
			krsInt[i], err = kr.KeyRangeFromDB(kRange, ds.ColTypes)
			if err != nil {
				return err
			}
		}
		sort.Slice(krsInt, func(i, j int) bool {
			return !kr.CmpRangesLess(krsInt[i].LowerBound, krsInt[j].LowerBound, ds.ColTypes)
		})

		for _, kRange := range krsInt {
			if err != nil {
				return err
			}
			resp, err := krClient.CreateKeyRange(ctx, &proto.CreateKeyRangeRequest{
				KeyRangeInfo: kRange.ToProto(),
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
	rCl := proto.NewTopologyServiceClient(cc)
	if _, err := rCl.UpdateCoordinator(ctx, &proto.UpdateCoordinatorRequest{
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

	cc, err := qc.getOrCreateRouterConn(qRouter)
	if err != nil {
		return err
	}

	/* Update current coordinator address. */
	/* Todo: check that router metadata is in sync. */

	host, err := config.GetHostOrHostname(config.CoordinatorConfig().Host)
	if err != nil {
		return err
	}
	rCl := proto.NewTopologyServiceClient(cc)
	if _, err := rCl.UpdateCoordinator(ctx, &proto.UpdateCoordinatorRequest{
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
	cl := proto.NewTopologyServiceClient(conn)
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
				_ = cli.ReportError(fmt.Errorf("failed to parse query \"%s\": %w", v.String, err))
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
		c := proto.NewTopologyServiceClient(cc)
		spqrlog.Zero.Debug().Str("address", address).Msg("updating coordinator address")
		_, err := c.UpdateCoordinator(ctx, &proto.UpdateCoordinatorRequest{
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
		cl := proto.NewReferenceRelationsServiceClient(cc)
		resp, err := cl.CreateReferenceRelations(context.TODO(),
			&proto.CreateReferenceRelationsRequest{
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

func (qc *ClusteredCoordinator) SyncReferenceRelations(ctx context.Context, relNames []*rfqn.RelationFQN, destShard string) error {
	if err := qc.Coordinator.SyncReferenceRelations(ctx, relNames, destShard); err != nil {
		return err
	}

	return qc.traverseRouters(ctx, func(cc *grpc.ClientConn) error {
		cl := proto.NewReferenceRelationsServiceClient(cc)

		for _, relName := range relNames {

			rel, err := qc.GetReferenceRelation(ctx, relName)

			if err != nil {
				return err
			}

			resp, err := cl.AlterReferenceRelationStorage(context.TODO(),
				&proto.AlterReferenceRelationStorageRequest{
					Relation: &proto.QualifiedName{
						RelationName: relName.RelationName,
						SchemaName:   relName.SchemaName,
					},
					ShardIds: rel.ShardIds,
				})
			if err != nil {
				return err
			}

			spqrlog.Zero.Debug().
				Interface("response", resp).
				Strs("shards", rel.ShardIds).
				Msg("sync reference relation response")
		}

		return nil
	})
}

// TODO: unit tests
func (qc *ClusteredCoordinator) DropReferenceRelation(ctx context.Context,
	relName *rfqn.RelationFQN) error {
	if err := qc.Coordinator.DropReferenceRelation(ctx, relName); err != nil {
		return err
	}

	return qc.traverseRouters(ctx, func(cc *grpc.ClientConn) error {
		cl := proto.NewReferenceRelationsServiceClient(cc)
		resp, err := cl.DropReferenceRelations(context.TODO(),
			&proto.DropReferenceRelationsRequest{
				Relations: []*proto.QualifiedName{{
					RelationName: relName.RelationName,
					SchemaName:   relName.SchemaName,
				}},
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
		cl := proto.NewDistributionServiceClient(cc)
		resp, err := cl.CreateDistribution(context.TODO(), &proto.CreateDistributionRequest{
			Distributions: []*proto.Distribution{distributions.DistributionToProto(ds)},
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
		cl := proto.NewDistributionServiceClient(cc)
		resp, err := cl.DropDistribution(context.TODO(), &proto.DropDistributionRequest{
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
		cl := proto.NewDistributionServiceClient(cc)
		resp, err := cl.AlterDistributionAttach(context.TODO(), &proto.AlterDistributionAttachRequest{
			Id: id,
			Relations: func() []*proto.DistributedRelation {
				res := make([]*proto.DistributedRelation, len(rels))
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
		cl := proto.NewDistributionServiceClient(cc)
		resp, err := cl.AlterDistributedRelation(context.TODO(), &proto.AlterDistributedRelationRequest{
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

// AlterDistributedRelationSchema changes the schema name of a relation attached to a distribution
// TODO: unit tests
func (qc *ClusteredCoordinator) AlterDistributedRelationSchema(ctx context.Context, id string, relName string, schemaName string) error {
	if err := qc.Coordinator.AlterDistributedRelationSchema(ctx, id, relName, schemaName); err != nil {
		return err
	}

	return qc.traverseRouters(ctx, func(cc *grpc.ClientConn) error {
		cl := proto.NewDistributionServiceClient(cc)
		resp, err := cl.AlterDistributedRelationSchema(context.TODO(), &proto.AlterDistributedRelationSchemaRequest{
			Id:           id,
			RelationName: relName,
			SchemaName:   schemaName,
		})
		if err != nil {
			return err
		}

		spqrlog.Zero.Debug().
			Interface("response", resp).
			Msg("alter relation schema response")
		return nil
	})
}

// AlterDistributedRelationSchema changes the distribution key of a relation attached to a distribution
// TODO: unit tests
func (qc *ClusteredCoordinator) AlterDistributedRelationDistributionKey(ctx context.Context, id string, relName string, distributionKey []distributions.DistributionKeyEntry) error {
	if err := qc.Coordinator.AlterDistributedRelationDistributionKey(ctx, id, relName, distributionKey); err != nil {
		return err
	}

	return qc.traverseRouters(ctx, func(cc *grpc.ClientConn) error {
		cl := proto.NewDistributionServiceClient(cc)
		resp, err := cl.AlterDistributedRelationDistributionKey(context.TODO(), &proto.AlterDistributedRelationDistributionKeyRequest{
			Id:              id,
			RelationName:    relName,
			DistributionKey: distributions.DistributionKeyToProto(distributionKey),
		})
		if err != nil {
			return err
		}

		spqrlog.Zero.Debug().
			Interface("response", resp).
			Msg("alter relation distribution key response")
		return nil
	})
}

func (qc *ClusteredCoordinator) DropSequence(ctx context.Context, seqName string, force bool) error {
	if err := qc.Coordinator.DropSequence(ctx, seqName, force); err != nil {
		return err
	}
	return qc.traverseRouters(ctx, func(cc *grpc.ClientConn) error {
		cl := proto.NewDistributionServiceClient(cc)
		resp, err := cl.DropSequence(context.TODO(), &proto.DropSequenceRequest{
			Name:  seqName,
			Force: force,
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
func (qc *ClusteredCoordinator) AlterDistributionDetach(ctx context.Context, id string, relName *rfqn.RelationFQN) error {
	/* Do what needs to be done in metadata */
	if err := qc.Coordinator.AlterDistributionDetach(ctx, id, relName); err != nil {
		return err
	}

	return qc.traverseRouters(ctx, func(cc *grpc.ClientConn) error {
		cl := proto.NewDistributionServiceClient(cc)
		protoRelation := proto.QualifiedName{RelationName: relName.RelationName, SchemaName: relName.SchemaName}
		resp, err := cl.AlterDistributionDetach(context.TODO(), &proto.AlterDistributionDetachRequest{
			Id:       id,
			RelNames: []*proto.QualifiedName{&protoRelation},
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

func (qc *ClusteredCoordinator) IsReadOnly() bool {
	return !qc.acquiredLock
}

func gossipMetaChanges(ctx context.Context, gossip *proto.MetaTransactionGossipRequest) func(cc *grpc.ClientConn) error {
	return func(cc *grpc.ClientConn) error {
		cl := proto.NewMetaTransactionGossipServiceClient(cc)
		resp, err := cl.ApplyMeta(ctx, gossip)
		spqrlog.Zero.Debug().Err(err).
			Interface("response", resp).
			Msg("send meta transaction gossip")
		return err
	}
}

func (qc *ClusteredCoordinator) ExecNoTran(ctx context.Context, chunk *mtran.MetaTransactionChunk) error {
	for _, gossipRequest := range chunk.GossipRequests {
		if gossipType := mtran.GetGossipRequestType(gossipRequest); gossipType == mtran.GR_UNKNOWN {
			return fmt.Errorf("invalid meta transaction request (exec no tran)")
		}
	}
	noGossipChunk, _ := mtran.NewMetaTransactionChunk(nil, chunk.QdbStatements)
	if err := qc.Coordinator.ExecNoTran(ctx, noGossipChunk); err != nil {
		return err
	} else {
		return qc.traverseRouters(ctx,
			gossipMetaChanges(ctx, &proto.MetaTransactionGossipRequest{Commands: chunk.GossipRequests}),
		)
	}
}

func (qc *ClusteredCoordinator) CommitTran(ctx context.Context, transaction *mtran.MetaTransaction) error {
	for _, gossipRequest := range transaction.Operations.GossipRequests {
		if gossipType := mtran.GetGossipRequestType(gossipRequest); gossipType == mtran.GR_UNKNOWN {
			return fmt.Errorf("invalid meta transaction request (commit tran)")
		}
	}
	noGossipTran := mtran.ToNoGossipTransaction(transaction)

	if err := qc.Coordinator.CommitTran(ctx, noGossipTran); err != nil {
		return err
	}
	return qc.traverseRouters(ctx,
		gossipMetaChanges(ctx,
			&proto.MetaTransactionGossipRequest{Commands: transaction.Operations.GossipRequests},
		),
	)
}

func (qc *ClusteredCoordinator) BeginTran(ctx context.Context) (*mtran.MetaTransaction, error) {
	return qc.Coordinator.BeginTran(ctx)
}
