package coord

import (
	"context"
	"crypto/tls"
	"encoding/binary"
	"errors"
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
	"github.com/pg-sharding/spqr/pkg/icp"
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
	"github.com/pg-sharding/spqr/pkg/transferworker"
	"github.com/pg-sharding/spqr/pkg/tsa"
	"github.com/pg-sharding/spqr/qdb"
	"github.com/pg-sharding/spqr/router/cache"
	rclient "github.com/pg-sharding/spqr/router/client"
	"github.com/pg-sharding/spqr/router/port"
	"github.com/pg-sharding/spqr/router/rfqn"
	"github.com/pg-sharding/spqr/router/route"
	spqrparser "github.com/pg-sharding/spqr/yacc/console"
	"github.com/sethvargo/go-retry"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
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
// ActiveTCPCount implements connmgr.ConnectionStatMgr.
func (ci grpcConnMgr) ActiveTCPCount() int64 {
	return 0
}

// TODO implement it
// ActiveTCPCount implements connmgr.ConnectionStatMgr
func (ci grpcConnMgr) TotalCancelCount() int64 {
	return 0
}

// TODO implement it
// ActiveTCPCount implements connmgr.ConnectionStatMgr
func (ci grpcConnMgr) TotalTCPCount() int64 {
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
func (ci grpcConnMgr) IterRouterSkipUnreachable(cb func(cc *grpc.ClientConn, addr string) error) error {
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
			continue
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
	return ci.IterRouterSkipUnreachable(func(cc *grpc.ClientConn, addr string) error {
		ctx := context.TODO()
		rrClient := proto.NewClientInfoServiceClient(cc)

		spqrlog.Zero.Debug().Msg("fetch clients with grpc")
		resp, err := rrClient.ListClients(ctx, nil)
		if err != nil {
			if st, ok := status.FromError(err); ok {
				if st.Code() == codes.Unavailable {
					// ignore this router
					spqrlog.Zero.Err(err).Msg("router is unavailable")
					return nil
				}
			}
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
func (ci grpcConnMgr) Put(_ client.Client) error {
	return spqrerror.New(spqrerror.SPQR_NOT_IMPLEMENTED, "grpcConnectionIterator put not implemented")
}

// TODO : implement
// TODO : unit tests
func (ci grpcConnMgr) ReportError(string) {
}

func (ci grpcConnMgr) ErrorCounts() map[string]uint64 {
	return nil
}

// TODO : implement
// TODO : unit tests
func (ci grpcConnMgr) Pop(_ uint) (bool, error) {
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
	return ci.IterRouter(func(cc *grpc.ClientConn, _ string) error {
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
		grpc.WithKeepaliveParams(keepaliveParams),
		grpc.WithDefaultServiceConfig(getRouterConnRetryPolicy()),
	)
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

	bounds sync.Map
	index  sync.Map

	routerConnCache sync.Map

	dataTransferWorkers sync.Map

	moveTaskWatcherInit sync.Once

	startupFinished bool
	maxTxnBatch     uint16
}

func (qc *ClusteredCoordinator) QDB() qdb.QDB {
	return qc.db
}

func (qc *ClusteredCoordinator) Cache() *cache.SchemaCache {
	return qc.cache
}

func (qc *ClusteredCoordinator) StartupFinished() bool {
	return qc.startupFinished
}

var _ coordinator.Coordinator = &ClusteredCoordinator{}

// getOrCreateRouterConn returns a cached connection or creates a new one.
func (qc *ClusteredCoordinator) getOrCreateRouterConn(r *topology.Router) (*grpc.ClientConn, func(), error) {
	connRaw, exists := qc.routerConnCache.LoadAndDelete(r.ID)

	if exists {
		conn, ok := connRaw.(*grpc.ClientConn)
		if !ok {
			return nil, func() {}, fmt.Errorf("unexpected connection type %T for router %s", connRaw, r.ID)
		}
		// Check if connection is still valid
		state := conn.GetState()
		if state == connectivity.Ready || state == connectivity.Idle {
			return conn, func() {
				_, loaded := qc.routerConnCache.LoadOrStore(r.ID, conn)
				if loaded {
					/* XXX: Fixup this mess */
					_ = conn.Close()
				}

			}, nil
		}
		// Connection is not healthy, close and remove it
		qc.closeRouterConn(r.ID)
	}

	// Create new connection
	conn, err := DialRouter(r)
	if err != nil {
		return nil, func() {}, err
	}

	return conn, func() {
		_, loaded := qc.routerConnCache.LoadOrStore(r.ID, conn)
		if loaded {
			/* XXX: Fixup this mess */
			_ = conn.Close()
		}

	}, nil
}

// closeRouterConn closes and removes a router connection from cache
func (qc *ClusteredCoordinator) closeRouterConn(routerID string) {

	connRaw, exists := qc.routerConnCache.Load(routerID)

	if exists {
		conn, ok := connRaw.(*grpc.ClientConn)
		if !ok {
			return
		}
		_ = conn.Close()
		qc.routerConnCache.Delete(routerID)
	}
}

// GetRouterConn implements meta.RouterConnector interface.
// It returns a gRPC connection to the specified router.
func (qc *ClusteredCoordinator) GetRouterConn(r *topology.Router) (*grpc.ClientConn, func(), error) {
	return qc.getOrCreateRouterConn(r)
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
			spqrlog.Zero.Error().Err(err).Msg("failed to list routers")
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

				cc, cf, err := qc.getOrCreateRouterConn(internalR)
				if err != nil {
					return err
				}

				defer cf()

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
		var staleConnIDs []string
		qc.routerConnCache.Range(func(k, _ any) bool {
			routerID, ok := k.(string)
			if !ok {
				return true
			}
			if !currentRouterIDs[routerID] {
				staleConnIDs = append(staleConnIDs, routerID)
			}
			return true
		})

		for _, routerID := range staleConnIDs {
			spqrlog.Zero.Debug().Str("router-id", routerID).Msg("cleaning up connection for removed router")
			qc.closeRouterConn(routerID)
		}

		time.Sleep(config.ValueOrDefaultDuration(config.CoordinatorConfig().IterationTimeout, defaultWatchRouterTimeout))
	}
}

func NewClusteredCoordinator(tlsconfig *tls.Config, db qdb.XQDB, maxTxnBatch uint16) (*ClusteredCoordinator, error) {
	return &ClusteredCoordinator{
		Coordinator:         coord.NewCoordinator(db, nil, maxTxnBatch),
		db:                  db,
		tlsconfig:           tlsconfig,
		rmgr:                rulemgr.NewMgr(config.CoordinatorConfig().FrontendRules, []*config.BackendRule{}),
		acquiredLock:        false,
		bounds:              sync.Map{},
		index:               sync.Map{},
		routerConnCache:     sync.Map{},
		dataTransferWorkers: sync.Map{},
		maxTxnBatch:         maxTxnBatch,
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
			Address: net.JoinHostPort(routerHost, config.RouterConfig().GrpcAPIPort),
			State:   qdb.OPENED,
		}
		if err := qc.RegisterRouter(ctx, router); err != nil {
			spqrlog.Zero.Error().Err(err).Msg("register router when locking coordinator")
		}

		host, err := config.GetHostOrHostname(config.CoordinatorConfig().Host)
		if err != nil {
			return err
		}
		coordAddr := net.JoinHostPort(host, config.CoordinatorConfig().GrpcAPIPort)
		return qc.UpdateCoordinator(ctx, coordAddr)
	}

	lock := func(ctx context.Context) error {
		currentCoord, _ := qc.GetCoordinator(ctx)
		host, err := config.GetHostOrHostname(config.CoordinatorConfig().Host)
		if err != nil {
			return err
		}
		addr := net.JoinHostPort(host, config.CoordinatorConfig().GrpcAPIPort)
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
		spqrlog.Zero.Log().Err(err).Msg("error getting qdb lock, retrying")

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
			topologyMap := topology.DataShardMapFromShardConnectConfig(shards.ShardsData)
			for id, shard := range topologyMap {
				if _, err := qc.db.GetShard(context.TODO(), id); err == nil {
					spqrlog.Zero.Debug().
						Str("shard", id).
						Msg("already exists. creating shard skipped")
					continue
				}
				if err := qc.AddDataShard(context.TODO(), shard); err != nil {
					spqrlog.Zero.Error().
						Err(err).
						Msg("failed to add shard")
				}
			}
			shardList, err := qc.db.GetTxMetaStorage(ctx)
			if err != nil {
				spqrlog.Zero.Error().Err(err).Msg("failed to get two phase tx storage shards")
			} else if len(shardList) == 0 && len(shards.ShardsData) > 0 {
				shardIDs := make([]string, 0, len(shards.ShardsData))
				for id := range shards.ShardsData {
					shardIDs = append(shardIDs, id)
				}
				firstShardID := slices.Min(shardIDs)
				s := []string{firstShardID}
				if err := qc.db.SetTxMetaStorage(ctx, s); err != nil {
					spqrlog.Zero.Error().Err(err).Strs("shard ids", s).Msg("failed to set two phase tx storage shards")
				}
			}
		}
		if err := qc.setUpSPQRGuard(ctx); err != nil {
			spqrlog.Zero.Error().Err(err).Msg("failed to set up spqrguard on shards")
		}
	}

	go qc.watchTaskGroups(context.TODO())

	ranges, err := qc.db.ListAllKeyRanges(context.TODO())
	if err != nil {
		spqrlog.Zero.Error().
			Err(err).
			Msg("failed to list key ranges")
	}

	wg := sync.WaitGroup{}
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
				KeyRangeID: move.KeyRangeID,
				ShardID:    move.ShardId,
			}
		} else if tx != nil {
			krm = &kr.MoveKeyRange{
				KeyRangeID: r.KeyRangeID,
				ShardID:    tx.ToShardId,
			}
		}

		if krm != nil {
			wg.Go(func() {
				spqrlog.Zero.Error().Str("key range id", krm.KeyRangeID).Str("shard id", krm.ShardID).Msg("finish key range move in progress")
				if qc.Move(context.TODO(), krm) != nil {
					spqrlog.Zero.Error().Err(err).Msg("error moving key range")
				}
			})
		}
	}
	wg.Wait()

	qc.startupFinished = true

	go qc.watchRouters(context.TODO())
}

// TODO: move down
func (qc *ClusteredCoordinator) setUpSPQRGuard(ctx context.Context) error {
	spqrlog.Zero.Debug().Msg("start setting up spqrguard")

	distributedRelations := make([]*rfqn.RelationFQN, 0)
	referenceRelations := make([]*rfqn.RelationFQN, 0)
	dss, err := qc.ListDistributions(ctx)
	if err != nil {
		return err
	}
	relsSet := make(map[string]struct{})
	for _, ds := range dss {
		if ds.Id == distributions.REPLICATED {
			for _, rel := range ds.FQNRelations {
				referenceRelations = append(referenceRelations, rel.Relation)
				relsSet[rel.Relation.String()] = struct{}{}
			}
			for _, rel := range ds.Relations {
				if _, ok := relsSet[rel.Relation.String()]; !ok {
					referenceRelations = append(referenceRelations, rel.Relation)
					relsSet[rel.Relation.String()] = struct{}{}
				}
			}
			continue
		}
		for _, rel := range ds.FQNRelations {
			distributedRelations = append(distributedRelations, rel.Relation)
			relsSet[rel.Relation.String()] = struct{}{}
		}
		for _, rel := range ds.Relations {
			if _, ok := relsSet[rel.Relation.String()]; !ok {
				distributedRelations = append(distributedRelations, rel.Relation)
				relsSet[rel.Relation.String()] = struct{}{}
			}
		}
	}

	return datatransfers.TraverseShards(ctx, datatransfers.SetUpSPQRGuard(distributedRelations, referenceRelations))
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
			cc, cf, err := qc.getOrCreateRouterConn(&topology.Router{
				ID:      rtr.ID,
				Address: rtr.Addr(),
			})
			if err != nil {
				return err
			}

			defer cf()

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

func (qc *ClusteredCoordinator) CreateKeyRange(ctx context.Context, keyRange *kr.KeyRange) ([]qdb.QdbStatement, error) {
	// add key range to metadb
	spqrlog.Zero.Debug().
		Bytes("lower-bound", keyRange.Raw()[0]).
		Str("shard-id", keyRange.ShardID).
		Str("key-range-id", keyRange.ID).
		Msg("add key range")

	return qc.Coordinator.CreateKeyRange(ctx, keyRange)
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
		return fmt.Errorf("failed to split key range in coordinator: %s", err)
	}

	if err := qc.traverseRouters(ctx, func(cc *grpc.ClientConn) error {
		cl := proto.NewKeyRangeServiceClient(cc)
		resp, err := cl.SplitKeyRange(ctx, &proto.SplitKeyRangeRequest{
			Bound:     req.Bound[0], // fix multidim case
			SourceId:  req.SourceID,
			NewId:     req.KeyRangeID,
			SplitLeft: req.SplitLeft,
		})
		spqrlog.Zero.Debug().Err(err).
			Interface("response", resp).
			Msg("split key range response")
		if err != nil {
			return fmt.Errorf("failed to split key range in router \"%s\": %s", cc.Target(), err)
		}
		return nil
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
func (qc *ClusteredCoordinator) DropKeyRange(ctx context.Context, id string) ([]qdb.QdbStatement, error) {
	// TODO: exclusive lock all routers
	spqrlog.Zero.Debug().Msg("qdb coordinator dropping all sharding keys")

	// Drop key range from qdb.
	return qc.Coordinator.DropKeyRange(ctx, id)
}

// TODO : unit tests
func (qc *ClusteredCoordinator) Unite(ctx context.Context, uniteKeyRange *kr.UniteKeyRange) error {
	if err := qc.Coordinator.Unite(ctx, uniteKeyRange); err != nil {
		return err
	}

	return qc.traverseRouters(ctx, func(cc *grpc.ClientConn) error {
		cl := proto.NewKeyRangeServiceClient(cc)
		resp, err := cl.MergeKeyRange(ctx, &proto.MergeKeyRangeRequest{
			BaseId:      uniteKeyRange.BaseKeyRangeID,
			AppendageId: uniteKeyRange.AppendageKeyRangeID,
		})

		spqrlog.Zero.Debug().Err(err).
			Interface("response", resp).
			Msg("merge key range response")
		return err
	})
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

func (qc *ClusteredCoordinator) GetKeyRangeMove(ctx context.Context, krID string) (*qdb.MoveKeyRange, error) {
	ls, err := qc.db.ListKeyRangeMoves(ctx)
	if err != nil {
		return nil, err
	}

	for _, krm := range ls {
		// after the coordinator restarts, it will continue the move that was previously initiated.
		// key range move already exist for this key range
		// complete it first
		if krm.KeyRangeID == krID {
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
		Str("key-range", req.KeyRangeID).
		Str("shard-id", req.ShardID).
		Msg("qdb coordinator move key range")

	keyRange, err := qc.GetKeyRange(ctx, req.KeyRangeID)
	if err != nil {
		return err
	}

	move, err := qc.GetKeyRangeMove(ctx, req.KeyRangeID)
	if err != nil {
		return err
	}

	if move == nil {
		// no need to move data to the same shard
		if keyRange.ShardID == req.ShardID {
			return nil
		}
		// No key range moves in progress
		move = &qdb.MoveKeyRange{
			MoveId:     uuid.NewString(),
			ShardId:    req.ShardID,
			KeyRangeID: req.KeyRangeID,
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
			_, err = qc.LockKeyRange(ctx, req.KeyRangeID)
			if err != nil {
				return err
			}
			if config.CoordinatorConfig().EnableICP {
				if err := icp.CheckControlPoint(nil, icp.AfterLockKeyRangeCP); err != nil {
					spqrlog.Zero.Info().Str("cp", icp.AfterLockKeyRangeCP).Err(err).Msg("error while checking control point")
				}
			}
			if err = qc.db.UpdateKeyRangeMoveStatus(ctx, move.MoveId, qdb.MoveKeyRangeLocked); err != nil {
				return err
			}
			move.Status = qdb.MoveKeyRangeLocked

		case qdb.MoveKeyRangeStarted: // nolint: staticcheck
			// move the data
			ds, err := qc.GetDistribution(ctx, keyRange.Distribution)
			if err != nil {
				return err
			}

			if keyRange.ShardID != req.ShardID {
				err = datatransfers.MoveKeys(ctx, keyRange.ShardID, req.ShardID, keyRange, ds, qc.db, qc, "key_range_move_"+move.MoveId)
				if err != nil {
					spqrlog.Zero.Error().Err(err).Msg("failed to move rows")
					return err
				}
				keyRange.ShardID = req.ShardID
				// TODO: move check to meta layer
				if err := meta.ValidateKeyRangeForModify(ctx, qc, keyRange); err != nil {
					return err
				}
				tranMngr := meta.NewTranEntityManager(qc)
				if err := tranMngr.UpdateKeyRange(ctx, keyRange, ds.ColTypes); err != nil {
					return err
				}
				if err := tranMngr.ExecNoTran(ctx); err != nil {
					return spqrerror.Newf(spqrerror.SPQR_KEYRANGE_ERROR, "failed to update a new key range: %s", err)
				}
			}
			// Notify all routers about scheme changes.
			if err := qc.traverseRouters(ctx, func(cc *grpc.ClientConn) error {
				cl := proto.NewKeyRangeServiceClient(cc)
				moveResp, err := cl.MoveKeyRange(ctx, &proto.MoveKeyRangeRequest{
					Id:        keyRange.ID,
					ToShardId: keyRange.ShardID,
				})
				spqrlog.Zero.Debug().Err(err).
					Interface("response", moveResp).
					Msg("move key range response")
				return err
			}); err != nil {
				return err
			}

			if err := qc.db.UpdateKeyRangeMoveStatus(ctx, move.MoveId, qdb.MoveKeyRangeComplete); err != nil {
				spqrlog.Zero.Error().Err(err).Msg("failed to update key range move status")
			}
			move.Status = qdb.MoveKeyRangeComplete

		case qdb.MoveKeyRangeLocked:
			// move the data
			ds, err := qc.GetDistribution(ctx, keyRange.Distribution)
			if err != nil {
				return err
			}

			err = datatransfers.MoveKeys(ctx, keyRange.ShardID, req.ShardID, keyRange, ds, qc.db, qc, "key_range_move_"+move.MoveId)
			if err != nil {
				spqrlog.Zero.Error().Err(err).Msg("failed to move rows")
				return err
			}
			if config.CoordinatorConfig().EnableICP {
				if err := icp.CheckControlPoint(nil, icp.AfterMoveKeysCP); err != nil {
					spqrlog.Zero.Info().Str("cp", icp.AfterMoveKeysCP).Err(err).Msg("error while checking control point")
				}
			}
			if err = qc.db.UpdateKeyRangeMoveStatus(ctx, move.MoveId, qdb.MoveKeyRangeDataMoved); err != nil {
				return err
			}
			move.Status = qdb.MoveKeyRangeDataMoved
		case qdb.MoveKeyRangeDataMoved:
			keyRange.ShardID = req.ShardID
			ds, err := qc.GetDistribution(ctx, keyRange.Distribution)
			if err != nil {
				return err
			}
			// TODO: move check to meta layer
			if err := meta.ValidateKeyRangeForModify(ctx, qc, keyRange); err != nil {
				return err
			}
			tranMngr := meta.NewTranEntityManager(qc)
			if err := tranMngr.UpdateKeyRange(ctx, keyRange, ds.ColTypes); err != nil {
				return err
			}
			if err := tranMngr.ExecNoTran(ctx); err != nil {
				return spqrerror.Newf(spqrerror.SPQR_KEYRANGE_ERROR, "failed to update a new key range: %s", err)
			}
			if config.CoordinatorConfig().EnableICP {
				if err := icp.CheckControlPoint(nil, icp.AfterCoordUpdateKeyRangeCP); err != nil {
					spqrlog.Zero.Info().Str("cp", icp.AfterCoordUpdateKeyRangeCP).Err(err).Msg("error while checking control point")
				}
			}
			if err = qc.db.UpdateKeyRangeMoveStatus(ctx, move.MoveId, qdb.MoveKeyRangeDataCoordMetaUpdated); err != nil {
				return err
			}
			move.Status = qdb.MoveKeyRangeDataCoordMetaUpdated
		case qdb.MoveKeyRangeDataCoordMetaUpdated:
			// Notify all routers about scheme changes.
			if err := qc.traverseRouters(ctx, func(cc *grpc.ClientConn) error {
				cl := proto.NewKeyRangeServiceClient(cc)
				moveResp, err := cl.MoveKeyRange(ctx, &proto.MoveKeyRangeRequest{
					Id:        keyRange.ID,
					ToShardId: keyRange.ShardID,
				})
				spqrlog.Zero.Debug().Err(err).
					Interface("response", moveResp).
					Msg("move key range response")
				return err
			}); err != nil {
				return err
			}

			if config.CoordinatorConfig().EnableICP {
				if err := icp.CheckControlPoint(nil, icp.AfterRouterUpdateKeyRangeCP); err != nil {
					spqrlog.Zero.Info().Str("cp", icp.AfterRouterUpdateKeyRangeCP).Err(err).Msg("error while checking control point")
				}
			}
			if err := qc.db.UpdateKeyRangeMoveStatus(ctx, move.MoveId, qdb.MoveKeyRangeComplete); err != nil {
				spqrlog.Zero.Error().Err(err).Msg("failed to update key range move status")
			}
			move.Status = qdb.MoveKeyRangeComplete
		case qdb.MoveKeyRangeComplete:
			// unlock key range
			if err := qc.UnlockKeyRange(ctx, req.KeyRangeID); err != nil {
				spqrlog.Zero.Error().Err(err).Msg("failed to unlock key range")
			}
			if config.CoordinatorConfig().EnableICP {
				if err := icp.CheckControlPoint(nil, icp.AfterUnlockKeyRangeCP); err != nil {
					spqrlog.Zero.Info().Str("cp", icp.AfterUnlockKeyRangeCP).Err(err).Msg("error while checking control point")
				}
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
	keyRange, err := qc.GetKeyRange(ctx, req.KeyRangeID)
	if err != nil {
		return err
	}
	if keyRange.ShardID == req.ShardID {
		return nil
	}
	if _, err = qc.GetKeyRange(ctx, req.DestKeyRangeID); err == nil {
		return spqrerror.Newf(spqrerror.SPQR_TRANSFER_ERROR, "key range \"%s\" already exists", req.DestKeyRangeID)
	}
	conns, err := config.LoadShardDataCfg(config.CoordinatorConfig().ShardDataCfg)
	if err != nil {
		return err
	}
	destShardConn, ok := conns.ShardsData[req.ShardID]
	if !ok {
		return spqrerror.New(spqrerror.SPQR_METADATA_CORRUPTION, fmt.Sprintf("destination shard of key range '%s' does not exist in shard data config", keyRange.ID))
	}
	destConn, err := datatransfers.GetMasterConnection(ctx, destShardConn, "redistribute_check_dest_conn")
	if err != nil {
		return err
	}
	defer func() {
		_ = destConn.Close(ctx)
	}()

	sourceShardConn, ok := conns.ShardsData[keyRange.ShardID]
	if !ok {
		return spqrerror.New(spqrerror.SPQR_METADATA_CORRUPTION, fmt.Sprintf("shard of key range '%s' does not exist in shard data config", keyRange.ID))
	}
	sourceConn, err := datatransfers.GetMasterConnection(ctx, sourceShardConn, "redistribute_check_source_conn")
	if err != nil {
		return err
	}
	defer func() {
		_ = sourceConn.Close(ctx)
	}()

	ds, err := qc.GetDistribution(ctx, keyRange.Distribution)
	if err != nil {
		return err
	}

	schemas := make(map[string]struct{})
	rels := make([]string, 0, len(ds.ListRelations()))
	for _, rel := range ds.ListRelations() {
		schemas[rel.Relation.GetSchema()] = struct{}{}
		rels = append(rels, rel.QualifiedName().String())
		sourceTable, err := datatransfers.CheckTableExists(ctx, sourceConn, rel.Relation)
		if err != nil {
			return err
		}
		if !sourceTable {
			spqrlog.Zero.Info().Str("rel", rel.QualifiedName().String()).Msg("source table does not exist")
			return spqrerror.Newf(spqrerror.SPQR_TRANSFER_ERROR, "relation \"%s\" does not exist on the source shard, possible misconfiguration of schema names", rel.QualifiedName())
		}
		for _, col := range rel.DistributionKey {
			exists, err := datatransfers.CheckColumnExists(ctx, sourceConn, rel.Relation, col.Column)
			if err != nil {
				return err
			}
			if !exists {
				return spqrerror.Newf(spqrerror.SPQR_TRANSFER_ERROR, "distribution key column \"%s\" not found in relation \"%s\" on source shard", col.Column, rel.QualifiedName())
			}
		}
		destTable, err := datatransfers.CheckTableExists(ctx, destConn, rel.Relation)
		if err != nil {
			return err
		}
		if !destTable {
			return spqrerror.Newf(spqrerror.SPQR_TRANSFER_ERROR, "relation \"%s\" does not exist on the destination shard", rel.QualifiedName())
		}
		// TODO check whole table schema for compatibility
		for _, col := range rel.DistributionKey {
			exists, err := datatransfers.CheckColumnExists(ctx, destConn, rel.Relation, col.Column)
			if err != nil {
				return err
			}
			if !exists {
				return spqrerror.Newf(spqrerror.SPQR_TRANSFER_ERROR, "distribution key column \"%s\" not found in relation \"%s\" on destination shard", col.Column, rel.QualifiedName())
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
			return fmt.Errorf(
				"error getting replicated distribution: %s", err)
		}
		replRels = make([]string, 0, len(replDs.Relations))

		for _, r := range replDs.ListRelations() {

			relExists, err := datatransfers.CheckTableExists(ctx, sourceConn, r.Relation)
			if err != nil {
				return fmt.Errorf(
					"failed to check for relation \"%s\" existence on source shard: %s", r.QualifiedName(), err)
			}
			if relExists {
				destRelExists, err := datatransfers.CheckTableExists(ctx, destConn, r.Relation)
				if err != nil {
					return fmt.Errorf(
						"failed to check for relation \"%s\" existence on destination shard: %s", r.QualifiedName(), err)
				}
				if !destRelExists {
					return fmt.Errorf(
						"replicated relation \"%s\" exists on source shard, but not on destination shard", r.QualifiedName())
				}
				replRels = append(replRels, r.QualifiedName().String())
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

	hasSpqrHash, err := shard.CheckExtension(ctx, sourceConn, "spqrhash", "1.1")
	if err != nil {
		return spqrerror.Newf(spqrerror.SPQR_TRANSFER_ERROR, "error checking for spqrhash extension on source shard: %s", err)
	}
	if !hasSpqrHash {
		return spqrerror.New(spqrerror.SPQR_TRANSFER_ERROR, "extension \"spqrhash\" not installed on source shard")
	}

	hasSpqrHash, err = shard.CheckExtension(ctx, destConn, "spqrhash", "1.1")
	if err != nil {
		return spqrerror.Newf(spqrerror.SPQR_TRANSFER_ERROR, "error checking for spqrhash extension on destination shard: %s", err)
	}
	if !hasSpqrHash {
		return spqrerror.New(spqrerror.SPQR_TRANSFER_ERROR, "extension \"spqrhash\" not installed on destination shard")
	}

	if err := datatransfers.SetupFDW(ctx, destConn, keyRange.ShardID, req.ShardID, schemas); err != nil {
		spqrlog.Zero.Error().Err(err).Msg("failed to setup move data FDW")
		return err
	}
	return nil
}

func (qc *ClusteredCoordinator) TaskWorkersID() []string {
	ret := []string{}

	qc.dataTransferWorkers.Range(func(k, _ any) bool {
		ret = append(ret, k.(string))
		return true
	})

	return ret
}

func (qc *ClusteredCoordinator) TaskState(id string) (*transferworker.TaskGroupWorkerState, error) {
	st, ok := qc.dataTransferWorkers.Load(id)
	if ok {
		state, ok := st.(*transferworker.TaskGroupWorkerState)
		if !ok {
			return nil, fmt.Errorf("unexpected state type %T for task %q", st, id)
		}
		return state, nil
	}

	return nil, fmt.Errorf("no such task \"%v\"", id)
}

/*
* Workhorse for all move data operations
 */
func (qc *ClusteredCoordinator) executeMoveInternal(
	ctx context.Context,
	taskGroup *tasks.MoveTaskGroup,
	nowait bool,
	invalidateTG bool,
) error {

	/* TODO: do not lose move data goroutine here,
	* remember its id in structure similar to client pool. */

	if invalidateTG && taskGroup != nil {
		qc.invalidateTaskGroupCache(taskGroup.ID)
	}

	execCtx, cancel := context.WithCancel(ctx)

	ch := make(chan error)
	qc.dataTransferWorkers.Store(taskGroup.ID, &transferworker.TaskGroupWorkerState{
		Cancel: cancel,
	})

	qc.moveTaskWatcherInit.Do(qc.bootstrapWatcher(context.TODO()))

	go func() {
		ch <- qc.executeMoveTaskGroup(execCtx, taskGroup)
		qc.dataTransferWorkers.Delete(taskGroup.ID)
	}()

	if !nowait {
		defer cancel()
		for {
			select {
			case <-ctx.Done():
				return spqrerror.NewByCode(spqrerror.SPQR_TRANSFER_ERROR)
			case err := <-ch:
				if err != nil {
					_ = qc.db.DropTaskGroupLock(ctx, taskGroup.ID)
					_ = qc.QDB().WriteTaskGroupStatus(ctx, taskGroup.ID, &qdb.TaskGroupStatus{State: string(tasks.TaskGroupError), Message: err.Error()})
				}
				return err
			}
		}
	}
	return nil
}

func (qc *ClusteredCoordinator) bootstrapWatcher(ctx context.Context) func() {

	return func() {

		go func() {
			/* XXX: configure this? */
			t := time.Tick(time.Second)

			for {
				select {
				case <-t:

					qc.dataTransferWorkers.Range(func(k, v any) bool {
						id, ok := k.(string)
						if !ok {
							return true
						}
						st, ok := v.(*transferworker.TaskGroupWorkerState)
						if !ok {
							return true
						}
						spqrlog.Zero.Debug().Str("id", id).Msg("rechecking task aliveness")
						stop, immediate, err := qc.QDB().CheckMoveTaskGroupStopFlag(ctx, id)
						if err != nil {
							spqrlog.Zero.Info().Err(err).Msg("failed to check for stop flag:")
						}

						// TODO create special error type here, use it to stop redistribute/balancer tasks
						if stop && immediate {

							/* Ideally, client should receive this error
							 spqrerror.Newf(
								spqrerror.SPQR_STOP_MOVE_TASK_GROUP,
								"move task stopped by STOP MOVE TASK GROUP command") */

							st.Cancel()
						}

						return true
					})
				case <-ctx.Done():
					return
				}
			}
		}()
	}
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
func (qc *ClusteredCoordinator) BatchMoveKeyRange(ctx context.Context, req *kr.BatchMoveKeyRange, issuer *tasks.MoveTaskGroupIssuer) error {
	if err := statistics.RecordMoveStart(time.Now()); err != nil {
		spqrlog.Zero.Error().Err(err).Msg("failed to record key range move start in statistics")
	}
	keyRange, err := qc.GetKeyRange(ctx, req.KeyRangeID)
	if err != nil {
		return err
	}
	if keyRange.ShardID == req.ShardID {
		return nil
	}
	if _, err = qc.GetKeyRange(ctx, req.DestKeyRangeID); err == nil {
		return spqrerror.Newf(spqrerror.SPQR_TRANSFER_ERROR, "key range \"%s\" already exists", req.DestKeyRangeID)
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
	sourceConn, err := datatransfers.GetMasterConnection(ctx, sourceShardConn, "get_key_stats")
	if err != nil {
		return err
	}
	defer func() {
		_ = sourceConn.Close(ctx)
	}()

	totalCount, relCount, err := qc.getKeyStats(ctx, sourceConn, ds.Relations, keyRange, nextBound)
	if err != nil {
		return err
	}
	var taskGroup *tasks.MoveTaskGroup

	tgID := req.TaskGroupID

	if tgID == "" {
		tgID = uuid.NewString()
	}

	if totalCount != 0 {
		biggestRelName, coeff := qc.getBiggestRelation(relCount, totalCount)
		taskGroup = &tasks.MoveTaskGroup{
			ID:        tgID,
			KridFrom:  req.KeyRangeID,
			KridTo:    req.DestKeyRangeID,
			ShardToID: req.ShardID,
			Type:      req.Type,
			BoundRel:  biggestRelName,
			Coeff:     coeff,
			BatchSize: int64(req.BatchSize),
			Limit:     req.Limit,
			Issuer:    issuer,
		}
	} else {
		taskGroup = &tasks.MoveTaskGroup{
			ID:        tgID,
			KridFrom:  req.KeyRangeID,
			KridTo:    req.DestKeyRangeID,
			ShardToID: req.ShardID,
			Type:      req.Type,
			CurrentTask: &tasks.MoveTask{
				ID:          uuid.NewString(),
				TaskGroupID: tgID,
				KridTemp:    req.DestKeyRangeID,
				State:       tasks.TaskPlanned,
				Bound:       nil,
			},
			Issuer: issuer,
		}
	}
	spqrlog.Zero.Debug().Str("taskGroup", fmt.Sprintf("%#v", taskGroup)).Msg("got task group")

	if err := qc.WriteMoveTaskGroup(ctx, taskGroup); err != nil {
		return err
	}
	if err := qc.QDB().WriteTaskGroupStatus(ctx, taskGroup.ID, &qdb.TaskGroupStatus{State: string(tasks.TaskGroupPlanned)}); err != nil {
		spqrlog.Zero.Error().Str("task group ID", taskGroup.ID).Err(err).Msg("failed to write task group status")
	}

	return qc.executeMoveInternal(ctx, taskGroup, false /*actually, does not matter here*/, false /* `nowait`, no we want to wait*/)
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
		relExists, err := datatransfers.CheckTableExists(ctx, conn, rel.Relation)
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
`, rel.QualifiedName(), cond)
		row := conn.QueryRow(ctx, query)
		var count int64
		if err = row.Scan(&count); err != nil {
			return 0, nil, err
		}
		relationCount[rel.Relation.RelationName] = count
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
	if groupBoundsInt, ok := qc.bounds.Load(taskGroup.ID); ok {
		indMap, _ := qc.index.Load(taskGroup.ID)
		groupBounds, ok := groupBoundsInt.([][][]byte)
		if !ok {
			return nil, fmt.Errorf("unexpected bounds type %T for task group %s", groupBoundsInt, taskGroup.ID)
		}
		ind, ok := indMap.(int)
		if !ok {
			return nil, fmt.Errorf("unexpected index type %T for task group %s", indMap, taskGroup.ID)
		}
		if ind < len(groupBounds) {
			qc.index.Store(taskGroup.ID, ind+1)
			return groupBounds[ind], nil
		}
	}

	spqrlog.Zero.Debug().Str("task group id", taskGroup.ID).Msg("generating new bounds batch")
	keyRange, err := qc.GetKeyRange(ctx, taskGroup.KridFrom)
	krFound := true
	if et, ok := err.(*spqrerror.SpqrError); ok && et.ErrorCode == spqrerror.SPQR_KEYRANGE_ERROR {
		krFound = false
		spqrlog.Zero.Debug().Str("task group id", taskGroup.ID).Str("key range", taskGroup.KridFrom).Msg("key range already moved")
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
	query := fmt.Sprintf(datatransfers.CalculateSplitBounds,
		selectAsColumns,
		orderByClause,
		rel.QualifiedName(),
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
		rel.QualifiedName(),
		condition,
		subColumns,
		subColumns,
		sort,
	)

	spqrlog.Zero.Debug().Str("task group id", taskGroup.ID).Str("query", query).Msg("get split bound")
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
			spqrlog.Zero.Error().Str("task group id", taskGroup.ID).Err(err).Str("rel", rel.QualifiedName().String()).Msg("error getting move tasks")
			return nil, err
		}
		for i, value := range values[:len(values)-1] {
			spqrlog.Zero.Debug().Str("task group id", taskGroup.ID).Str("value", value).Int("index", i).Msg("got split bound")
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
	qc.bounds.Store(taskGroup.ID, boundList)
	qc.index.Store(taskGroup.ID, 1)
	return boundList[0], nil
}

func (qc *ClusteredCoordinator) getNextMoveTask(
	ctx context.Context,
	conn *pgx.Conn,
	taskGroup *tasks.MoveTaskGroup,
	rel *distributions.DistributedRelation,
	ds *distributions.Distribution) (*tasks.MoveTask, error) {

	if taskGroup.Limit > 0 && taskGroup.TotalKeys >= taskGroup.Limit {
		return nil, nil
	}

	stop, _, err := qc.QDB().CheckMoveTaskGroupStopFlag(ctx, taskGroup.ID)
	if err != nil {
		return nil, fmt.Errorf("failed to check for stop flag: %s", err)
	}
	// TODO create special error type here, use it to stop redistribute/balancer tasks
	if stop {
		spqrlog.Zero.Info().Msg("received stop flag, gracefully stopping move task group")
		return nil, spqrerror.Newf(
			spqrerror.SPQR_STOP_MOVE_TASK_GROUP,
			"move task stopped by STOP MOVE TASK GROUP command")
	}
	keyRange, err := qc.GetKeyRange(ctx, taskGroup.KridFrom)
	krFound := true
	if et, ok := err.(*spqrerror.SpqrError); ok && et.ErrorCode == spqrerror.SPQR_KEYRANGE_ERROR {
		krFound = false
		spqrlog.Zero.Debug().Str("key range", taskGroup.KridFrom).Msg("key range already moved")
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

	/* Getting next key range bound can be costly (seq scan) */
	stop, _, err = qc.QDB().CheckMoveTaskGroupStopFlag(ctx, taskGroup.ID)
	if err != nil {
		return nil, fmt.Errorf("failed to check for stop flag: %s", err)
	}

	// TODO create special error type here, use it to stop redistribute/balancer tasks
	if stop {
		spqrlog.Zero.Info().Msg("received stop flag, gracefully stopping move task group")
		return nil, spqrerror.Newf(
			spqrerror.SPQR_STOP_MOVE_TASK_GROUP,
			"move task stopped by STOP MOVE TASK GROUP command")
	}

	boundKR, err := kr.KeyRangeFromBytes(bound, keyRange.ColumnTypes)
	if err != nil {
		return nil, err
	}
	if kr.CmpRangesEqual(boundKR.LowerBound, keyRange.LowerBound, keyRange.ColumnTypes) {
		// move whole key range
		return &tasks.MoveTask{
			ID: uuid.NewString(),
			KridTemp: func() string {
				if taskGroup.TotalKeys > 0 {
					return taskGroup.KridFrom
				}
				return taskGroup.KridTo
			}(),
			State:       tasks.TaskPlanned,
			Bound:       nil,
			TaskGroupID: taskGroup.ID,
		}, nil
	}
	task := &tasks.MoveTask{ID: uuid.NewString(), KridTemp: uuid.NewString(), State: tasks.TaskPlanned, Bound: bound, TaskGroupID: taskGroup.ID}
	if taskGroup.TotalKeys == 0 {
		task.KridTemp = taskGroup.KridTo
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

// executeMoveTaskGroup executes the given MoveTaskGroup.
// All intermediary states of the task group are synced with the QDB for reliability.
//
// Parameters:
//   - ctx (context.Context): The context for QDB operations.
//   - taskGroup (*tasks.MoveTaskGroup): Move tasks to execute.
//
// Returns:
//   - error: An error if any occurred.
func (qc *ClusteredCoordinator) executeMoveTaskGroup(ctx context.Context, taskGroup *tasks.MoveTaskGroup) error {
	if taskGroup == nil {
		return nil
	}
	keyRange, err := qc.GetKeyRange(ctx, taskGroup.KridFrom)
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
	sourceConn, err := datatransfers.GetMasterConnection(ctx, sourceShardConn, "move_task_group_service_conn")
	if err != nil {
		return err
	}
	defer func() {
		if sourceConn != nil {
			_ = sourceConn.Close(ctx)
		}
	}()

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
	host, err := config.GetHostOrHostname(config.CoordinatorConfig().Host)
	if err != nil {
		return err
	}
	addr := net.JoinHostPort(host, config.CoordinatorConfig().GrpcAPIPort)
	if err := qc.db.TryTaskGroupLock(ctx, taskGroup.ID, addr); err != nil {
		return fmt.Errorf("failed to acquire lock on task group \"%s\": %s", taskGroup.ID, err)
	}
	if err := qc.QDB().WriteTaskGroupStatus(ctx, taskGroup.ID, &qdb.TaskGroupStatus{State: string(tasks.TaskGroupRunning), Message: fmt.Sprintf("executed by \"%s\"", addr)}); err != nil {
		spqrlog.Zero.Error().Str("task group ID", taskGroup.ID).Err(err).Msg("failed to write task group status")
		return err
	}

	var delayedError error
	for {
		if taskGroup.CurrentTask == nil {
			if taskGroup.BoundRel == "" {
				break
			}
			if err := sourceConn.Ping(ctx); err != nil {
				sourceConn, err = datatransfers.GetMasterConnection(ctx, sourceShardConn, "move_task_group_service_conn")
				if err != nil {
					return fmt.Errorf("failed to re-setup connection with source shard: %s", err)
				}
				defer func() {
					_ = sourceConn.Close(ctx)
				}()
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
			if task.Bound == nil && taskGroup.KridTo == task.KridTemp {
				if err := qc.RenameKeyRange(ctx, taskGroup.KridFrom, task.KridTemp); err != nil {
					return err
				}
				if config.CoordinatorConfig().EnableICP {
					if err := icp.CheckControlPoint(nil, icp.AfterRenameKeyRangeCP); err != nil {
						spqrlog.Zero.Info().Str("cp", icp.AfterRenameKeyRangeCP).Err(err).Msg("error while checking control point")
					}
				}
				task.State = tasks.TaskSplit
				if err := qc.UpdateMoveTask(ctx, task); err != nil {
					return err
				}
				break
			}
			if task.Bound != nil {
				if err := qc.Split(ctx, &kr.SplitKeyRange{
					Bound:      task.Bound,
					SourceID:   taskGroup.KridFrom,
					KeyRangeID: task.KridTemp,
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
			if config.CoordinatorConfig().EnableICP {
				if err := icp.CheckControlPoint(nil, icp.AfterSplitKeyRangeCP); err != nil {
					spqrlog.Zero.Info().Str("cp", icp.AfterSplitKeyRangeCP).Err(err).Msg("error while checking control point")
				}
			}
			task.State = tasks.TaskSplit
			if err := qc.UpdateMoveTask(ctx, task); err != nil {
				return err
			}
		case tasks.TaskSplit:
			if err := qc.Move(ctx, &kr.MoveKeyRange{KeyRangeID: task.KridTemp, ShardID: taskGroup.ShardToID}); err != nil {
				return err
			}
			if config.CoordinatorConfig().EnableICP {
				if err := icp.CheckControlPoint(nil, icp.AfterMoveCP); err != nil {
					spqrlog.Zero.Info().Str("cp", icp.AfterMoveCP).Err(err).Msg("error while checking control point")
				}
			}
			task.State = tasks.TaskMoved
			if err := qc.UpdateMoveTask(ctx, task); err != nil {
				return err
			}
		case tasks.TaskMoved:
			if task.KridTemp != taskGroup.KridTo {
				if err := qc.Unite(ctx, &kr.UniteKeyRange{BaseKeyRangeID: taskGroup.KridTo, AppendageKeyRangeID: task.KridTemp}); err != nil {
					return err
				}
			}
			if config.CoordinatorConfig().EnableICP {
				if err := icp.CheckControlPoint(nil, icp.AfterUniteKeyRangeCP); err != nil {
					spqrlog.Zero.Info().Str("cp", icp.AfterUniteKeyRangeCP).Err(err).Msg("error while checking control point")
				}
			}
			taskGroup.CurrentTask = nil
			// TODO: get exact key count here
			taskGroup.TotalKeys += taskGroup.BatchSize
			// TODO: wrap in transaction inside etcd
			if err := qc.db.UpdateMoveTaskGroupTotalKeys(ctx, taskGroup.ID, taskGroup.TotalKeys); err != nil {
				return err
			}
			if err := qc.db.DropMoveTask(ctx, task.ID); err != nil {
				return err
			}
		}
	}
	if err := qc.DropMoveTaskGroup(ctx, taskGroup.ID, delayedError != nil); err != nil {
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
func (qc *ClusteredCoordinator) RetryMoveTaskGroup(ctx context.Context, id string, nowait bool) error {
	taskGroup, err := qc.GetMoveTaskGroup(ctx, id)
	if err != nil {
		return spqrerror.Newf(spqrerror.SPQR_TRANSFER_ERROR, "failed to get move task group: %s", err)
	}

	return qc.executeMoveInternal(ctx, taskGroup, nowait, true)
}

// StopMoveTaskGroup gracefully stops the execution of current move task group.
// When current move task is completed, move task group will be finished.
//
// Parameters:
// - ctx (context.Context): The context for the request.
//
// Returns:
// - error: An error if the operation fails, otherwise nil.
func (qc *ClusteredCoordinator) StopMoveTaskGroup(ctx context.Context, id string, immediate bool) error {
	return qc.QDB().AddMoveTaskGroupStopFlag(ctx, id, immediate)
}

func (qc *ClusteredCoordinator) GetMoveTaskGroupBoundsCache(_ context.Context, id string) ([][][]byte, int, error) {
	if groupBoundsInt, ok := qc.bounds.Load(id); ok {
		indMap, _ := qc.index.Load(id)
		groupBounds, ok := groupBoundsInt.([][][]byte)
		if !ok {
			return nil, 0, fmt.Errorf("unexpected bounds type %T for task %s", groupBoundsInt, id)
		}
		ind, ok := indMap.(int)
		if !ok {
			return nil, 0, fmt.Errorf("unexpected index type %T for task %s", indMap, id)
		}
		return groupBounds, ind, nil
	}
	return nil, 0, nil
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
	keyRange, err := qc.GetKeyRange(ctx, req.KeyRangeID)
	if err != nil {
		return spqrerror.Newf(spqrerror.SPQR_INVALID_REQUEST, "key range \"%s\" not found", req.KeyRangeID)
	}

	taskID, err := qc.db.GetKeyRangeRedistributeTaskId(ctx, req.KeyRangeID)
	if err != nil {
		return err
	}
	if taskID != "" {
		task, err := qc.db.GetRedistributeTask(ctx, taskID)
		if err != nil {
			return nil
		}
		if task == nil {
			return fmt.Errorf("failed to redistribute key range \"%s\": it's linked to redistribute task \"%s\" not present in qdb", req.KeyRangeID, taskID)
		}
		taskGroupID, err := qc.db.GetRedistributeTaskTaskGroupId(ctx, task.ID)
		if err != nil {
			return err
		}
		taskGroup, err := qc.GetMoveTaskGroup(ctx, taskGroupID)
		if err != nil {
			return err
		}
		return qc.internalExecRedistributeTaskWrapper(ctx, req, tasks.RedistributeTaskFromDB(task, taskGroup), true)
	}

	spqrlog.Zero.Debug().Msg("process redistribute in clustered coordinator")

	if _, err = qc.GetShard(ctx, req.ShardID); err != nil {
		return spqrerror.Newf(spqrerror.SPQR_TRANSFER_ERROR, "error getting destination shard: %s", err.Error())
	}

	tss, err := qc.ListMoveTaskGroups(ctx)
	if err != nil {
		return err
	}
	for _, ts := range tss {
		if ts.KridFrom == req.KeyRangeID {
			return fmt.Errorf("there is already a move task group \"%s\" for key range \"%s\"", ts.ID, ts.KridFrom)
		}
	}

	if keyRange.ShardID == req.ShardID {
		return nil
	}

	if req.BatchSize <= 0 {
		return spqrerror.Newf(spqrerror.SPQR_INVALID_REQUEST, "incorrect batch size %d", req.BatchSize)
	}

	if req.Check {
		if err := qc.checkKeyRangeMove(ctx, &kr.BatchMoveKeyRange{
			TaskGroupID:    req.TaskGroupID,
			KeyRangeID:     req.KeyRangeID,
			ShardID:        req.ShardID,
			BatchSize:      req.BatchSize,
			Limit:          -1,
			DestKeyRangeID: uuid.NewString(),
			Type:           tasks.SplitRight,
		}); err != nil {
			return err
		}
	}

	return qc.internalExecRedistributeTaskWrapper(ctx, req, &tasks.RedistributeTask{
		ID:          uuid.NewString(),
		TaskGroupID: req.TaskGroupID,
		KeyRangeID:  req.KeyRangeID,
		ShardID:     req.ShardID,
		BatchSize:   req.BatchSize,
		TempKrID:    uuid.NewString(),
		State:       tasks.RedistributeTaskPlanned,
	}, false)
}

func (qc *ClusteredCoordinator) internalExecRedistributeTaskWrapper(ctx context.Context, req *kr.RedistributeKeyRange, task *tasks.RedistributeTask, exists bool) error {
	if !req.Apply {
		return nil
	}
	host, err := config.GetHostOrHostname(config.CoordinatorConfig().Host)
	if err != nil {
		return err
	}
	addr := net.JoinHostPort(host, config.CoordinatorConfig().GrpcAPIPort)

	execCtx, cancel := context.WithCancel(context.TODO())
	if err := qc.db.LockRedistributeTask(execCtx, task.ID, addr); err != nil {
		cancel()
		return fmt.Errorf("failed to execute redistribute task: unable to acquire lock in qdb: %s", err)
	}
	// TODO: update batch size if exists
	if !exists {
		if err := qc.db.CreateRedistributeTask(execCtx, tasks.RedistributeTaskToDB(task)); err != nil {
			cancel()
			return err
		}
	}

	/* Should we wait for the completion? */
	if req.NoWait {
		go func() {
			defer cancel()
			err := qc.executeRedistributeTask(execCtx, task)
			/* We have no way to report error, but at least, log it */
			if err != nil {
				if err2 := qc.db.DropRedistributeTaskLock(ctx, task.ID); err2 != nil {
					spqrlog.Zero.Error().Err(err2).Msg("failed to drop redistribute task lock")
				}
				spqrlog.Zero.Error().Err(err).Msg("failed to execute redistribute")
			}
		}()

		return nil
	}

	defer cancel()

	ch := make(chan error)
	go func() {
		ch <- qc.executeRedistributeTask(execCtx, task)
	}()

	for {
		select {
		case err := <-ch:
			if err2 := qc.db.DropRedistributeTaskLock(ctx, task.ID); err2 != nil {
				spqrlog.Zero.Error().Err(err2).Msg("failed to drop redistribute task lock")
			}
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
			if task.TaskGroup != nil {
				if err := qc.executeMoveTaskGroup(ctx, task.TaskGroup); err != nil {
					return err
				}
				task.State = tasks.RedistributeTaskMoved
				if err := qc.db.UpdateRedistributeTask(ctx, tasks.RedistributeTaskToDB(task)); err != nil {
					return err
				}
				break
			}
			if err := qc.BatchMoveKeyRange(ctx, &kr.BatchMoveKeyRange{
				TaskGroupID:    task.TaskGroupID,
				KeyRangeID:     task.KeyRangeID,
				ShardID:        task.ShardID,
				BatchSize:      task.BatchSize,
				Limit:          -1,
				DestKeyRangeID: task.TempKrID,
				Type:           tasks.SplitRight,
			}, &tasks.MoveTaskGroupIssuer{Type: tasks.IssuerRedistributeTask, ID: task.ID}); err != nil {
				return err
			}
			task.State = tasks.RedistributeTaskMoved
			if err := qc.db.UpdateRedistributeTask(ctx, tasks.RedistributeTaskToDB(task)); err != nil {
				return err
			}
		case tasks.RedistributeTaskMoved:
			if err := qc.RenameKeyRange(ctx, task.TempKrID, task.KeyRangeID); err != nil {
				return err
			}
			return qc.db.DropRedistributeTask(ctx, tasks.RedistributeTaskToDB(task))
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
//   - krID (string): The ID of the key range to be renamed.
//   - krIdNew (string): The new ID for the specified key range.
//
// Returns:
// - error: An error if renaming key range was unsuccessful.
func (qc *ClusteredCoordinator) RenameKeyRange(ctx context.Context, krID, krIDNew string) error {
	if err := qc.Coordinator.RenameKeyRange(ctx, krID, krIDNew); err != nil {
		return err
	}
	return qc.traverseRouters(ctx, func(cc *grpc.ClientConn) error {
		cl := proto.NewKeyRangeServiceClient(cc)
		_, err := cl.RenameKeyRange(ctx, &proto.RenameKeyRangeRequest{KeyRangeId: krID, NewKeyRangeId: krIDNew})

		return err
	})
}

// TODO : unit tests
func (qc *ClusteredCoordinator) SyncRouterMetadata(ctx context.Context, qRouter *topology.Router) error {
	spqrlog.Zero.Debug().
		Str("address", qRouter.Address).
		Msg("qdb coordinator: sync router metadata")

	if err := retry.Do(ctx, retry.WithMaxRetries(4, retry.NewExponential(time.Second)), func(ctx context.Context) error {
		cc, cf, err := qc.getOrCreateRouterConn(qRouter)
		if err != nil {
			return err
		}
		defer cf()

		// Configure shards
		shCl := proto.NewShardServiceClient(cc)
		spqrlog.Zero.Debug().Msg("qdb coordinator: configure shards")
		coordShards, err := qc.ListShards(ctx)
		if err != nil {
			return err
		}
		shardResp, err := shCl.ListShards(ctx, &emptypb.Empty{})
		if err != nil {
			if st, ok := status.FromError(err); ok {
				if st.Code() == codes.Canceled && st.Message() == "grpc: the client connection is closing" {
					return retry.RetryableError(err)
				}
			}
			return err
		}
		routerShards := make([]*topology.DataShard, 0, len(shardResp.Shards))
		for _, sh := range shardResp.Shards {
			shard, err := topology.DataShardFromProto(sh)
			if err != nil {
				return err
			}
			routerShards = append(routerShards, shard)
		}
		needToAdd, needToDelete, needToUpdate := qc.shardsDiff(routerShards, coordShards)

		// Process adds, updates and deletes with continue-on-error so that a
		// single shard failure does not block progress on remaining shards.
		// Connection-level errors (gRPC connection closing) still abort
		// immediately because the transport is broken for all operations.
		var shardErrs []error

		for _, sh := range needToAdd {
			_, err = shCl.AddDataShard(ctx, &proto.AddShardRequest{Shard: topology.DataShardToProto(sh, true)})
			if err != nil {
				if st, ok := status.FromError(err); ok {
					if st.Code() == codes.Canceled && st.Message() == "grpc: the client connection is closing" {
						// Connection broken — abort immediately as retryable.
						return retry.RetryableError(errors.Join(append(shardErrs, fmt.Errorf("shard %s (add): %w", sh.ID, err))...))
					}
				}
				spqrlog.Zero.Warn().Err(err).Str("shard", sh.ID).Msg("shard add failed; continuing to next shard")
				shardErrs = append(shardErrs, fmt.Errorf("shard %s (add): %w", sh.ID, err))
				continue
			}
		}

		for _, sh := range needToUpdate {
			_, err = shCl.AlterShard(ctx, &proto.AlterShardRequest{
				Id:      sh.ID,
				Options: topology.GenericOptionsToProto(sh.Options(), false),
			})
			if err != nil {
				if st, ok := status.FromError(err); ok {
					switch st.Code() {
					case codes.Canceled:
						if st.Message() == "grpc: the client connection is closing" {
							return retry.RetryableError(errors.Join(append(shardErrs, fmt.Errorf("shard %s: %w", sh.ID, err))...))
						}
					case codes.Unimplemented:
						return fmt.Errorf("router does not support UpdateShard RPC; please upgrade all routers before changing shard configuration")
					}
				}
				spqrlog.Zero.Warn().Err(err).Str("shard", sh.ID).Msg("shard update failed; continuing to next shard")
				shardErrs = append(shardErrs, fmt.Errorf("shard %s: %w", sh.ID, err))
				continue
			}
		}
		for _, sh := range needToDelete {
			_, err = shCl.DropShard(ctx, &proto.DropShardRequest{
				Id: sh.ID,
			})
			if err != nil {
				if st, ok := status.FromError(err); ok {
					if st.Code() == codes.Canceled && st.Message() == "grpc: the client connection is closing" {
						// Connection broken — abort immediately as retryable.
						return retry.RetryableError(errors.Join(append(shardErrs, fmt.Errorf("shard %s (delete): %w", sh.ID, err))...))
					}
				}
				spqrlog.Zero.Warn().Err(err).Str("shard", sh.ID).Msg("shard delete failed; continuing to next shard")
				shardErrs = append(shardErrs, fmt.Errorf("shard %s (delete): %w", sh.ID, err))
				continue
			}
		}

		if len(shardErrs) > 0 {
			return errors.Join(shardErrs...)
		}
		return nil
	}); err != nil {
		spqrlog.Zero.Debug().Err(err).Msg("error in shard ops")
		return err
	}

	if err := retry.Do(ctx, retry.WithMaxRetries(4, retry.NewExponential(time.Second)), func(ctx context.Context) error {
		cc, cf, err := qc.getOrCreateRouterConn(qRouter)
		if err != nil {
			return err
		}
		defer cf()

		// Configure distributions
		dsCl := proto.NewDistributionServiceClient(cc)
		spqrlog.Zero.Debug().Msg("qdb coordinator: configure distributions")
		dss, err := qc.ListDistributions(ctx)
		if err != nil {
			return err
		}
		resp, err := dsCl.ListDistributions(ctx, nil)
		if err != nil {
			if st, ok := status.FromError(err); ok {
				if st.Code() == codes.Canceled && st.Message() == "grpc: the client connection is closing" {
					return retry.RetryableError(err)
				}
			}
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
			if st, ok := status.FromError(err); ok {
				if st.Code() == codes.Canceled && st.Message() == "grpc: the client connection is closing" {
					return retry.RetryableError(err)
				}
			}
			return err
		}
		spqrlog.Zero.Debug().Msg("clustered coordinator: distributions dropped successfully")
		gossipCl := proto.NewMetaTransactionGossipServiceClient(cc)
		if len(dss) > 0 {
			distribsToCreate := make([]*proto.Distribution, len(dss))
			for i, ds := range dss {
				distribsToCreate[i] = distributions.DistributionToProto(ds)
			}
			commands := []*proto.MetaTransactionGossipCommand{{
				CreateDistribution: &proto.CreateDistributionGossip{
					Distributions: distribsToCreate,
				},
			}}
			if _, err := gossipCl.ApplyMeta(ctx, &proto.MetaTransactionGossipRequest{Commands: commands}); err != nil {
				if st, ok := status.FromError(err); ok {
					if st.Code() == codes.Canceled && st.Message() == "grpc: the client connection is closing" {
						return retry.RetryableError(err)
					}
				}
				return err
			}
			spqrlog.Zero.Debug().Msg("qdb coordinator: distributions created")
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
			// TODO: We need to group the key ranges into batches. Executing in batches will improve performance.
			for _, kRange := range krsInt {
				commands := []*proto.MetaTransactionGossipCommand{
					{CreateKeyRange: &proto.CreateKeyRangeGossip{
						KeyRangeInfo: kRange.ToProto(),
						ColumnTypes:  ds.ColTypes,
					}},
				}
				resp, err := gossipCl.ApplyMeta(ctx, &proto.MetaTransactionGossipRequest{Commands: commands})
				if err != nil {
					if st, ok := status.FromError(err); ok {
						if st.Code() == codes.Canceled && st.Message() == "grpc: the client connection is closing" {
							return retry.RetryableError(err)
						}
					}
					return err
				}
				spqrlog.Zero.Debug().
					Interface("response", resp).
					Msg("got response while adding key range")
			}
		}

		spqrlog.Zero.Debug().Msg("successfully add all key ranges")
		return nil
	}); err != nil {
		spqrlog.Zero.Debug().Err(err).Msg("error in distribution & key range")
		return err
	}

	if err := retry.Do(ctx, retry.WithMaxRetries(4, retry.NewConstant(time.Second)), func(ctx context.Context) error {
		cc, cf, err := qc.getOrCreateRouterConn(qRouter)
		if err != nil {
			return err
		}

		defer cf()

		storage, err := qc.db.GetTxMetaStorage(ctx)
		if err != nil {
			return err
		}

		s := proto.NewTwoPhaseTxMetaServiceClient(cc)
		// Ignore the error
		_, _ = s.SetTwoPhaseTxMetaStorage(ctx, &proto.SetTwoPhaseTxMetaStorageRequest{Storage: storage})
		return nil
	}); err != nil {
		return err
	}

	if err := retry.Do(ctx, retry.WithMaxRetries(4, retry.NewExponential(time.Second)), func(ctx context.Context) error {
		cc, cf, err := qc.getOrCreateRouterConn(qRouter)
		if err != nil {
			return err
		}
		defer cf()

		host, err := config.GetHostOrHostname(config.CoordinatorConfig().Host)
		if err != nil {
			return err
		}
		rCl := proto.NewTopologyServiceClient(cc)
		if _, err := rCl.UpdateCoordinator(ctx, &proto.UpdateCoordinatorRequest{
			Address: net.JoinHostPort(host, config.CoordinatorConfig().GrpcAPIPort),
		}); err != nil {
			if st, ok := status.FromError(err); ok {
				if st.Code() == codes.Canceled && st.Message() == "grpc: the client connection is closing" {
					return retry.RetryableError(err)
				}
			}
			return err
		}

		if resp, err := rCl.OpenRouter(ctx, nil); err != nil {
			if st, ok := status.FromError(err); ok {
				if st.Code() == codes.Canceled && st.Message() == "grpc: the client connection is closing" {
					return retry.RetryableError(err)
				}
			}
			return err
		} else {
			spqrlog.Zero.Debug().
				Interface("response", resp).
				Msg("open router response")
		}

		return nil
	}); err != nil {
		spqrlog.Zero.Error().Err(err).Msg("error in sync coordinator & open router")
		return err
	}
	return nil
}

// TODO : unit tests
func (qc *ClusteredCoordinator) SyncRouterCoordinatorAddress(ctx context.Context, qRouter *topology.Router) error {
	spqrlog.Zero.Debug().
		Str("address", qRouter.Address).
		Msg("qdb coordinator: sync coordinator address")

	return retry.Do(ctx, retry.WithMaxRetries(4, retry.NewExponential(time.Second)), func(ctx context.Context) error {
		cc, cf, err := qc.getOrCreateRouterConn(qRouter)
		if err != nil {
			return err
		}
		defer cf()

		/* Update current coordinator address. */
		/* Todo: check that router metadata is in sync. */

		host, err := config.GetHostOrHostname(config.CoordinatorConfig().Host)
		if err != nil {
			return err
		}
		rCl := proto.NewTopologyServiceClient(cc)
		if _, err := rCl.UpdateCoordinator(ctx, &proto.UpdateCoordinatorRequest{
			Address: net.JoinHostPort(host, config.CoordinatorConfig().GrpcAPIPort),
		}); err != nil {
			if st, ok := status.FromError(err); ok {
				if st.Code() == codes.Canceled && st.Message() == "grpc: the client connection is closing" {
					return retry.RetryableError(err)
				}
			}
			return err
		}

		if resp, err := rCl.OpenRouter(ctx, nil); err != nil {
			if st, ok := status.FromError(err); ok {
				if st.Code() == codes.Canceled && st.Message() == "grpc: the client connection is closing" {
					return retry.RetryableError(err)
				}
			}
			return err
		} else {
			spqrlog.Zero.Debug().
				Interface("response", resp).
				Msg("open router response")
		}

		return nil
	})
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
		Msg("init client connection")

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

	r := route.NewRoute(nil, nil, nil, time.Duration(0) /* never do healthcheck */)
	params := map[string]string{
		"client_encoding": "UTF8",
		"DateStyle":       "ISO",
	}
	for k, v := range params {
		cl.SetParam(k, v, false)
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

			tts, err := meta.ProcMetadataCommand(ctx, tstmt, qc, ci, cl.Rule(), nil, qc.IsReadOnly())
			if err != nil {
				if err := cli.ReportError(err); err != nil {
					return err
				}
			} else {
				if err := cli.ReplyTTS(tts); err != nil {
					spqrlog.Zero.Error().Err(err).Msg("processing error")
				} else {
					spqrlog.Zero.Debug().Msg("processed OK")
				}
			}
		default:
			return spqrerror.Newf(spqrerror.SPQR_COMPLEX_QUERY, "unsupported msg type %T", msg)
		}
	}
}

func (qc *ClusteredCoordinator) AddDataShard(ctx context.Context, shard *topology.DataShard) error {
	if err := qc.db.AddShard(ctx, topology.DataShardToDB(shard)); err != nil {
		return err
	}

	if err := qc.traverseRouters(ctx, func(cc *grpc.ClientConn) error {
		c := proto.NewShardServiceClient(cc)
		_, err := c.AddDataShard(ctx, &proto.AddShardRequest{
			Shard: topology.DataShardToProto(shard, true),
		})
		return err
	}); err != nil {
		rbCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		if dropErr := qc.db.DropShard(rbCtx, shard.ID); dropErr != nil {
			spqrlog.Zero.Error().Err(dropErr).Str("shard", shard.ID).Msg("failed to roll back shard in qdb")
		}
		if dropErr := qc.traverseRouters(rbCtx, func(cc *grpc.ClientConn) error {
			c := proto.NewShardServiceClient(cc)
			_, rollbackErr := c.DropShard(rbCtx, &proto.DropShardRequest{Id: shard.ID})
			return rollbackErr
		}); dropErr != nil {
			spqrlog.Zero.Error().Err(dropErr).Str("shard", shard.ID).Msg("failed to roll back shard on routers")
		}
		return err
	}

	return nil
}

func (qc *ClusteredCoordinator) AlterShardOptions(ctx context.Context, shardID string, options []topology.GenericOption) error {
	if err := qc.Coordinator.AlterShardOptions(ctx, shardID, options); err != nil {
		return err
	}

	shard, err := qc.GetShard(ctx, shardID)
	if err != nil {
		return err
	}

	return qc.traverseRouters(ctx, func(cc *grpc.ClientConn) error {
		c := proto.NewShardServiceClient(cc)
		_, err := c.AlterShard(ctx, &proto.AlterShardRequest{
			Id:      shardID,
			Options: topology.GenericOptionsToProto(shard.Options(), true),
		})
		if err != nil {
			if st, ok := status.FromError(err); ok && st.Code() == codes.Unimplemented {
				return fmt.Errorf("router does not support AlterShardOptions RPC; please upgrade all routers to a version that supports it")
			}
			return err
		}
		return nil
	})
}

// TODO : unit tests
func (qc *ClusteredCoordinator) DropShard(ctx context.Context, shardID string) error {
	if err := qc.db.DropShard(ctx, shardID); err != nil {
		return err
	}

	return qc.traverseRouters(ctx, func(cc *grpc.ClientConn) error {
		c := proto.NewShardServiceClient(cc)
		_, err := c.DropShard(ctx, &proto.DropShardRequest{
			Id: shardID,
		})
		return err
	})
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

// CreateReferenceRelation creates reference relation in QDB.
// If enabled, CreateReferenceRelation also attempts to set up the relation in spqrguard extension.
// TODO: unit tests
func (qc *ClusteredCoordinator) CreateReferenceRelation(ctx context.Context,
	r *rrelation.ReferenceRelation, entry []*rrelation.AutoIncrementEntry) error {
	if err := qc.Coordinator.CreateReferenceRelation(ctx, r, entry); err != nil {
		return err
	}

	relationFQNs := []*rfqn.RelationFQN{r.RelationName}
	go func() {
		if err := datatransfers.TraverseShards(ctx, datatransfers.SetUpSPQRGuard([]*rfqn.RelationFQN{}, relationFQNs)); err != nil {
			spqrlog.Zero.Err(err).Msg("failed to set up spqrguard")
		}
	}()

	return qc.traverseRouters(ctx, func(cc *grpc.ClientConn) error {
		cl := proto.NewReferenceRelationsServiceClient(cc)
		resp, err := cl.CreateReferenceRelations(ctx,
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

func (qc *ClusteredCoordinator) SyncReferenceRelations(ctx context.Context, relationFQNs []*rfqn.RelationFQN, destShard string) error {
	if err := qc.Coordinator.SyncReferenceRelations(ctx, relationFQNs, destShard); err != nil {
		return err
	}

	return qc.traverseRouters(ctx, func(cc *grpc.ClientConn) error {
		cl := proto.NewReferenceRelationsServiceClient(cc)

		for _, relationFQN := range relationFQNs {

			rel, err := qc.GetReferenceRelation(ctx, relationFQN)

			if err != nil {
				return err
			}

			resp, err := cl.AlterReferenceRelationStorage(ctx,
				&proto.AlterReferenceRelationStorageRequest{
					Relation: rfqn.RelationFQNToProto(relationFQN),
					ShardIds: rel.ShardIDs,
				})
			if err != nil {
				return err
			}

			spqrlog.Zero.Debug().
				Interface("response", resp).
				Strs("shards", rel.ShardIDs).
				Msg("sync reference relation response")
		}

		return nil
	})
}

// AlterReferenceRelationStorage implements meta.EntityMgr.
func (qc *ClusteredCoordinator) AlterReferenceRelationStorageAdvanced(ctx context.Context, relationFQN *rfqn.RelationFQN, shs []string) error {
	rel, err := qc.GetReferenceRelation(ctx, relationFQN)
	if err != nil {
		return err
	}
	shardsExSet := make(map[string]struct{})
	shardsToAdd := make([]string, 0)
	shardsIntersect := make([]string, 0)
	for _, sh := range rel.ShardIDs {
		shardsExSet[sh] = struct{}{}
	}
	for _, sh := range shs {
		if _, ok := shardsExSet[sh]; !ok {
			shardsToAdd = append(shardsToAdd, sh)
		} else if ok {
			shardsIntersect = append(shardsIntersect, sh)
		}
	}

	if len(shardsIntersect) < len(rel.ShardIDs) {
		// We need to drop shards
		if err := qc.db.AlterReferenceRelationStorage(ctx, relationFQN, shardsIntersect); err != nil {
			return fmt.Errorf("failed to alter reference relation storage: failed to remove excess shards in coordinator: %s", err)
		}
		if err := qc.traverseRouters(ctx, func(cc *grpc.ClientConn) error {
			c := proto.NewReferenceRelationsServiceClient(cc)
			_, err := c.AlterReferenceRelationStorage(ctx, &proto.AlterReferenceRelationStorageRequest{
				Relation: rfqn.RelationFQNToProto(relationFQN),
				ShardIds: shardsIntersect,
			})
			return err
		}); err != nil {
			return fmt.Errorf("failed to alter reference relation storage: failed to remove excess shards in routers: %s", err)
		}
	}

	rels := []*rfqn.RelationFQN{relationFQN}
	for _, sh := range shardsToAdd {
		if err := qc.SyncReferenceRelations(ctx, rels, sh); err != nil {
			return fmt.Errorf("failed to alter reference relation storage: failed to sync relation on shard \"%s\": %s", sh, err)
		}
	}
	return nil
}

// TODO: unit tests
func (qc *ClusteredCoordinator) DropReferenceRelation(ctx context.Context, relationFQN *rfqn.RelationFQN) error {
	if err := qc.Coordinator.DropReferenceRelation(ctx, relationFQN); err != nil {
		return err
	}

	return qc.traverseRouters(ctx, func(cc *grpc.ClientConn) error {
		cl := proto.NewReferenceRelationsServiceClient(cc)
		resp, err := cl.DropReferenceRelations(ctx,
			&proto.DropReferenceRelationsRequest{
				Relations: []*proto.QualifiedName{rfqn.RelationFQNToProto(relationFQN)},
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

// CreateDistribution creates distribution in QDB.
// Sending data to routers is done through ExecNoTran or CommTran.
func (qc *ClusteredCoordinator) CreateDistribution(ctx context.Context, ds *distributions.Distribution) ([]qdb.QdbStatement, error) {
	return qc.Coordinator.CreateDistribution(ctx, ds)
}

// DropDistribution deletes distribution from QDB
// TODO: unit tests
func (qc *ClusteredCoordinator) DropDistribution(ctx context.Context, id string) error {
	if err := qc.Coordinator.DropDistribution(ctx, id); err != nil {
		return err
	}

	return qc.traverseRouters(ctx, func(cc *grpc.ClientConn) error {
		cl := proto.NewDistributionServiceClient(cc)
		resp, err := cl.DropDistribution(ctx, &proto.DropDistributionRequest{
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

	relationFQNs := make([]*rfqn.RelationFQN, len(rels))
	for i, rel := range rels {
		relationFQNs[i] = rel.Relation
	}
	go func() {
		if err := datatransfers.TraverseShards(ctx, datatransfers.SetUpSPQRGuard(relationFQNs, []*rfqn.RelationFQN{})); err != nil {
			spqrlog.Zero.Err(err).Msg("failed to set up spqrguard")
		}
	}()

	return qc.traverseRouters(ctx, func(cc *grpc.ClientConn) error {
		cl := proto.NewDistributionServiceClient(cc)
		resp, err := cl.AlterDistributionAttach(ctx, &proto.AlterDistributionAttachRequest{
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
		resp, err := cl.AlterDistributedRelation(ctx, &proto.AlterDistributedRelationRequest{
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
func (qc *ClusteredCoordinator) AlterDistributedRelationSchema(ctx context.Context, id string, relationFQN *rfqn.RelationFQN, schemaName string) error {
	if err := qc.Coordinator.AlterDistributedRelationSchema(ctx, id, relationFQN, schemaName); err != nil {
		return err
	}

	return qc.traverseRouters(ctx, func(cc *grpc.ClientConn) error {
		cl := proto.NewDistributionServiceClient(cc)
		resp, err := cl.AlterDistributedRelationSchema(ctx, &proto.AlterDistributedRelationSchemaRequest{
			Id:           id,
			RelationName: relationFQN.RelationName,
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
func (qc *ClusteredCoordinator) AlterDistributedRelationDistributionKey(ctx context.Context, id string, relationFQN *rfqn.RelationFQN, distributionKey []distributions.DistributionKeyEntry) error {
	if err := qc.Coordinator.AlterDistributedRelationDistributionKey(ctx, id, relationFQN, distributionKey); err != nil {
		return err
	}

	return qc.traverseRouters(ctx, func(cc *grpc.ClientConn) error {
		cl := proto.NewDistributionServiceClient(cc)
		resp, err := cl.AlterDistributedRelationDistributionKey(ctx, &proto.AlterDistributedRelationDistributionKeyRequest{
			Id:              id,
			RelationName:    relationFQN.RelationName,
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
		resp, err := cl.DropSequence(ctx, &proto.DropSequenceRequest{
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

func (qc *ClusteredCoordinator) AlterSequenceDetachRelation(ctx context.Context, rel *rfqn.RelationFQN) error {
	if err := qc.Coordinator.AlterSequenceDetachRelation(ctx, rel); err != nil {
		return err
	}
	return qc.traverseRouters(ctx, func(cc *grpc.ClientConn) error {
		cl := proto.NewDistributionServiceClient(cc)
		_, err := cl.AlterSequenceDetachRelation(ctx, &proto.AlterSequenceDetachRelationRequest{
			RelationName: rfqn.RelationFQNToProto(rel),
		})
		if err != nil {
			return err
		}

		spqrlog.Zero.Debug().
			Msg("alter sequence detach relation response")
		return nil
	})
}

// AlterDistributionDetach detaches relation from distribution
// TODO: unit tests
func (qc *ClusteredCoordinator) AlterDistributionDetach(ctx context.Context, id string, relationFQN *rfqn.RelationFQN) error {
	/* Do what needs to be done in metadata */
	if err := qc.Coordinator.AlterDistributionDetach(ctx, id, relationFQN); err != nil {
		return err
	}

	return qc.traverseRouters(ctx, func(cc *grpc.ClientConn) error {
		cl := proto.NewDistributionServiceClient(cc)
		resp, err := cl.AlterDistributionDetach(ctx, &proto.AlterDistributionDetachRequest{
			Id:       id,
			RelNames: []*proto.QualifiedName{rfqn.RelationFQNToProto(relationFQN)},
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
		if _, ok := mtran.GetGossipRequestType(gossipRequest); !ok {
			return fmt.Errorf("invalid meta transaction request (exec no tran)")
		}
	}
	if err := qc.Coordinator.ExecNoTran(ctx, chunk); err != nil {
		return err
	} else {
		return qc.traverseRouters(ctx,
			gossipMetaChanges(ctx, &proto.MetaTransactionGossipRequest{Commands: chunk.GossipRequests}),
		)
	}
}

func (qc *ClusteredCoordinator) CommitTran(ctx context.Context, transaction *mtran.MetaTransaction) error {
	for _, gossipRequest := range transaction.Operations.GossipRequests {
		if _, ok := mtran.GetGossipRequestType(gossipRequest); !ok {
			return fmt.Errorf("invalid meta transaction request (commit tran)")
		}
	}

	if err := qc.Coordinator.CommitTran(ctx, transaction); err != nil {
		return err
	}

	for _, gossipRequest := range transaction.Operations.GossipRequests {
		reqType, _ := mtran.GetGossipRequestType(gossipRequest)
		switch reqType {
		case mtran.GRCreateKeyRange:
			if err := qc.updateKeyRangeMetaOnShard(ctx, gossipRequest.CreateKeyRange.KeyRangeInfo.ShardId, datatransfers.InsertKeyRangeMeta, gossipRequest.CreateKeyRange.KeyRangeInfo.Krid); err != nil {
				return err
			}
		}
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

func (qc *ClusteredCoordinator) GetTxnBatchSize() uint16 {
	return qc.maxTxnBatch
}

func (qc *ClusteredCoordinator) CreateUniqueIndex(ctx context.Context, dsID string, idx *distributions.UniqueIndex) error {
	if err := qc.Coordinator.CreateUniqueIndex(ctx, dsID, idx); err != nil {
		return err
	}

	req := &proto.CreateUniqueIndexRequest{
		DistributionId: dsID,
		Idx:            distributions.UniqueIndexToProto(idx),
	}
	return qc.traverseRouters(ctx, func(cc *grpc.ClientConn) error {
		cl := proto.NewDistributionServiceClient(cc)
		resp, err := cl.CreateUniqueIndex(ctx, req)
		if err != nil {
			return err
		}

		spqrlog.Zero.Debug().
			Interface("response", resp).
			Msg("create unique index response")
		return nil
	})
}

func (qc *ClusteredCoordinator) DropUniqueIndex(ctx context.Context, idxID string) error {
	if err := qc.Coordinator.DropUniqueIndex(ctx, idxID); err != nil {
		return err
	}

	req := &proto.DropUniqueIndexRequest{
		IdxId: idxID,
	}
	return qc.traverseRouters(ctx, func(cc *grpc.ClientConn) error {
		cl := proto.NewDistributionServiceClient(cc)
		resp, err := cl.DropUniqueIndex(ctx, req)
		if err != nil {
			return err
		}

		spqrlog.Zero.Debug().
			Interface("response", resp).
			Msg("drop unique index response")
		return nil
	})
}

func (qc *ClusteredCoordinator) shardsDiff(routerShards []*topology.DataShard, coordShards []*topology.DataShard) (added []*topology.DataShard, deleted []*topology.DataShard, updated []*topology.DataShard) {
	routerShardsMap := map[string]*topology.DataShard{}
	coordShardsMap := map[string]*topology.DataShard{}

	for _, sh := range routerShards {
		routerShardsMap[sh.ID] = sh
	}
	for _, sh := range coordShards {
		coordShardsMap[sh.ID] = sh
	}

	for id, sh := range coordShardsMap {
		if rSh, exist := routerShardsMap[id]; !exist {
			added = append(added, sh)
		} else if !topology.ShardConfigEqual(rSh, sh) {
			updated = append(updated, sh)
		}
	}
	for id, sh := range routerShardsMap {
		if _, exist := coordShardsMap[id]; !exist {
			deleted = append(deleted, sh)
		}
	}

	return
}

func (qc *ClusteredCoordinator) watchTaskGroups(ctx context.Context) {
	spqrlog.Zero.Debug().Msg("start task groups watch iteration")
	for {
		// TODO check we are still coordinator
		if !qc.acquiredLock {
			time.Sleep(500 * time.Millisecond)
			continue
		}

		tgs, err := qc.ListMoveTaskGroups(ctx)
		if err != nil {
			spqrlog.Zero.Error().Err(err).Msg("watch task groups iteration: failed to list task groups")
			time.Sleep(10 * time.Second)
			continue
		}

		for id := range tgs {
			locked, err := qc.db.CheckTaskGroupLocked(ctx, id)
			if err != nil {
				spqrlog.Zero.Error().Err(err).Str("task group id", id).Msg("watch task groups iteration: failed to check task group lock")
				time.Sleep(10 * time.Second)
				continue
			}
			if !locked {
				status, err := qc.db.GetTaskGroupStatus(ctx, id)
				if err != nil {
					spqrlog.Zero.Error().Err(err).Str("task group id", id).Msg("watch task groups iteration: failed to get task group status")
					continue
				}
				if status == nil {
					continue
				}
				if status.State == string(tasks.TaskGroupRunning) {
					// Executor probably failed, update status
					if err := qc.db.WriteTaskGroupStatus(ctx, id, &qdb.TaskGroupStatus{State: string(tasks.TaskGroupError), Message: "task group lost running"}); err != nil {
						spqrlog.Zero.Error().Err(err).Str("task group id", id).Msg("watch task groups iteration: failed to update task group status")
					}
				}
			}
		}

		time.Sleep(config.ValueOrDefaultDuration(config.CoordinatorConfig().IterationTimeout, defaultWatchRouterTimeout))
	}
}

func getRouterConnRetryPolicy() string {
	return `{
            "methodConfig": [{
                "name": [
					{"service": "spqr.TopologyService"},
					{"service": "spqr.DistributionService"},
					{"service": "spqr.KeyRangeService"},
					{"service": "spqr.MetaTransactionGossipService"},
					{"service": "spqr.OperationService"},
					{"service": "spqr.PoolService"},
					{"service": "spqr.ReferenceRelationsService"},
					{"service": "spqr.RouterService"},
					{"service": "spqr.ShardService"}
				],

                "retryPolicy": {
                    "MaxAttempts": 4,
                    "InitialBackoff": "1s",
                    "MaxBackoff": "20s",
                    "BackoffMultiplier": 2.0,
                    "RetryableStatusCodes": [ "UNAVAILABLE" ]
                }
            }]
        }`
}

func (qc *ClusteredCoordinator) invalidateTaskGroupCache(id string) {
	qc.bounds.Delete(id)
	qc.index.Delete(id)
}

func (qc *ClusteredCoordinator) updateKeyRangeMetaOnShard(ctx context.Context, shardId string, query string, args ...any) error {
	conns, err := config.LoadShardDataCfg(config.CoordinatorConfig().ShardDataCfg)
	if err != nil {
		return err
	}
	shardData, ok := conns.ShardsData[shardId]
	if !ok {
		return spqrerror.New(spqrerror.SPQR_METADATA_CORRUPTION, fmt.Sprintf("could not update key range on shard: shard \"%s\" does not exist in shard data config", shardId))
	}
	conn, err := datatransfers.GetMasterConnection(ctx, shardData, "key_range_meta_update")
	if err != nil {
		return err
	}

	tx, err := conn.Begin(ctx)
	if err != nil {
		return err
	}

	defer func() {
		_ = tx.Rollback(ctx)
	}()

	/* "INSERT INTO spqr_metadata.spqr_local_key_ranges (key_range_id) VALUES ($1) ON CONFLICT DO NOTHING;", keyRangeId */
	if _, err := tx.Exec(ctx, query, args...); err != nil {
		return err
	}

	return tx.Commit(ctx)
}
