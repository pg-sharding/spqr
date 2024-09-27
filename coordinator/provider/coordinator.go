package provider

import (
	"context"
	"crypto/tls"
	"encoding/binary"
	"fmt"
	"github.com/jackc/pgx/v5"
	"math"
	"net"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/pg-sharding/spqr/pkg/models/distributions"

	"github.com/pg-sharding/spqr/pkg/models/spqrerror"

	"github.com/google/uuid"

	"github.com/pg-sharding/spqr/pkg/datatransfers"
	"github.com/pg-sharding/spqr/pkg/meta"
	"github.com/pg-sharding/spqr/pkg/models/topology"
	proto "github.com/pg-sharding/spqr/pkg/protos"
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
	"github.com/pg-sharding/spqr/pkg/models/tasks"
	"github.com/pg-sharding/spqr/pkg/pool"
	routerproto "github.com/pg-sharding/spqr/pkg/protos"
	"github.com/pg-sharding/spqr/qdb"
	psqlclient "github.com/pg-sharding/spqr/router/client"
	"github.com/pg-sharding/spqr/router/port"
	"github.com/pg-sharding/spqr/router/route"
	spqrparser "github.com/pg-sharding/spqr/yacc/console"
)

type grpcConnectionIterator struct {
	*qdbCoordinator
}

// TODO : unit tests
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
		defer cc.Close()

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
func (ci grpcConnectionIterator) ClientPoolForeach(cb func(client client.ClientInfo) error) error {
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
			err = cb(psqlclient.NewNoopClient(client, addr))
			if err != nil {
				return err
			}
		}
		return nil
	})
}

// TODO : implement
// TODO : unit tests
func (ci grpcConnectionIterator) Put(client client.Client) error {
	return spqrerror.New(spqrerror.SPQR_NOT_IMPLEMENTED, "grpcConnectionIterator put not implemented")
}

// TODO : implement
// TODO : unit tests
func (ci grpcConnectionIterator) Pop(id uint) (bool, error) {
	return true, spqrerror.New(spqrerror.SPQR_NOT_IMPLEMENTED, "grpcConnectionIterator pop not implemented")
}

// TODO : implement
// TODO : unit tests
func (ci grpcConnectionIterator) Shutdown() error {
	return spqrerror.New(spqrerror.SPQR_NOT_IMPLEMENTED, "grpcConnectionIterator shutdown not implemented")
}

// TODO : unit tests
func (ci grpcConnectionIterator) ForEach(cb func(sh shard.Shardinfo) error) error {
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
			err = cb(NewCoordShardInfo(conn, addr))
			if err != nil {
				return err
			}
		}
		return nil
	})
}

// TODO : unit tests
func (ci grpcConnectionIterator) ForEachPool(cb func(p pool.Pool) error) error {
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

var _ connectiterator.ConnectIterator = &grpcConnectionIterator{}

func DialRouter(r *topology.Router) (*grpc.ClientConn, error) {
	spqrlog.Zero.Debug().
		Str("router-id", r.ID).
		Msg("dialing router")
	// TODO: add creds
	return grpc.NewClient(r.Address, grpc.WithInsecure()) //nolint:all
}

type CoordinatorClient interface {
	client.Client

	CancelMsg() *pgproto3.CancelRequest
}

type qdbCoordinator struct {
	tlsconfig *tls.Config
	db        qdb.XQDB
}

func (qc *qdbCoordinator) ShareKeyRange(id string) error {
	return qc.db.ShareKeyRange(id)
}

func (qc *qdbCoordinator) QDB() qdb.QDB {
	return qc.db
}

var _ coordinator.Coordinator = &qdbCoordinator{}

// watchRouters traverse routers one check if they are opened
// for clients. If not, initialize metadata and open router
// TODO : unit tests
func (qc *qdbCoordinator) watchRouters(ctx context.Context) {
	for {
		/* check we are still coordinator */
		spqrlog.Zero.Debug().Msg("start routers watch iteration")

		// TODO: lock router
		rtrs, err := qc.db.ListRouters(ctx)
		if err != nil {
			spqrlog.Zero.Error().Err(err).Msg("")
			time.Sleep(time.Second)
			continue
		}

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

				defer cc.Close()

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
					Msg("router watchdog coroutine failed")
			}
		}

		// TODO: configure sleep
		time.Sleep(time.Second)
	}
}

func NewCoordinator(tlsconfig *tls.Config, db qdb.XQDB) (*qdbCoordinator, error) {
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

	return &qdbCoordinator{
		db:        db,
		tlsconfig: tlsconfig,
	}, nil
}

// TODO : unit tests
func (qc *qdbCoordinator) lockCoordinator(ctx context.Context, initialRouter bool) bool {
	updateCoordinator := func() bool {
		if !initialRouter {
			return true
		}
		router := &topology.Router{
			ID:      uuid.NewString(),
			Address: net.JoinHostPort(config.RouterConfig().Host, config.RouterConfig().GrpcApiPort),
			State:   qdb.OPENED,
		}
		if err := qc.RegisterRouter(ctx, router); err != nil {
			spqrlog.Zero.Error().Err(err).Msg("register router when locking coordinator")
		}

		if err := qc.UpdateCoordinator(ctx, net.JoinHostPort(config.CoordinatorConfig().Host, config.CoordinatorConfig().GrpcApiPort)); err != nil {
			return false
		}
		return true
	}

	if qc.db.TryCoordinatorLock(context.TODO()) != nil {
		for {
			select {
			case <-ctx.Done():
				return false
			case <-time.After(time.Second):
				if err := qc.db.TryCoordinatorLock(context.TODO()); err == nil {
					return updateCoordinator()
				} else {
					spqrlog.Zero.Error().Err(err).Msg("qdb already taken, waiting for connection")
				}
			}
		}
	}

	return updateCoordinator()
}

// RunCoordinator side effect: it runs an asynchronous goroutine
// that checks the availability of the SPQR router
// TODO : unit tests
func (qc *qdbCoordinator) RunCoordinator(ctx context.Context, initialRouter bool) {
	if !qc.lockCoordinator(ctx, initialRouter) {
		return
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
func (qc *qdbCoordinator) traverseRouters(ctx context.Context, cb func(cc *grpc.ClientConn) error) error {
	spqrlog.Zero.Debug().Msg("qdb coordinator traverse")

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
			defer cc.Close()

			if err := cb(cc); err != nil {
				spqrlog.Zero.Debug().Err(err).Str("router id", rtr.ID).Msg("traverse routers")
				return err
			}

			return nil
		}(); err != nil {
			return err
		}
	}

	return nil
}

// TODO : unit tests
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
			State:   v.State,
		})
	}

	return retRouters, nil
}

// TODO : unit tests
func (qc *qdbCoordinator) AddRouter(ctx context.Context, router *topology.Router) error {
	return qc.db.AddRouter(ctx, topology.RouterToDB(router))
}

// TODO : unit tests
func (qc *qdbCoordinator) CreateKeyRange(ctx context.Context, keyRange *kr.KeyRange) error {
	// add key range to metadb
	spqrlog.Zero.Debug().
		Bytes("lower-bound", keyRange.Raw()[0]).
		Str("shard-id", keyRange.ShardID).
		Str("key-range-id", keyRange.ID).
		Msg("add key range")

	err := ops.CreateKeyRangeWithChecks(ctx, qc.db, keyRange)
	if err != nil {
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

// GetKeyRange gets key range by id
// TODO unit tests
func (qc *qdbCoordinator) GetKeyRange(ctx context.Context, krId string) (*kr.KeyRange, error) {
	krDb, err := qc.db.GetKeyRange(ctx, krId)
	if err != nil {
		return nil, err
	}
	ds, err := qc.db.GetDistribution(ctx, krDb.DistributionId)
	if err != nil {
		return nil, err
	}
	return kr.KeyRangeFromDB(krDb, ds.ColTypes), nil
}

// TODO : unit tests
func (qc *qdbCoordinator) ListKeyRanges(ctx context.Context, distribution string) ([]*kr.KeyRange, error) {
	keyRanges, err := qc.db.ListKeyRanges(ctx, distribution)
	if err != nil {
		return nil, err
	}

	keyr := make([]*kr.KeyRange, 0, len(keyRanges))
	for _, keyRange := range keyRanges {
		ds, err := qc.db.GetDistribution(ctx, keyRange.DistributionId)
		if err != nil {
			return nil, err
		}
		keyr = append(keyr, kr.KeyRangeFromDB(keyRange, ds.ColTypes))
	}

	return keyr, nil
}

// TODO : unit tests
func (qc *qdbCoordinator) ListAllKeyRanges(ctx context.Context) ([]*kr.KeyRange, error) {
	keyRanges, err := qc.db.ListAllKeyRanges(ctx)
	if err != nil {
		return nil, err
	}

	keyr := make([]*kr.KeyRange, 0, len(keyRanges))
	for _, keyRange := range keyRanges {
		ds, err := qc.db.GetDistribution(ctx, keyRange.DistributionId)
		if err != nil {
			return nil, err
		}
		keyr = append(keyr, kr.KeyRangeFromDB(keyRange, ds.ColTypes))
	}

	return keyr, nil
}

// TODO : unit tests
func (qc *qdbCoordinator) MoveKeyRange(ctx context.Context, keyRange *kr.KeyRange) error {
	return ops.ModifyKeyRangeWithChecks(ctx, qc.db, keyRange)
}

// TODO : unit tests
func (qc *qdbCoordinator) LockKeyRange(ctx context.Context, keyRangeID string) (*kr.KeyRange, error) {

	keyRangeDB, err := qc.QDB().LockKeyRange(ctx, keyRangeID)
	if err != nil {
		return nil, err
	}
	ds, err := qc.QDB().GetDistribution(ctx, keyRangeDB.DistributionId)
	if err != nil {
		_ = qc.QDB().UnlockKeyRange(ctx, keyRangeID)
		return nil, err
	}

	keyRange := kr.KeyRangeFromDB(keyRangeDB, ds.ColTypes)

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
func (qc *qdbCoordinator) UnlockKeyRange(ctx context.Context, keyRangeID string) error {
	if err := qc.db.UnlockKeyRange(ctx, keyRangeID); err != nil {
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
func (qc *qdbCoordinator) Split(ctx context.Context, req *kr.SplitKeyRange) error {
	spqrlog.Zero.Debug().
		Str("krid", req.Krid).
		Interface("bound", req.Bound).
		Str("source-id", req.SourceID).
		Msg("split request is")

	if _, err := qc.db.GetKeyRange(ctx, req.Krid); err == nil {
		return spqrerror.Newf(spqrerror.SPQR_KEYRANGE_ERROR, "key range %v already present in qdb", req.Krid)
	}

	krOldDB, err := qc.db.LockKeyRange(ctx, req.SourceID)
	if err != nil {
		return err
	}

	defer func() {
		if err := qc.db.UnlockKeyRange(ctx, req.SourceID); err != nil {
			spqrlog.Zero.Error().Err(err).Msg("")
		}
	}()

	ds, err := qc.QDB().GetDistribution(ctx, krOldDB.DistributionId)

	if err != nil {
		return err
	}

	krOld := kr.KeyRangeFromDB(krOldDB, ds.ColTypes)

	eph := kr.KeyRangeFromBytes(req.Bound, ds.ColTypes)

	if kr.CmpRangesEqual(krOld.LowerBound, eph.LowerBound, ds.ColTypes) {
		return spqrerror.New(spqrerror.SPQR_KEYRANGE_ERROR, "failed to split because bound equals lower of the key range")
	}

	if kr.CmpRangesLess(eph.LowerBound, krOld.LowerBound, ds.ColTypes) {
		return spqrerror.New(spqrerror.SPQR_KEYRANGE_ERROR, "failed to split because bound is out of key range")
	}

	krs, err := qc.ListKeyRanges(ctx, ds.ID)
	if err != nil {
		return err
	}
	for _, kRange := range krs {
		if kr.CmpRangesLess(krOld.LowerBound, kRange.LowerBound, ds.ColTypes) && kr.CmpRangesLessEqual(kRange.LowerBound, eph.LowerBound, ds.ColTypes) {
			return spqrerror.Newf(spqrerror.SPQR_KEYRANGE_ERROR, "failed to split because bound intersects with \"%s\" key range", kRange.ID)
		}
	}

	krNew := kr.KeyRangeFromDB(
		&qdb.KeyRange{
			// fix multidim case
			LowerBound: func() [][]byte {
				if req.SplitLeft {
					return krOld.Raw()
				}
				return req.Bound
			}(),
			KeyRangeID:     req.Krid,
			ShardID:        krOld.ShardID,
			DistributionId: krOld.Distribution,
		},
		ds.ColTypes,
	)

	spqrlog.Zero.Debug().
		Bytes("lower-bound", krNew.Raw()[0]).
		Str("shard-id", krNew.ShardID).
		Str("id", krNew.ID).
		Msg("new key range")

	if req.SplitLeft {
		krOld.LowerBound = kr.KeyRangeFromBytes(req.Bound, ds.ColTypes).LowerBound
	}

	if err := ops.ModifyKeyRangeWithChecks(ctx, qc.db, krOld); err != nil {
		return err
	}

	if err := ops.CreateKeyRangeWithChecks(ctx, qc.db, krNew); err != nil {
		return spqrerror.Newf(spqrerror.SPQR_KEYRANGE_ERROR, "failed to add a new key range: %s", err.Error())
	}

	if err := qc.traverseRouters(ctx, func(cc *grpc.ClientConn) error {
		cl := routerproto.NewKeyRangeServiceClient(cc)
		resp, err := cl.SplitKeyRange(ctx, &routerproto.SplitKeyRangeRequest{
			Bound:    req.Bound[0], // fix multidim case
			SourceId: req.SourceID,
			NewId:    krNew.ID,
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
func (qc *qdbCoordinator) DropKeyRangeAll(ctx context.Context) error {
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

	return qc.db.DropKeyRangeAll(ctx)
}

// TODO : unit tests
func (qc *qdbCoordinator) DropKeyRange(ctx context.Context, id string) error {
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
	return qc.db.DropKeyRange(ctx, id)
}

// TODO : unit tests
func (qc *qdbCoordinator) Unite(ctx context.Context, uniteKeyRange *kr.UniteKeyRange) error {
	krBaseDb, err := qc.db.LockKeyRange(ctx, uniteKeyRange.BaseKeyRangeId)
	if err != nil {
		return err
	}

	defer func() {
		if err := qc.db.UnlockKeyRange(ctx, uniteKeyRange.BaseKeyRangeId); err != nil {
			spqrlog.Zero.Error().Err(err).Msg("")
		}
	}()

	krAppendageDb, err := qc.db.LockKeyRange(ctx, uniteKeyRange.AppendageKeyRangeId)
	if err != nil {
		return err
	}

	defer func() {
		if err := qc.db.UnlockKeyRange(ctx, uniteKeyRange.AppendageKeyRangeId); err != nil {
			spqrlog.Zero.Error().Err(err).Msg("")
		}
	}()

	ds, err := qc.db.GetDistribution(ctx, krBaseDb.DistributionId)
	if err != nil {
		return err
	}

	krBase := kr.KeyRangeFromDB(krBaseDb, ds.ColTypes)
	krAppendage := kr.KeyRangeFromDB(krAppendageDb, ds.ColTypes)

	if krBase.ShardID != krAppendage.ShardID {
		return spqrerror.New(spqrerror.SPQR_KEYRANGE_ERROR, "failed to unite key ranges routing different shards")
	}
	if krBase.Distribution != krAppendage.Distribution {
		return spqrerror.New(spqrerror.SPQR_KEYRANGE_ERROR, "failed to unite key ranges of different distributions")
	}
	// TODO: check all types when composite keys are supported
	krLeft, krRight := krBase, krAppendage
	if kr.CmpRangesLess(krRight.LowerBound, krLeft.LowerBound, ds.ColTypes) {
		krLeft, krRight = krRight, krLeft
	}

	krs, err := qc.ListKeyRanges(ctx, ds.ID)
	if err != nil {
		return err
	}
	for _, kRange := range krs {
		if kRange.ID != krLeft.ID &&
			kRange.ID != krRight.ID &&
			kr.CmpRangesLessEqual(krLeft.LowerBound, kRange.LowerBound, ds.ColTypes) &&
			kr.CmpRangesLessEqual(kRange.LowerBound, krRight.LowerBound, ds.ColTypes) {
			return spqrerror.New(spqrerror.SPQR_KEYRANGE_ERROR, "failed to unite non-adjacent key ranges")
		}
	}

	if err := qc.db.DropKeyRange(ctx, krAppendage.ID); err != nil {
		return spqrerror.Newf(spqrerror.SPQR_KEYRANGE_ERROR, "failed to drop an old key range: %s", err.Error())
	}

	if krLeft.ID != krBase.ID {
		krBase.LowerBound = krAppendage.LowerBound
	}

	if err := ops.ModifyKeyRangeWithChecks(ctx, qc.db, krBase); err != nil {
		return spqrerror.Newf(spqrerror.SPQR_KEYRANGE_ERROR, "failed to update a new key range: %s", err.Error())
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
func (qc *qdbCoordinator) RecordKeyRangeMove(ctx context.Context, m *qdb.MoveKeyRange) (string, error) {
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

func (qc *qdbCoordinator) GetKeyRangeMove(ctx context.Context, krId string) (*qdb.MoveKeyRange, error) {
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
func (qc *qdbCoordinator) Move(ctx context.Context, req *kr.MoveKeyRange) error {
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
			MoveId:     uuid.New().String(),
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
			if err = qc.db.UpdateKeyRangeMoveStatus(ctx, move.MoveId, qdb.MoveKeyRangeComplete); err != nil {
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

// TODO check naming when moving the whole range
func (qc *qdbCoordinator) BatchMoveKeyRange(ctx context.Context, req *kr.BatchMoveKeyRange) error {
	keyRange, err := qc.GetKeyRange(ctx, req.KrId)
	if err != nil {
		return err
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
	// TODO may prefer replica connection
	conns, err := config.LoadShardDataCfg(config.CoordinatorConfig().ShardDataCfg)
	if err != nil {
		return err
	}
	if _, ok := conns.ShardsData[keyRange.ShardID]; !ok {
		return spqrerror.New(spqrerror.SPQR_METADATA_CORRUPTION, fmt.Sprintf("shard of key range '%s' does not exist in shard data config", keyRange.ID))
	}
	sourceShardConn := conns.ShardsData[keyRange.ShardID]
	sourceMasterConn, err := sourceShardConn.GetMasterConnection(ctx)
	if err != nil {
		return err
	}

	totalCount, relCount, err := qc.getKeyStats(ctx, sourceMasterConn, ds.Relations, keyRange, nextBound)
	biggestRelName, coeff := qc.getBiggestRelation(relCount, totalCount)
	biggestRel := ds.Relations[biggestRelName]
	tasks, err := qc.getMoveTasks(ctx, sourceMasterConn, req, biggestRel, kr.GetKRCondition(biggestRel, keyRange, nextBound, ""), coeff, ds)
	if err != nil {
		return err
	}

	execCtx := context.TODO()
	ch := make(chan error)
	go func() {
		ch <- qc.executeMoveTasks(execCtx, tasks)
	}()

	for {
		select {
		case <-ctx.Done():
			return spqrerror.NewByCode(spqrerror.SPQR_TRANSFER_ERROR)
		case err := <-ch:
			return err
		}
	}
}

func (*qdbCoordinator) getKeyStats(
	ctx context.Context,
	conn *pgx.Conn,
	relations map[string]*distributions.DistributedRelation,
	keyRange *kr.KeyRange,
	nextBound kr.KeyRangeBound,
) (totalCount int64, relationCount map[string]int64, err error) {
	relationCount = make(map[string]int64)
	for _, rel := range relations {
		cond := kr.GetKRCondition(rel, keyRange, nextBound, "")
		query := fmt.Sprintf(`
				SELECT count(*) 
				FROM %s as t
				WHERE %s;
`, rel.Name, cond)
		row := conn.QueryRow(ctx, query)
		var count int64
		if err = row.Scan(&count); err != nil {
			return 0, nil, err
		}
		relationCount[rel.Name] = count
		totalCount += count
	}
	return
}

func (*qdbCoordinator) getBiggestRelation(relCount map[string]int64, totalCount int64) (string, float64) {
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

// TODO possible split by itself with big limit
func (qc *qdbCoordinator) getMoveTasks(ctx context.Context, conn *pgx.Conn, req *kr.BatchMoveKeyRange, rel *distributions.DistributedRelation, condition string, coeff float64, ds *distributions.Distribution) (*tasks.MoveTaskGroup, error) {
	taskList := make([]*tasks.MoveTask, 0)
	step := int64(float64(req.BatchSize)*coeff + 1)

	limit := func() int64 {
		switch l := req.Limit.(type) {
		case kr.RedistributeAllKeys:
			return math.MaxInt64
		case kr.RedistributeKeyAmount:
			return l.Amount
		default:
			panic(fmt.Sprintf("unexpected RedistributeLimit type %T", l))
		}
	}()

	columns := strings.Join(rel.GetDistributionKeyColumns(), ", ")
	query := fmt.Sprintf(`
WITH 
sub as (
    SELECT %s, row_number() OVER(ORDER BY i %s) as row_n
    FROM %s as t
    WHERE %s
	LIMIT %d
),
constants AS (
    SELECT %d as row_count, %d as batch_size
),
max_row AS (
    SELECT count(1) as row_n
    FROM sub
)
SELECT sub.*
FROM sub JOIN max_row ON true JOIN constants ON true
WHERE (sub.row_n %% constants.batch_size = 0 AND sub.row_n < constants.row_count)
   OR (sub.row_n = constants.row_count)
   OR (max_row.row_n < constants.row_count AND sub.row_n = max_row.row_n);
`,
		columns,
		func() string {
			switch req.Type {
			case tasks.SplitLeft:
				return "ASC"
			case tasks.SplitRight:
				fallthrough
			default:
				return "DESC"
			}
		}(),
		rel.Name,
		condition,
		limit,
		limit,
		step,
	)
	rows, err := conn.Query(ctx, query)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		values := make([]string, len(columns)+1)
		if err = rows.Scan(&values); err != nil {
			return nil, err
		}

		bound := make([][]byte, len(columns))
		for i, t := range ds.ColTypes {
			switch t {
			case qdb.ColumnTypeVarcharDeprecated:
				fallthrough
			case qdb.ColumnTypeVarchar:
				bound[i] = []byte(values[i])
			case qdb.ColumnTypeUinteger:
				number, err := strconv.ParseUint(values[i], 10, 64)
				if err != nil {
					return nil, err
				}
				bound[i] = make([]byte, 8)
				binary.PutUvarint(bound[i], number)
			case qdb.ColumnTypeVarcharHashed:
				// or is it?
				fallthrough
			case qdb.ColumnTypeInteger:
				number, err := strconv.ParseInt(values[i], 10, 64)
				if err != nil {
					return nil, err
				}
				bound[i] = make([]byte, 8)
				binary.PutVarint(bound[i], number)
			}
		}
		taskList = append(taskList, &tasks.MoveTask{KrIdTemp: uuid.NewString(), State: tasks.TaskPlanned, Bound: bound})
	}
	taskList[0].KrIdTemp = req.DestKrId

	_, moveWhole := req.Limit.(kr.RedistributeAllKeys)

	// Avoid splitting key range by its own bound when moving th whole range
	if moveWhole {
		taskList[len(taskList)-1] = &tasks.MoveTask{KrIdTemp: req.KrId, Bound: nil, State: tasks.TaskSplit}
	}

	return &tasks.MoveTaskGroup{
		Tasks:     taskList,
		KrIdFrom:  req.KrId,
		KrIdTo:    req.DestKrId,
		ShardToId: req.ShardId,
		Type:      req.Type,
	}, nil
}

func (qc *qdbCoordinator) getNextKeyRange(ctx context.Context, keyRange *kr.KeyRange) (*kr.KeyRange, error) {
	krs, err := qc.ListKeyRanges(ctx, keyRange.Distribution)
	if err != nil {
		return nil, err
	}

	ind := slices.IndexFunc(krs, func(other *kr.KeyRange) bool {
		return other.ID == keyRange.ID
	})
	if ind < len(krs)-1 {
		return krs[ind+1], nil
	}
	return nil, nil
}

func (qc *qdbCoordinator) executeMoveTasks(ctx context.Context, taskGroup *tasks.MoveTaskGroup) error {
	for len(taskGroup.Tasks) != 0 {
		task := taskGroup.Tasks[0]
		switch task.State {
		case tasks.TaskPlanned:
			if err := qc.Split(ctx, &kr.SplitKeyRange{
				Bound:    task.Bound,
				SourceID: taskGroup.KrIdTo,
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
			if err := qc.WriteTaskGroup(ctx, taskGroup); err != nil {
				return err
			}
		case tasks.TaskSplit:
			if err := qc.Move(ctx, &kr.MoveKeyRange{Krid: task.KrIdTemp, ShardId: taskGroup.ShardToId}); err != nil {
				return err
			}
			task.State = tasks.TaskMoved
			if err := qc.WriteTaskGroup(ctx, taskGroup); err != nil {
				return err
			}
		case tasks.TaskMoved:
			if task.KrIdTemp != taskGroup.KrIdTo {
				if err := qc.Unite(ctx, &kr.UniteKeyRange{BaseKeyRangeId: taskGroup.KrIdTo, AppendageKeyRangeId: task.KrIdTemp}); err != nil {
					return err
				}
			}
			taskGroup.Tasks = taskGroup.Tasks[1:]
			if err := qc.WriteTaskGroup(ctx, taskGroup); err != nil {
				return err
			}
		}
	}
	return qc.RemoveTaskGroup(ctx)
}

// RedistributeKeyRange moves the whole key range to another shard in batches
// TODO: persistent task type
func (qc *qdbCoordinator) RedistributeKeyRange(ctx context.Context, req *kr.RedistributeKeyRange) error {
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

func (qc *qdbCoordinator) executeRedistributeTask(ctx context.Context, task *tasks.RedistributeTask) error {
	for {
		switch task.State {
		case tasks.RedistributeTaskPlanned:
			if err := qc.BatchMoveKeyRange(ctx, &kr.BatchMoveKeyRange{
				KrId:      task.KrId,
				ShardId:   task.ShardId,
				BatchSize: task.BatchSize,
				Limit:     kr.RedistributeAllKeys{},
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
			if err := qc.renameKeyRange(ctx, task.TempKrId, task.KrId); err != nil {
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

// TODO: implement
func (qc *qdbCoordinator) renameKeyRange(ctx context.Context, krId, krIdNew string) error {
	panic("not implemented")
}

// TODO : unit tests
func (qc *qdbCoordinator) SyncRouterMetadata(ctx context.Context, qRouter *topology.Router) error {
	spqrlog.Zero.Debug().
		Str("address", qRouter.Address).
		Msg("qdb coordinator: sync router metadata")

	cc, err := DialRouter(qRouter)
	if err != nil {
		return err
	}
	defer cc.Close()

	// Configure distributions
	dsCl := routerproto.NewDistributionServiceClient(cc)
	spqrlog.Zero.Debug().Msg("qdb coordinator: configure distributions")
	dss, err := qc.db.ListDistributions(ctx)
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
				res[i] = distributions.DistributionToProto(distributions.DistributionFromDB(ds))
			}
			return res
		}(),
	}); err != nil {
		return err
	}

	// Configure key ranges.
	krClient := routerproto.NewKeyRangeServiceClient(cc)
	spqrlog.Zero.Debug().Msg("qdb coordinator: configure key ranges")
	keyRanges, err := qc.db.ListAllKeyRanges(ctx)
	if err != nil {
		return err
	}
	if _, err = krClient.DropAllKeyRanges(ctx, nil); err != nil {
		return err
	}

	for _, keyRange := range keyRanges {
		ds, err := qc.db.GetDistribution(ctx, keyRange.DistributionId)
		if err != nil {
			return err
		}
		resp, err := krClient.CreateKeyRange(ctx, &routerproto.CreateKeyRangeRequest{
			KeyRangeInfo: kr.KeyRangeFromDB(keyRange, ds.ColTypes).ToProto(),
		})

		if err != nil {
			return err
		}

		spqrlog.Zero.Debug().
			Interface("response", resp).
			Msg("got response while adding key range")
	}
	spqrlog.Zero.Debug().Msg("successfully add all key ranges")

	rCl := routerproto.NewTopologyServiceClient(cc)
	if _, err := rCl.UpdateCoordinator(ctx, &routerproto.UpdateCoordinatorRequest{
		Address: net.JoinHostPort(config.CoordinatorConfig().Host, config.CoordinatorConfig().GrpcApiPort),
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
func (qc *qdbCoordinator) SyncRouterCoordinatorAddress(ctx context.Context, qRouter *topology.Router) error {
	spqrlog.Zero.Debug().
		Str("address", qRouter.Address).
		Msg("qdb coordinator: sync router metadata")

	cc, err := DialRouter(qRouter)
	if err != nil {
		return err
	}
	defer cc.Close()

	/* Update current coordinator address. */
	/* Todo: check that router metadata is in sync. */

	rCl := routerproto.NewTopologyServiceClient(cc)
	if _, err := rCl.UpdateCoordinator(ctx, &routerproto.UpdateCoordinatorRequest{
		Address: net.JoinHostPort(config.CoordinatorConfig().Host, config.CoordinatorConfig().GrpcApiPort),
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
func (qc *qdbCoordinator) RegisterRouter(ctx context.Context, r *topology.Router) error {
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
	defer conn.Close()
	cl := routerproto.NewTopologyServiceClient(conn)
	_, err = cl.GetRouterStatus(ctx, nil)
	if err != nil {
		return spqrerror.Newf(spqrerror.SPQR_CONNECTION_ERROR, "failed to ping router: %s", err)
	}

	return qc.db.AddRouter(ctx, qdb.NewRouter(r.Address, r.ID, qdb.OPENED))
}

// TODO : unit tests
func (qc *qdbCoordinator) UnregisterRouter(ctx context.Context, rID string) error {
	spqrlog.Zero.Debug().
		Str("router", rID).
		Msg("unregister router")
	return qc.db.DeleteRouter(ctx, rID)
}

func (qc *qdbCoordinator) GetTaskGroup(ctx context.Context) (*tasks.MoveTaskGroup, error) {
	group, err := qc.db.GetTaskGroup(ctx)
	if err != nil {
		return nil, err
	}
	return tasks.TaskGroupFromDb(group), nil
}

func (qc *qdbCoordinator) WriteTaskGroup(ctx context.Context, taskGroup *tasks.MoveTaskGroup) error {
	return qc.db.WriteTaskGroup(ctx, tasks.TaskGroupToDb(taskGroup))
}

func (qc *qdbCoordinator) RemoveTaskGroup(ctx context.Context) error {
	return qc.db.RemoveTaskGroup(ctx)
}

// TODO : unit tests
func (qc *qdbCoordinator) PrepareClient(nconn net.Conn, pt port.RouterPortType) (CoordinatorClient, error) {
	cl := psqlclient.NewPsqlClient(nconn, pt, "", false, "")

	tlsconfig := qc.tlsconfig
	if pt == port.UnixSocketPortType {
		tlsconfig = nil
	}
	if err := cl.Init(tlsconfig); err != nil {
		return nil, err
	}

	if cl.CancelMsg() != nil {
		return cl, nil
	}

	spqrlog.Zero.Info().
		Str("user", cl.Usr()).
		Str("db", cl.DB()).
		Msg("initialized client connection")

	var authRule *config.AuthCfg
	if config.CoordinatorConfig().Auth != nil {
		authRule = config.CoordinatorConfig().Auth
	} else {
		spqrlog.Zero.Warn().Msg("ATTENTION! Skipping auth checking!")
		authRule = &config.AuthCfg{
			Method: config.AuthOK,
		}
	}

	if err := cl.AssignRule(&config.FrontendRule{
		AuthRule: authRule,
	}); err != nil {
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
func (qc *qdbCoordinator) ProcClient(ctx context.Context, nconn net.Conn, pt port.RouterPortType) error {
	cl, err := qc.PrepareClient(nconn, pt)
	if err != nil {
		spqrlog.Zero.Error().Err(err).Msg("")
		return err
	}

	if cl.CancelMsg() != nil {
		// TODO: cancel client here
		return nil
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

			if err := meta.Proc(ctx, tstmt, qc, ci, cli, nil); err != nil {
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
func (qc *qdbCoordinator) AddDataShard(ctx context.Context, shard *datashards.DataShard) error {
	return qc.db.AddShard(ctx, qdb.NewShard(shard.ID, shard.Cfg.Hosts))
}

func (qc *qdbCoordinator) AddWorldShard(_ context.Context, _ *datashards.DataShard) error {
	panic("implement me")
}

func (qc *qdbCoordinator) DropShard(ctx context.Context, shardId string) error {
	return qc.db.DropShard(ctx, shardId)
}

// TODO : unit tests
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

// TODO : unit tests
func (qc *qdbCoordinator) UpdateCoordinator(ctx context.Context, address string) error {
	return qc.traverseRouters(ctx, func(cc *grpc.ClientConn) error {
		c := proto.NewTopologyServiceClient(cc)
		spqrlog.Zero.Debug().Str("address", address).Msg("updating coordinator address")
		_, err := c.UpdateCoordinator(ctx, &routerproto.UpdateCoordinatorRequest{
			Address: address,
		})
		return err
	})
}

// TODO : unit tests
func (qc *qdbCoordinator) GetCoordinator(ctx context.Context) (string, error) {
	addr, err := qc.db.GetCoordinator(ctx)

	spqrlog.Zero.Debug().Str("address", addr).Msg("resp qdb coordinator: get coordinator")
	return addr, err
}

// ListDistributions returns all distributions from QDB
// TODO: unit tests
func (qc *qdbCoordinator) ListDistributions(ctx context.Context) ([]*distributions.Distribution, error) {
	distrs, err := qc.db.ListDistributions(ctx)
	if err != nil {
		return nil, err
	}
	res := make([]*distributions.Distribution, 0)
	for _, ds := range distrs {
		res = append(res, distributions.DistributionFromDB(ds))
	}
	return res, nil
}

// CreateDistribution creates distribution in QDB
// TODO: unit tests
func (qc *qdbCoordinator) CreateDistribution(ctx context.Context, ds *distributions.Distribution) error {
	if err := qc.db.CreateDistribution(ctx, distributions.DistributionToDB(ds)); err != nil {
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
func (qc *qdbCoordinator) DropDistribution(ctx context.Context, id string) error {
	if err := qc.db.DropDistribution(ctx, id); err != nil {
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

// GetDistribution retrieves info about distribution from QDB
// TODO: unit tests
func (qc *qdbCoordinator) GetDistribution(ctx context.Context, id string) (*distributions.Distribution, error) {
	ds, err := qc.db.GetDistribution(ctx, id)
	if err != nil {
		return nil, err
	}
	return distributions.DistributionFromDB(ds), nil
}

// GetRelationDistribution retrieves info about distribution attached to relation from QDB
// TODO: unit tests
func (qc *qdbCoordinator) GetRelationDistribution(ctx context.Context, relName string) (*distributions.Distribution, error) {
	ds, err := qc.db.GetRelationDistribution(ctx, relName)
	if err != nil {
		return nil, err
	}
	return distributions.DistributionFromDB(ds), nil
}

// AlterDistributionAttach attaches relation to distribution
// TODO: unit tests
func (qc *qdbCoordinator) AlterDistributionAttach(ctx context.Context, id string, rels []*distributions.DistributedRelation) error {
	if err := qc.db.AlterDistributionAttach(ctx, id, func() []*qdb.DistributedRelation {
		qdbRels := make([]*qdb.DistributedRelation, len(rels))
		for i, rel := range rels {
			qdbRels[i] = distributions.DistributedRelationToDB(rel)
		}
		return qdbRels
	}()); err != nil {
		return err
	}

	return qc.traverseRouters(ctx, func(cc *grpc.ClientConn) error {
		cl := routerproto.NewDistributionServiceClient(cc)
		resp, err := cl.AlterDistributionAttach(context.TODO(), &routerproto.AlterDistributionAttachRequest{
			Id: id,
			Relations: func() []*routerproto.DistributedRelation {
				res := make([]*routerproto.DistributedRelation, len(rels))
				for i, rel := range rels {
					res[i] = distributions.DistributedRelatitonToProto(rel)
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

// AlterDistributionDetach detaches relation from distribution
// TODO: unit tests
func (qc *qdbCoordinator) AlterDistributionDetach(ctx context.Context, id string, relName string) error {
	if err := qc.db.AlterDistributionDetach(ctx, id, relName); err != nil {
		return err
	}

	return qc.traverseRouters(ctx, func(cc *grpc.ClientConn) error {
		cl := routerproto.NewDistributionServiceClient(cc)
		resp, err := cl.AlterDistributionDetach(context.TODO(), &routerproto.AlterDistributionDetachRequest{
			Id:       id,
			RelNames: []string{relName},
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

func (qc *qdbCoordinator) GetShard(ctx context.Context, shardID string) (*datashards.DataShard, error) {
	panic("implement or delete me")
}
