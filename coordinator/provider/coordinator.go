package provider

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
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
	"github.com/pg-sharding/spqr/pkg/pool"
	routerproto "github.com/pg-sharding/spqr/pkg/protos"
	"github.com/pg-sharding/spqr/qdb"
	router "github.com/pg-sharding/spqr/router"
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
		if err := cb(cc, r.Address); err != nil {
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
		resp, err := rrClient.ListClients(ctx, &routerproto.ListClientsRequest{})
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
		resp, err := rrBackConn.ListBackendConnections(ctx, &routerproto.ListBackendConnectionsRequest{})
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
		resp, err := rrBackConn.ListPools(ctx, &routerproto.ListPoolsRequest{})
		if err != nil {
			spqrlog.Zero.Error().Msg("error fetching pools with grpc")
			return err
		}

		for _, p := range resp.Pools {
			err = cb(NewCoordPool(p))
			if err != nil {
				return err
			}
		}
		return nil
	})
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

				rrClient := routerproto.NewTopologyServiceClient(cc)

				resp, err := rrClient.GetRouterStatus(ctx, &routerproto.GetRouterStatusRequest{})
				if err != nil {
					return err
				}

				switch resp.Status {
				case routerproto.RouterStatus_CLOSED:
					spqrlog.Zero.Debug().Msg("router is closed")
					if err := qc.SyncRouterMetadata(ctx, internalR); err != nil {
						return err
					}
					if _, err := rrClient.OpenRouter(ctx, &routerproto.OpenRouterRequest{}); err != nil {
						return err
					}

					/* Mark router as opened in qdb */
					err := qc.db.OpenRouter(ctx, internalR.ID)
					if err != nil {
						return err
					}
				case routerproto.RouterStatus_OPENED:
					spqrlog.Zero.Debug().Msg("router is opened")

					/* Mark router as opened in qdb */
					err := qc.db.OpenRouter(ctx, internalR.ID)
					if err != nil {
						return err
					}
					// TODO: consistency checks
				}
				return nil
			}(); err != nil {
				spqrlog.Zero.Error().
					Str("router id", r.ID).
					Err(err).
					Msg("router watchdog coroutine failed")
			}
		}

		time.Sleep(time.Second)
	}
}

func NewCoordinator(tlsconfig *tls.Config, db qdb.XQDB) *qdbCoordinator {
	return &qdbCoordinator{
		db:        db,
		tlsconfig: tlsconfig,
	}
}

// TODO : unit tests
func (qc *qdbCoordinator) lockCoordinator(ctx context.Context, initialRouter bool) bool {
	registerRouter := func() bool {
		if !initialRouter {
			return true
		}
		router := &topology.Router{
			ID:      uuid.NewString(),
			Address: fmt.Sprintf("%s:%s", config.RouterConfig().Host, config.RouterConfig().GrpcApiPort),
			State:   qdb.OPENED,
		}
		if err := qc.RegisterRouter(ctx, router); err != nil {
			spqrlog.Zero.Error().Err(err).Msg("register router when locking coordinator")
		}
		if err := qc.SyncRouterMetadata(ctx, router); err != nil {
			spqrlog.Zero.Error().Err(err).Msg("sync router metadata when locking coordinator")
		}
		if err := qc.UpdateCoordinator(ctx, fmt.Sprintf("%s:%s", config.CoordinatorConfig().Host, config.CoordinatorConfig().GrpcApiPort)); err != nil {
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
					return registerRouter()
				} else {
					spqrlog.Zero.Error().Err(err).Msg("qdb already taken, waiting for connection")
				}
			}
		}
	}

	return registerRouter()
}

// TODO : unit tests
// RunCoordinator side effect: it runs an asynchronous goroutine
// that checks the availability of the SPQR router
func (qc *qdbCoordinator) RunCoordinator(ctx context.Context, initialRouter bool) {
	if !qc.lockCoordinator(ctx, initialRouter) {
		return
	}

	ranges, err := qc.db.ListAllKeyRanges(context.TODO())
	if err != nil {
		spqrlog.Zero.Error().
			Err(err).
			Msg("faild to list key ranges")
	}

	for _, r := range ranges {
		tx, err := qc.db.GetTransferTx(context.TODO(), r.KeyRangeID)
		if err != nil {
			continue
		}
		if tx.ToStatus == qdb.Commited && tx.FromStatus != qdb.Commited {
			datatransfers.ResolvePreparedTransaction(context.TODO(), tx.FromShardId, tx.FromTxName, true)
			tem := kr.MoveKeyRange{
				ShardId: tx.ToShardId,
				Krid:    r.KeyRangeID,
			}
			err = qc.Move(context.TODO(), &tem)
			if err != nil {
				spqrlog.Zero.Error().Err(err).Msg("failed to move key range")
			}
		} else if tx.FromStatus != qdb.Commited {
			datatransfers.ResolvePreparedTransaction(context.TODO(), tx.ToShardId, tx.ToTxName, false)
			datatransfers.ResolvePreparedTransaction(context.TODO(), tx.FromShardId, tx.FromTxName, false)
		}

		err = qc.db.RemoveTransferTx(context.TODO(), r.KeyRangeID)
		if err != nil {
			spqrlog.Zero.Error().Err(err).Msg("error removing from qdb")
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

			if err := cb(cc); err != nil {
				spqrlog.Zero.Debug().Err(err).Str("router id", rtr.ID).Msg("traverse routers")
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
func (qc *qdbCoordinator) AddKeyRange(ctx context.Context, keyRange *kr.KeyRange) error {
	// add key range to metadb
	spqrlog.Zero.Debug().
		Bytes("lower-bound", keyRange.LowerBound).
		Str("shard-id", keyRange.ShardID).
		Str("key-range-id", keyRange.ID).
		Msg("add key range")

	err := ops.AddKeyRangeWithChecks(ctx, qc.db, keyRange)
	if err != nil {
		return err
	}

	return qc.traverseRouters(ctx, func(cc *grpc.ClientConn) error {
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
		return nil
	})
}

// TODO : unit tests
func (qc *qdbCoordinator) ListKeyRanges(ctx context.Context, distribution string) ([]*kr.KeyRange, error) {
	keyRanges, err := qc.db.ListKeyRanges(ctx, distribution)
	if err != nil {
		return nil, err
	}

	keyr := make([]*kr.KeyRange, 0, len(keyRanges))
	for _, keyRange := range keyRanges {
		keyr = append(keyr, kr.KeyRangeFromDB(keyRange))
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
		keyr = append(keyr, kr.KeyRangeFromDB(keyRange))
	}

	return keyr, nil
}

// TODO : unit tests
func (qc *qdbCoordinator) MoveKeyRange(ctx context.Context, keyRange *kr.KeyRange) error {
	return ops.ModifyKeyRangeWithChecks(ctx, qc.db, keyRange)
}

// TODO : unit tests
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
		if err != nil {
			return err
		}

		spqrlog.Zero.Debug().
			Interface("response", resp).
			Msg("lock key range response")
		return nil
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

	krOld, err := qc.db.LockKeyRange(ctx, req.SourceID)
	if err != nil {
		return err
	}

	defer func() {
		if err := qc.db.UnlockKeyRange(ctx, req.SourceID); err != nil {
			spqrlog.Zero.Error().Err(err).Msg("")
		}
	}()

	ds, err := qc.db.GetDistribution(ctx, krOld.DistributionId)
	if err != nil {
		return err
	}

	if kr.CmpRangesEqual(krOld.LowerBound, req.Bound) {
		return spqrerror.New(spqrerror.SPQR_KEYRANGE_ERROR, "failed to split because bound equals lower of the key range")
	}
	if kr.CmpRangesLess(req.Bound, krOld.LowerBound) {
		return spqrerror.New(spqrerror.SPQR_KEYRANGE_ERROR, "failed to split because bound is out of key range")
	}

	krs, err := qc.db.ListKeyRanges(ctx, ds.ID)
	if err != nil {
		return err
	}
	for _, kRange := range krs {
		if kr.CmpRangesLess(krOld.LowerBound, kRange.LowerBound) && kr.CmpRangesLessEqual(kRange.LowerBound, req.Bound) {
			return spqrerror.Newf(spqrerror.SPQR_KEYRANGE_ERROR, "failed to split because bound intersects with \"%s\" key range", kRange.KeyRangeID)
		}
	}

	krNew := &kr.KeyRange{
		LowerBound: func() []byte {
			if req.SplitLeft {
				return krOld.LowerBound
			}
			return req.Bound
		}(),
		ID:           req.Krid,
		ShardID:      krOld.ShardID,
		Distribution: krOld.DistributionId,
	}

	spqrlog.Zero.Debug().
		Bytes("lower-bound", krNew.LowerBound).
		Str("shard-id", krNew.ShardID).
		Str("id", krNew.ID).
		Msg("new key range")

	if req.SplitLeft {
		krOld.LowerBound = req.Bound
	}
	if err := ops.ModifyKeyRangeWithChecks(ctx, qc.db, kr.KeyRangeFromDB(krOld)); err != nil {
		return err
	}

	if err := ops.AddKeyRangeWithChecks(ctx, qc.db, krNew); err != nil {
		return spqrerror.Newf(spqrerror.SPQR_KEYRANGE_ERROR, "failed to add a new key range: %s", err.Error())
	}

	if err := qc.traverseRouters(ctx, func(cc *grpc.ClientConn) error {
		cl := routerproto.NewKeyRangeServiceClient(cc)
		resp, err := cl.SplitKeyRange(ctx, &routerproto.SplitKeyRangeRequest{
			Bound:    req.Bound,
			SourceId: req.SourceID,
			NewId:    krNew.ID,
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

// TODO : unit tests
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

// TODO : unit tests
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

// TODO : unit tests
func (qc *qdbCoordinator) Unite(ctx context.Context, uniteKeyRange *kr.UniteKeyRange) error {
	krBase, err := qc.db.LockKeyRange(ctx, uniteKeyRange.BaseKeyRangeId)
	if err != nil {
		return err
	}

	defer func() {
		if err := qc.db.UnlockKeyRange(ctx, uniteKeyRange.BaseKeyRangeId); err != nil {
			spqrlog.Zero.Error().Err(err).Msg("")
		}
	}()

	krAppendage, err := qc.db.LockKeyRange(ctx, uniteKeyRange.AppendageKeyRangeId)
	if err != nil {
		return err
	}

	defer func() {
		if err := qc.db.UnlockKeyRange(ctx, uniteKeyRange.AppendageKeyRangeId); err != nil {
			spqrlog.Zero.Error().Err(err).Msg("")
		}
	}()

	if krBase.ShardID != krAppendage.ShardID {
		return spqrerror.New(spqrerror.SPQR_KEYRANGE_ERROR, "failed to unite key ranges routing different shards")
	}
	if krBase.DistributionId != krAppendage.DistributionId {
		return spqrerror.New(spqrerror.SPQR_KEYRANGE_ERROR, "failed to unite key ranges of different distributions")
	}
	ds, err := qc.db.GetDistribution(ctx, krBase.DistributionId)
	if err != nil {
		return err
	}
	// TODO: check all types when composite keys are supported
	krLeft, krRight := krBase, krAppendage
	if kr.CmpRangesLess(krRight.LowerBound, krLeft.LowerBound) {
		krLeft, krRight = krRight, krLeft
	}

	krs, err := qc.db.ListKeyRanges(ctx, ds.ID)
	if err != nil {
		return err
	}
	for _, kRange := range krs {
		if kRange.KeyRangeID != krLeft.KeyRangeID &&
			kRange.KeyRangeID != krRight.KeyRangeID &&
			kr.CmpRangesLessEqual(krLeft.LowerBound, kRange.LowerBound) &&
			kr.CmpRangesLessEqual(kRange.LowerBound, krRight.LowerBound) {
			return spqrerror.New(spqrerror.SPQR_KEYRANGE_ERROR, "failed to unite non-adjacent key ranges")
		}
	}

	if err := qc.db.DropKeyRange(ctx, krAppendage.KeyRangeID); err != nil {
		return spqrerror.Newf(spqrerror.SPQR_KEYRANGE_ERROR, "failed to drop an old key range: %s", err.Error())
	}

	if krLeft.KeyRangeID != krBase.KeyRangeID {
		krBase.LowerBound = krAppendage.LowerBound
	}

	if err := ops.ModifyKeyRangeWithChecks(ctx, qc.db, kr.KeyRangeFromDB(krBase)); err != nil {
		return spqrerror.Newf(spqrerror.SPQR_KEYRANGE_ERROR, "failed to update a new key range: %s", err.Error())
	}

	if err := qc.traverseRouters(ctx, func(cc *grpc.ClientConn) error {
		cl := routerproto.NewKeyRangeServiceClient(cc)
		resp, err := cl.MergeKeyRange(ctx, &routerproto.MergeKeyRangeRequest{
			BaseId:      uniteKeyRange.BaseKeyRangeId,
			AppendageId: uniteKeyRange.AppendageKeyRangeId,
		})

		spqrlog.Zero.Debug().
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

// Move key range from one logical shard to another
// This function reshards data by locking a portion of it,
// making it unavailable for read and write access during the process.
// TODO : unit tests
func (qc *qdbCoordinator) Move(ctx context.Context, req *kr.MoveKeyRange) error {
	// First, we create a record in the qdb to track the data movement.
	// If the coordinator crashes during the process, we need to rerun this function.

	spqrlog.Zero.Debug().
		Str("key-range", req.Krid).
		Str("shard-id", req.ShardId).
		Msg("qdb coordinator move key range")

	keyRange, err := qc.db.GetKeyRange(ctx, req.Krid)
	if err != nil {
		return err
	}

	// no need to move data to the same shard
	if keyRange.ShardID == req.ShardId {
		return nil
	}

	moveId, err := qc.RecordKeyRangeMove(ctx,
		&qdb.MoveKeyRange{
			MoveId:     uuid.New().String(),
			ShardId:    req.ShardId,
			KeyRangeID: req.Krid,
			Status:     qdb.MoveKeyRangePlanned,
		})

	/* NOOP if key range move was already recorded */
	if err != nil {
		return err
	}

	krmv, err := qc.LockKeyRange(ctx, req.Krid)
	if err != nil {
		return err
	}
	defer func() {
		if err := qc.UnlockKeyRange(ctx, req.Krid); err != nil {
			spqrlog.Zero.Error().Err(err).Msg("")
		}
	}()

	defer func() {
		// set compelted status in the end of key range move operation
		if err := qc.db.UpdateKeyRangeMoveStatus(ctx, moveId, qdb.MoveKeyRangeComplete); err != nil {
			spqrlog.Zero.Error().Err(err).Msg("")
		}
	}()

	/* physical changes on shards */
	err = datatransfers.MoveKeys(ctx, keyRange.ShardID, req.ShardId, *keyRange, qc.db)
	if err != nil {
		spqrlog.Zero.Error().Err(err).Msg("failed to move rows")
		return err
	}

	// Update the state of the distributed key-range metadata
	krmv.ShardID = req.ShardId
	if err := ops.ModifyKeyRangeWithChecks(ctx, qc.db, krmv); err != nil {
		// TODO: check if unlock here is ok
		return err
	}

	// Notify all routers about scheme changes.
	if err := qc.traverseRouters(ctx, func(cc *grpc.ClientConn) error {
		cl := routerproto.NewKeyRangeServiceClient(cc)
		moveResp, err := cl.MoveKeyRange(ctx, &routerproto.MoveKeyRangeRequest{
			Id:        krmv.ID,
			ToShardId: krmv.ShardID,
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
	resp, err := dsCl.ListDistributions(ctx, &routerproto.ListDistributionsRequest{})
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
	spqrlog.Zero.Debug().Msg("successfully add all key ranges")

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
	_, err = cl.GetRouterStatus(ctx, &routerproto.GetRouterStatusRequest{})
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

// TODO : unit tests
func (qc *qdbCoordinator) PrepareClient(nconn net.Conn) (CoordinatorClient, error) {
	cl := psqlclient.NewPsqlClient(nconn, port.DefaultRouterPortType, "")

	if err := cl.Init(qc.tlsconfig); err != nil {
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
	r.SetParams(cl.Params())
	if err := cl.Auth(r); err != nil {
		return nil, err
	}
	spqrlog.Zero.Info().Msg("client auth OK")

	return cl, nil
}

// TODO : unit tests
func (qc *qdbCoordinator) ProcClient(ctx context.Context, nconn net.Conn) error {
	cl, err := qc.PrepareClient(nconn)
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

func (qc *qdbCoordinator) GetShardInfo(ctx context.Context, shardID string) (*datashards.DataShard, error) {
	panic("implement or delete me")
}
