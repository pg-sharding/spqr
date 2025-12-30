package qdb

import (
	"context"
	"encoding/json"
	"fmt"
	"path"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/pg-sharding/spqr/coordinator/statistics"
	"github.com/pg-sharding/spqr/pkg/models/spqrerror"
	"github.com/pg-sharding/spqr/router/rfqn"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/clientv3util"
	"go.etcd.io/etcd/client/v3/concurrency"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/pg-sharding/spqr/pkg/spqrlog"

	retry "github.com/sethvargo/go-retry"
)

type EtcdQDB struct {
	cli *clientv3.Client
}

var _ XQDB = &EtcdQDB{}

func NewEtcdQDB(addr string, maxCallSendMsgSize int) (*EtcdQDB, error) {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints: []string{addr},
		DialOptions: []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		},
		MaxCallSendMsgSize: maxCallSendMsgSize,
	})
	if err != nil {
		return nil, err
	}

	spqrlog.Zero.Debug().
		Str("address", addr).
		Uint("client", spqrlog.GetPointer(cli)).
		Msg("etcdqdb: NewEtcdQDB")

	return &EtcdQDB{
		cli: cli,
	}, nil
}

const (
	keyRangesNamespace               = "/keyranges/"
	distributionNamespace            = "/distributions/"
	keyRangeMovesNamespace           = "/krmoves/"
	routersNamespace                 = "/routers/"
	shardsNamespace                  = "/shards/"
	relationMappingNamespace         = "/relation_mappings/"
	taskGroupsNamespace              = "/move_task_groups/"
	moveTaskNamespace                = "/move_tasks/"
	redistributeTaskPath             = "/redistribute_task/"
	balancerTaskPath                 = "/balancer_task/"
	transactionNamespace             = "/transfer_txs/"
	sequenceNamespace                = "/sequences/"
	referenceRelationsNamespace      = "/reference_relations"
	uniqueIndexesNamespace           = "/unique_indexes"
	columnSequenceMappingNamespace   = "/column_sequence_mappings/"
	lockNamespace                    = "/lock"
	totalKeysNamespace               = "/total_keys/"
	stopMoveTaskGroupNamespace       = "/stop_move_task_group/"
	moveTaskByGroupNamespace         = "/group_move_tasks/"
	uniqueIndexesByRelationNamespace = "/relation_unique_indexes"

	CoordKeepAliveTtl  = 3
	coordLockKey       = "coordinator_exists"
	sequenceSpace      = "sequence_space"
	transactionRequest = "transaction_request"

	MaxLockRetry = 7
)

func LockPath(key string) string {
	return path.Join(lockNamespace, key)
}

func keyRangeNodePath(key string) string {
	return path.Join(keyRangesNamespace, key)
}

func routerNodePath(key string) string {
	return path.Join(routersNamespace, key)
}

func shardNodePath(key string) string {
	return path.Join(shardsNamespace, key)
}

func distributionNodePath(key string) string {
	return path.Join(distributionNamespace, key)
}

func referenceRelationNodePath(key string) string {
	return path.Join(referenceRelationsNamespace, key)
}

func uniqueIndexNodePath(key string) string {
	return path.Join(uniqueIndexesNamespace, key)
}

func relationMappingNodePath(key string) string {
	return path.Join(relationMappingNamespace, key)
}

func keyRangeMovesNodePath(key string) string {
	return path.Join(keyRangeMovesNamespace, key)
}

func transferTxNodePath(key string) string {
	return path.Join(transactionNamespace, key)
}

func sequenceNodePath(key string) string {
	return path.Join(sequenceNamespace, key)
}

func relationSequenceMappingNodePath(relName string) string {
	return path.Join(columnSequenceMappingNamespace, relName)
}

func columnSequenceMappingNodePath(relName, colName string) string {
	return path.Join(relationSequenceMappingNodePath(relName), colName)
}

func taskGroupNodePath(id string) string {
	return path.Join(taskGroupsNamespace, id)
}

func totalKeysNodePath(id string) string {
	return path.Join(totalKeysNamespace, id)
}

func taskGroupStopFlagNodePath(id string) string {
	return path.Join(stopMoveTaskGroupNamespace, id)
}

func moveTaskNodePath(id string) string {
	return path.Join(moveTaskNamespace, id)
}

func moveTaskByGroupNodePath(taskGroupID string) string {
	return path.Join(moveTaskByGroupNamespace, taskGroupID)
}

func keyRangeLockNamespace() string {
	return path.Join(lockNamespace, keyRangesNamespace)
}

func uniqueIndexesByRelationNodePath(relName string) string {
	return path.Join(uniqueIndexesByRelationNamespace, relName)
}

func (q *EtcdQDB) Client() *clientv3.Client {
	return q.cli
}

// ==============================================================================
//                                 KEY RANGES
// ==============================================================================

// TODO : unit tests
func (q *EtcdQDB) CreateKeyRange(ctx context.Context, keyRange *KeyRange) error {
	spqrlog.Zero.Debug().
		Interface("key-range", keyRange).
		Msg("etcdqdb: add key range")

	t := time.Now()

	rawKeyRange, err := json.Marshal(keyRangeToInternal(keyRange))
	if err != nil {
		return err
	}
	ops := []clientv3.Op{
		clientv3.OpPut(keyRangeNodePath(keyRange.KeyRangeID), string(rawKeyRange)),
	}
	if keyRange.Locked {
		ops = append(ops, clientv3.OpPut(keyRangeNodePath(keyRange.KeyRangeID), "locked"))
	}

	resp, err := q.cli.Txn(ctx).
		If(clientv3.Compare(clientv3.Version(keyRangeNodePath(keyRange.KeyRangeID)), "=", 0)).
		Then(ops...).
		Commit()
	if err != nil {
		return err
	}
	if !resp.Succeeded {
		return fmt.Errorf("key range \"%s\" already exists", keyRange.KeyRangeID)
	}

	spqrlog.Zero.Debug().
		Interface("response", resp).
		Msg("etcdqdb: put key range to qdb")

	statistics.RecordQDBOperation("CreateKeyRange", time.Since(t))
	return err
}

// TODO : unit tests
func (q *EtcdQDB) fetchKeyRange(ctx context.Context, krNodePath string) (*KeyRange, error) {
	spqrlog.Zero.Debug().
		Interface("path", krNodePath).
		Msg("etcdqdb: fetch key range")

	resp, err := q.cli.Txn(ctx).
		If(clientv3.Compare(clientv3.Version(krNodePath), "!=", 0)).
		Then(clientv3.OpGet(krNodePath), clientv3.OpGet(LockPath(krNodePath))).
		Commit()

	if err != nil {
		return nil, spqrerror.Newf(spqrerror.SPQR_KEYRANGE_ERROR, "failed to fetch key range at \"%s\": failed to commit transaction: %s", krNodePath, err)
	}
	if !resp.Succeeded {
		return nil, spqrerror.Newf(spqrerror.SPQR_KEYRANGE_ERROR, "no key range found at %v", krNodePath)
	}
	if len(resp.Responses) != 2 {
		return nil, fmt.Errorf("failed to fetch key range at \"%s\": unexpected etcd response count %d", krNodePath, len(resp.Responses))
	}
	kRange := &internalKeyRange{}
	if err := json.Unmarshal(resp.Responses[0].GetResponseRange().Kvs[0].Value, &kRange); err != nil {
		return nil, err
	}
	isLocked := resp.Responses[1].GetResponseRange().Count > 0 && string(resp.Responses[1].GetResponseRange().Kvs[0].Value) == "locked"

	return keyRangeFromInternal(kRange, isLocked), nil
}

// TODO : unit tests
func (q *EtcdQDB) GetKeyRange(ctx context.Context, id string) (*KeyRange, error) {
	spqrlog.Zero.Debug().
		Str("id", id).
		Msg("etcdqdb: get key range")

	t := time.Now()

	kRange, err := q.fetchKeyRange(ctx, keyRangeNodePath(id))

	spqrlog.Zero.Debug().
		Interface("ret", kRange).
		Msg("etcdqdb: get key range")
	statistics.RecordQDBOperation("GetKeyRange", time.Since(t))
	return kRange, err
}

// TODO : unit tests
func (q *EtcdQDB) UpdateKeyRange(ctx context.Context, keyRange *KeyRange) error {
	spqrlog.Zero.Debug().
		Interface("key-range", keyRange).
		Msg("etcdqdb: update key range")

	t := time.Now()

	rawKeyRange, err := json.Marshal(keyRangeToInternal(keyRange))
	if err != nil {
		return err
	}

	resp, err := q.cli.Put(ctx, keyRangeNodePath(keyRange.KeyRangeID), string(rawKeyRange))
	if err != nil {
		return err
	}

	spqrlog.Zero.Debug().
		Interface("response", resp).
		Msg("etcdqdb: put key range to qdb")
	statistics.RecordQDBOperation("UpdateKeyRange", time.Since(t))
	return err
}

// TODO : unit tests
func (q *EtcdQDB) DropKeyRangeAll(ctx context.Context) error {
	spqrlog.Zero.Debug().Msg("etcdqdb: drop all key ranges")

	resp, err := q.cli.Delete(ctx, keyRangesNamespace, clientv3.WithPrefix())
	if err != nil {
		return err
	}

	spqrlog.Zero.Debug().
		Interface("response", resp).
		Msg("etcdqdb: got delete with prefix reps")

	return nil
}

// TODO : unit tests
func (q *EtcdQDB) DropKeyRange(ctx context.Context, id string) error {
	spqrlog.Zero.Debug().
		Str("id", id).
		Msg("etcdqdb: drop key range")

	t := time.Now()

	resp, err := q.cli.Delete(ctx, keyRangeNodePath(id))

	spqrlog.Zero.Debug().
		Interface("response", resp).
		Msg("etcdqdb: drop key range")

	statistics.RecordQDBOperation("DropKeyRange", time.Since(t))
	return err
}

// TODO : unit tests
func (q *EtcdQDB) ListKeyRanges(ctx context.Context, distribution string) ([]*KeyRange, error) {
	spqrlog.Zero.Debug().
		Str("distribution", distribution).
		Msg("etcdqdb: list key ranges")

	allKrs, err := q.ListAllKeyRanges(ctx)
	if err != nil {
		return nil, err
	}
	t := time.Now()

	keyRanges := make([]*KeyRange, 0, len(allKrs))

	for i := range allKrs {
		if allKrs[i].DistributionId == distribution {
			keyRanges = append(keyRanges, allKrs[i])
		}
	}

	spqrlog.Zero.Debug().
		Str("distribution", distribution).
		Int("key ranges count", len(keyRanges)).
		Msg("etcdqdb: list key ranges")

	statistics.RecordQDBOperation("ListKeyRanges", time.Since(t))
	return keyRanges, nil
}

// TODO : unit tests
func (q *EtcdQDB) ListAllKeyRanges(ctx context.Context) ([]*KeyRange, error) {
	spqrlog.Zero.Debug().Msg("etcdqdb: list all key ranges")

	t := time.Now()

	resp, err := q.cli.Txn(ctx).Then(clientv3.OpGet(keyRangesNamespace, clientv3.WithPrefix()), clientv3.OpGet(keyRangeLockNamespace(), clientv3.WithPrefix())).Commit()
	if err != nil {
		return nil, err
	}
	if len(resp.Responses) != 2 {
		return nil, fmt.Errorf("failed to list key ranges: unexpected txn response number %d", len(resp.Responses))
	}
	krDbs := resp.Responses[0].GetResponseRange().Kvs
	locks := make(map[string]bool)
	for _, kv := range resp.Responses[1].GetResponseRange().Kvs {
		id := string(kv.Key[len(keyRangeLockNamespace())+1:])
		locks[id] = string(kv.Value) == "locked"
		spqrlog.Zero.Debug().Str("key", string(kv.Key)).Str("id", id).Str("value", string(kv.Value)).Msg("got lock")
	}

	keyRanges := make([]*KeyRange, 0, len(krDbs))

	for _, e := range krDbs {
		var kRange *internalKeyRange
		if err := json.Unmarshal(e.Value, &kRange); err != nil {
			return nil, err
		}

		krLocked := false
		v, ok := locks[kRange.KeyRangeID]
		if ok {
			krLocked = v
		}
		keyRanges = append(keyRanges, keyRangeFromInternal(kRange, krLocked))
	}

	sort.Slice(keyRanges, func(i, j int) bool {
		return keyRanges[i].KeyRangeID < keyRanges[j].KeyRangeID
	})

	spqrlog.Zero.Debug().
		Interface("response", resp).
		Msg("etcdqdb: list all key ranges")

	statistics.RecordQDBOperation("ListAllKeyRanges", time.Since(t))
	return keyRanges, nil
}

func (q *EtcdQDB) NoWaitLockKeyRange(ctx context.Context, idKeyRange string) (*KeyRange, error) {
	spqrlog.Zero.Debug().
		Str("id", idKeyRange).
		Msg("etcdqdb: lock key range (nowait)")
	t := time.Now()
	kr, err := q.internalNoWaitLockKeyRange(ctx, idKeyRange)
	statistics.RecordQDBOperation("LockKeyRange", time.Since(t))
	return kr, err
}

func (q *EtcdQDB) internalNoWaitLockKeyRange(ctx context.Context, idKeyRange string) (*KeyRange, error) {
	resp, err := q.cli.Txn(ctx).
		If(
			//check exists key range lock
			clientv3.Compare(clientv3.Version(LockPath(keyRangeNodePath(idKeyRange))), "=", 0),
			//check exists key range
			clientv3.Compare(clientv3.Version(keyRangeNodePath(idKeyRange)), ">", 0),
		).
		Then(
			clientv3.OpPut(LockPath(keyRangeNodePath(idKeyRange)), "locked"),
			clientv3.OpGet(keyRangeNodePath(idKeyRange)),
		).
		Else(
			clientv3.OpGet(LockPath(keyRangeNodePath(idKeyRange)), clientv3.WithCountOnly()),
			clientv3.OpGet(keyRangeNodePath(idKeyRange), clientv3.WithCountOnly()),
		).
		Commit()
	if err != nil {
		return nil, err
	}
	if !resp.Succeeded {
		if len(resp.Responses) != 2 {
			return nil, fmt.Errorf("unexpected (case 0) etcd lock '%s' response parts count=%d",
				idKeyRange, len(resp.Responses))
		}
		if resp.Responses[1].GetResponseRange().Count == 0 {
			return nil, spqrerror.Newf(spqrerror.SPQR_KEYRANGE_ERROR, "cant't lock non existent key range %v", idKeyRange)
		}
		spqrlog.Zero.Debug().
			Str("id", idKeyRange).
			Msg(fmt.Sprintf("unsuccessful lock '%s' LS:%d, KR:%d", idKeyRange, resp.Responses[0], resp.Responses[1]))
		return nil, retry.RetryableError(spqrerror.Newf(spqrerror.SPQR_KEYRANGE_ERROR, "key range %v is locked", idKeyRange))
	} else {
		if len(resp.Responses) != 2 {
			return nil, fmt.Errorf("unexpected (case 1) etcd lock '%s' response parts count=%d",
				idKeyRange, len(resp.Responses))
		} else {
			rng := resp.Responses[1].GetResponseRange()
			if len(rng.Kvs) != 1 {
				return nil, fmt.Errorf("unexpected (case 2) etcd lock '%s' response parts count=%d",
					idKeyRange, len(rng.Kvs))
			}
			if rng.Kvs[0] == nil {
				return nil, fmt.Errorf("unexpected etcd lock '%s' invalid key range value  (case 0)",
					idKeyRange)
			}
			kv := rng.Kvs[0].Value
			if kv == nil {
				return nil, fmt.Errorf("unexpected etcd lock '%s' invalid key range value  (case 1)",
					idKeyRange)
			}
			keyRange := &internalKeyRange{}
			if err := json.Unmarshal(kv, &keyRange); err != nil {
				return nil, err
			}
			return keyRangeFromInternal(keyRange, true), nil
		}
	}
}

// TODO : unit tests
func (q *EtcdQDB) LockKeyRange(ctx context.Context, idKeyRange string) (*KeyRange, error) {
	spqrlog.Zero.Debug().
		Str("id", idKeyRange).
		Msg("etcdqdb: lock key range")

	t := time.Now()
	if kr, err := retry.DoValue(ctx, retry.WithMaxRetries(MaxLockRetry,
		retry.NewFibonacci(LockRetryStep)),
		func(ctx context.Context) (*KeyRange, error) {
			return q.internalNoWaitLockKeyRange(ctx, idKeyRange)
		}); err != nil {
		statistics.RecordQDBOperation("LockKeyRange", time.Since(t))
		return nil, err
	} else {
		statistics.RecordQDBOperation("LockKeyRange", time.Since(t))
		return kr, nil
	}
}

// TODO : unit tests
func (q *EtcdQDB) UnlockKeyRange(ctx context.Context, idKeyRange string) error {
	spqrlog.Zero.Debug().
		Str("id", idKeyRange).
		Msg("etcdqdb: unlock key range")

	t := time.Now()
	_, err := q.cli.Delete(ctx, LockPath(keyRangeNodePath(idKeyRange)))
	if err != nil {
		return retry.RetryableError(err)
	}
	statistics.RecordQDBOperation("UnlockKeyRange", time.Since(t))
	return err
}

func (q *EtcdQDB) ListLockedKeyRanges(ctx context.Context) ([]string, error) {
	spqrlog.Zero.Debug().
		Str("key-range lock request", "").
		Msg("etcdqdb: list locked key ranges")
	krLockNs := path.Join(lockNamespace, keyRangesNamespace)
	resp, err := q.cli.Get(ctx, krLockNs, clientv3.WithPrefix())
	result := make([]string, 0, len(resp.Kvs))
	if err != nil {
		return nil, err
	}
	for _, v := range resp.Kvs {
		etcdKey := string(v.Key)
		if strings.Index(etcdKey, krLockNs+"/") != 0 {
			return nil, fmt.Errorf("invalid key in etcd lock namespace:%s", etcdKey)
		}
		result = append(result, etcdKey[len(krLockNs)+1:])
	}
	return result, nil
}

// TODO : unit tests
func (q *EtcdQDB) CheckLockedKeyRange(ctx context.Context, id string) (*KeyRange, error) {
	spqrlog.Zero.Debug().
		Str("id", id).
		Msg("etcdqdb: check locked key range")

	t := time.Now()

	kRange, err := q.GetKeyRange(ctx, id)
	if err != nil {
		return nil, err
	}
	if !kRange.Locked {
		statistics.RecordQDBOperation("CheckLockedKeyRange", time.Since(t))
		return nil, spqrerror.Newf(spqrerror.SPQR_KEYRANGE_ERROR, "key range %v not locked", id)
	}
	statistics.RecordQDBOperation("CheckLockedKeyRange", time.Since(t))
	return kRange, nil
}

// TODO : unit tests
func (q *EtcdQDB) ShareKeyRange(id string) error {
	spqrlog.Zero.Debug().
		Str("id", id).
		Msg("etcdqdb: share key range")
	return fmt.Errorf("implement ShareKeyRange")
}

// TODO: unit tests
func (q *EtcdQDB) RenameKeyRange(ctx context.Context, krId, krIdNew string) error {
	spqrlog.Zero.Debug().
		Str("id", krId).
		Str("new id", krIdNew).
		Msg("etcdqdb: rename key range")

	t := time.Now()

	kr, err := q.fetchKeyRange(ctx, keyRangeNodePath(krId))
	if err != nil {
		return err
	}
	kr.KeyRangeID = krIdNew
	kr.Locked = false

	if _, err = q.cli.Delete(ctx, keyRangeNodePath(krId)); err != nil {
		return err
	}

	_, err = q.cli.Delete(ctx, LockPath(keyRangeNodePath(krId)))
	if err != nil {
		return err
	}

	err = q.CreateKeyRange(ctx, kr)
	statistics.RecordQDBOperation("RenameKeyRange", time.Since(t))
	return err
}

// ==============================================================================
//                           Transfer transactions
// ==============================================================================

// TODO : unit tests
func (q *EtcdQDB) RecordTransferTx(ctx context.Context, key string, info *DataTransferTransaction) error {
	spqrlog.Zero.Debug().
		Str("key", key).
		Msg("etcdqdb: record data transfer tx")

	t := time.Now()
	bts, err := json.Marshal(info)
	if err != nil {
		spqrlog.Zero.Error().Err(err).Msg("failed to marshal transaction")
		return err
	}

	_, err = q.cli.Put(ctx, transferTxNodePath(key), string(bts))
	if err != nil {
		spqrlog.Zero.Error().Err(err).Msg("failed to write transaction")
		return err
	}

	statistics.RecordQDBOperation("RecordTransferTx", time.Since(t))
	return nil
}

// TODO : unit tests
func (q *EtcdQDB) GetTransferTx(ctx context.Context, key string) (*DataTransferTransaction, error) {
	spqrlog.Zero.Debug().
		Str("key", key).
		Msg("etcdqdb: get data transfer tx")

	t := time.Now()
	resp, err := q.cli.Get(ctx, transferTxNodePath(key))
	if err != nil {
		spqrlog.Zero.Error().Err(err).Msg("failed to get transaction")
		return nil, err
	}

	var st DataTransferTransaction
	if len(resp.Kvs) == 0 {
		return nil, nil
	}

	if err := json.Unmarshal(resp.Kvs[0].Value, &st); err != nil {
		spqrlog.Zero.Error().Err(err).Msg("failed to unmarshal transaction")
		return nil, err
	}
	statistics.RecordQDBOperation("GetTransferTx", time.Since(t))
	return &st, nil
}

// TODO : unit tests
func (q *EtcdQDB) RemoveTransferTx(ctx context.Context, key string) error {
	spqrlog.Zero.Debug().
		Str("key", key).
		Msg("etcdqdb: remove data transfer tx")

	t := time.Now()
	_, err := q.cli.Delete(ctx, transferTxNodePath(key))
	if err != nil {
		spqrlog.Zero.Error().Err(err).Msg("failed to delete transaction")
		return err
	}
	statistics.RecordQDBOperation("RemoveTransferTx", time.Since(t))
	return nil
}

// ==============================================================================
//	                           COORDINATOR LOCK
// ==============================================================================

// TODO : unit tests
func (q *EtcdQDB) TryCoordinatorLock(ctx context.Context, addr string) error {
	spqrlog.Zero.Debug().
		Str("address", addr).
		Msg("etcdqdb: try coordinator lock")

	leaseGrantResp, err := q.cli.Grant(ctx, CoordKeepAliveTtl)
	if err != nil {
		spqrlog.Zero.Error().Err(err).Msg("etcdqdb: lease grant failed")
		return err
	}

	// KeepAlive attempts to keep the given lease alive forever. If the keepalive responses posted
	// to the channel are not consumed promptly the channel may become full. When full, the lease
	// client will continue sending keep alive requests to the etcd server, but will drop responses
	// until there is capacity on the channel to send more responses.

	keepAliveCh, err := q.cli.KeepAlive(context.Background(), leaseGrantResp.ID)
	if err != nil {
		spqrlog.Zero.Error().Err(err).Msg("etcdqdb: lease keep alive failed")
		return err
	}

	op := clientv3.OpPut(coordLockKey, addr, clientv3.WithLease(clientv3.LeaseID(leaseGrantResp.ID)))
	tx := q.cli.Txn(ctx).If(clientv3util.KeyMissing(coordLockKey)).Then(op)
	stat, err := tx.Commit()
	if err != nil {
		spqrlog.Zero.Error().Err(err).Msg("etcdqdb: failed to commit coordinator lock")
		return err
	}

	if !stat.Succeeded {
		_, err := q.cli.Revoke(ctx, leaseGrantResp.ID)
		if err != nil {
			return err
		}
		return spqrerror.New(spqrerror.SPQR_UNEXPECTED, "qdb is already in use")
	}

	// okay, we acquired lock, time to spawn keep alive channel
	go func() {
		for resp := range keepAliveCh {
			spqrlog.Zero.Debug().
				Uint64("raft-term", resp.RaftTerm).
				Int64("lease-id", int64(resp.ID)).
				Msg("etcd keep alive channel")
		}
	}()

	return nil
}

// TODO : unit tests
// TODO : implement
func (q *EtcdQDB) UpdateCoordinator(ctx context.Context, address string) error {
	return spqrerror.New(spqrerror.SPQR_NOT_IMPLEMENTED, "UpdateCoordinator not implemented")
}

// TODO : unit tests
func (q *EtcdQDB) GetCoordinator(ctx context.Context) (string, error) {
	spqrlog.Zero.Debug().
		Msg("etcdqdb: get coordinator addr")

	resp, err := q.cli.Get(ctx, coordLockKey)
	if err != nil {
		return "", err
	}

	switch len(resp.Kvs) {
	case 0:
		return "", spqrerror.New(spqrerror.SPQR_CONNECTION_ERROR, "coordinator address was not found")
	case 1:
		return string(resp.Kvs[0].Value), nil
	default:
		return "", spqrerror.New(spqrerror.SPQR_CONNECTION_ERROR, "multiple addresses were found")
	}
}

// ==============================================================================
//                                  ROUTERS
// ==============================================================================

// TODO : unit tests
func (q *EtcdQDB) AddRouter(ctx context.Context, r *Router) error {
	spqrlog.Zero.Debug().
		Str("id", r.ID).
		Str("address", r.Address).
		Str("state", string(r.State)).
		Msg("etcdqdb: add router")

	getResp, err := q.cli.Get(ctx, routerNodePath(r.ID))
	if err != nil {
		return err
	}
	if len(getResp.Kvs) != 0 {
		return spqrerror.Newf(spqrerror.SPQR_ROUTER_ERROR, "router id %s already exists", r.ID)
	}

	routers, err := q.ListRouters(ctx)
	if err != nil {
		return err
	}
	for _, router := range routers {
		if router.Address == r.Address {
			return spqrerror.Newf(spqrerror.SPQR_ROUTER_ERROR, "router with address %s already exists", r.Address)
		}
	}

	bts, err := json.Marshal(r)
	if err != nil {
		return err
	}
	resp, err := q.cli.Put(ctx, routerNodePath(r.ID), string(bts))
	if err != nil {
		return err
	}

	spqrlog.Zero.Debug().
		Interface("response", resp).
		Msg("etcdqdb: put router to qdb")

	return nil
}

// TODO : unit tests
func (q *EtcdQDB) DeleteRouter(ctx context.Context, id string) error {
	spqrlog.Zero.Debug().
		Str("id", id).
		Msg("etcdqdb: drop router")

	if id == "*" {
		id = ""
	}
	resp, err := q.cli.Delete(ctx, routerNodePath(id))
	if err != nil {
		return err
	}

	spqrlog.Zero.Debug().
		Interface("response", resp).
		Msg("etcdqdb: drop router")

	return nil
}

// TODO : unit tests
func (q *EtcdQDB) DeleteRouterAll(ctx context.Context) error {
	spqrlog.Zero.Debug().
		Msg("etcdqdb: unregister all routers")

	resp, err := q.cli.Delete(ctx, routerNodePath(""), clientv3.WithPrefix())
	if err != nil {
		return err
	}

	spqrlog.Zero.Debug().
		Interface("response", resp).
		Msg("etcdqdb: unregister all routers")

	return nil
}

// TODO : unit tests
func (q *EtcdQDB) OpenRouter(ctx context.Context, id string) error {
	spqrlog.Zero.Debug().
		Str("id", id).
		Msg("etcdqdb: open router")
	getResp, err := q.cli.Get(ctx, routerNodePath(id))
	if err != nil {
		return err
	}
	if len(getResp.Kvs) == 0 {
		return spqrerror.Newf(spqrerror.SPQR_ROUTER_ERROR, "router with id %s does not exist", id)
	}

	var routers []*Router
	for _, e := range getResp.Kvs {
		var st Router
		if err := json.Unmarshal(e.Value, &st); err != nil {
			return err
		}
		// TODO: create routers in qdb properly
		routers = append(routers, &st)
	}

	/*  */

	if len(routers) != 1 {
		return spqrerror.Newf(spqrerror.SPQR_ROUTER_ERROR, "sync failed: more than one router with id %s", id)
	}

	if routers[0].State == OPENED {
		spqrlog.Zero.Debug().
			Msg("etcdqdb: router already opened, nothing to do here")
		return nil
	}

	routers[0].State = OPENED

	bts, err := json.Marshal(routers[0])
	if err != nil {
		return err
	}
	resp, err := q.cli.Put(ctx, routerNodePath(routers[0].ID), string(bts))
	if err != nil {
		return err
	}

	spqrlog.Zero.Debug().
		Interface("response", resp).
		Msg("etcdqdb: put router to qdb")

	return nil
}

// TODO : unit tests
func (q *EtcdQDB) CloseRouter(ctx context.Context, id string) error {
	spqrlog.Zero.Debug().
		Str("id", id).
		Msg("etcdqdb: close router")
	getResp, err := q.cli.Get(ctx, routerNodePath(id))
	if err != nil {
		return err
	}
	if len(getResp.Kvs) == 0 {
		return spqrerror.Newf(spqrerror.SPQR_ROUTER_ERROR, "router with id %s does not exist", id)
	}

	var routers []*Router
	for _, e := range getResp.Kvs {
		var st Router
		if err := json.Unmarshal(e.Value, &st); err != nil {
			return err
		}
		// TODO: create routers in qdb properly
		routers = append(routers, &st)
	}

	if len(routers) != 1 {
		return spqrerror.Newf(spqrerror.SPQR_ROUTER_ERROR, "sync failed: more than one router with id %s", id)
	}

	if routers[0].State == CLOSED {
		spqrlog.Zero.Debug().
			Msg("etcdqdb: router already closed, nothing to do here")
		return nil
	}

	routers[0].State = CLOSED

	bts, err := json.Marshal(routers[0])
	if err != nil {
		return err
	}
	resp, err := q.cli.Put(ctx, routerNodePath(routers[0].ID), string(bts))
	if err != nil {
		return err
	}

	spqrlog.Zero.Debug().
		Interface("response", resp).
		Msg("etcdqdb: put router to qdb")

	return nil
}

// TODO : unit tests
func (q *EtcdQDB) ListRouters(ctx context.Context) ([]*Router, error) {
	spqrlog.Zero.Debug().Msg("etcdqdb: list routers")
	t := time.Now()
	resp, err := q.cli.Get(ctx, routersNamespace, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}

	var ret []*Router
	for _, e := range resp.Kvs {
		var st Router
		if err := json.Unmarshal(e.Value, &st); err != nil {
			return nil, err
		}
		// TODO: create routers in qdb properly
		if len(st.State) == 0 {
			st.State = CLOSED
		}
		ret = append(ret, &st)
	}

	sort.Slice(ret, func(i, j int) bool {
		return ret[i].ID < ret[j].ID
	})

	spqrlog.Zero.Debug().
		Interface("response", resp).
		Msg("etcdqdb: list routers")

	statistics.RecordQDBOperation("ListRouters", time.Since(t))
	return ret, nil
}

// ==============================================================================
//                                  SHARDS
// ==============================================================================

// TODO : unit tests
func (q *EtcdQDB) AddShard(ctx context.Context, shard *Shard) error {
	spqrlog.Zero.Debug().
		Str("id", shard.ID).
		Strs("hosts", shard.RawHosts).
		Msg("etcdqdb: add shard")
	t := time.Now()

	bytes, err := json.Marshal(shard)
	if err != nil {
		return err
	}
	resp, err := q.cli.Txn(ctx).
		If(
			//check exists shard with key
			clientv3.Compare(clientv3.Version(shardNodePath(shard.ID)), "=", 0),
		).
		Then(
			clientv3.OpPut(shardNodePath(shard.ID), string(bytes)),
		).
		Commit()

	if err != nil {
		return err
	}
	if len(resp.Responses) == 0 {
		return fmt.Errorf("shard with id %s already exists", shard.ID)
	}

	spqrlog.Zero.Debug().
		Interface("response", resp).
		Msg("etcdqdb: add shard")

	statistics.RecordQDBOperation("AddShard", time.Since(t))
	return nil
}

// TODO : unit tests
func (q *EtcdQDB) ListShards(ctx context.Context) ([]*Shard, error) {
	spqrlog.Zero.Debug().Msg("etcdqdb: list shards")
	t := time.Now()

	resp, err := q.cli.Get(ctx, shardsNamespace, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}

	shards := make([]*Shard, 0, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		var shard *Shard
		if err := json.Unmarshal(kv.Value, &shard); err != nil {
			return nil, err
		}
		shards = append(shards, shard)
	}

	sort.Slice(shards, func(i, j int) bool {
		return shards[i].ID < shards[j].ID
	})

	statistics.RecordQDBOperation("ListShards", time.Since(t))
	return shards, nil
}

func (q *EtcdQDB) GetShard(ctx context.Context, id string) (*Shard, error) {
	spqrlog.Zero.Debug().
		Str("id", id).
		Msg("etcdqdb: get shard")
	t := time.Now()

	nodePath := shardNodePath(id)
	resp, err := q.cli.Get(ctx, nodePath)
	if err != nil {
		return nil, err
	}

	if len(resp.Kvs) == 0 {
		return nil, spqrerror.Newf(spqrerror.SPQR_NO_DATASHARD, "unknown shard %s", id)
	}
	if len(resp.Kvs) > 1 {
		return nil, spqrerror.Newf(spqrerror.SPQR_METADATA_CORRUPTION,
			"multiple metadata entries (%d) found for shard %q; expected exactly one",
			len(resp.Kvs), id)
	}

	shardInfo := &Shard{
		ID: id,
	}
	if err := json.Unmarshal(resp.Kvs[0].Value, shardInfo); err != nil {
		return nil, err
	}
	statistics.RecordQDBOperation("GetShard", time.Since(t))
	return shardInfo, nil
}

// TODO : unit tests
func (q *EtcdQDB) DropShard(ctx context.Context, id string) error {
	spqrlog.Zero.Debug().
		Str("id", id).
		Msg("etcdqdb: drop shard")
	t := time.Now()

	nodePath := shardNodePath(id)
	_, err := q.cli.Delete(ctx, nodePath)
	statistics.RecordQDBOperation("DropShard", time.Since(t))
	return err
}

// ==============================================================================
//                               REFERENCE RELATIONS
// ==============================================================================

// CreateReferenceRelation implements XQDB.
func (q *EtcdQDB) CreateReferenceRelation(ctx context.Context, r *ReferenceRelation) error {
	spqrlog.Zero.Debug().
		Str("tablename", r.TableName).
		Msg("etcdqdb: create reference relation")

	rrJson, err := json.Marshal(r)
	if err != nil {
		return err
	}
	resp, err := q.cli.Put(ctx, referenceRelationNodePath(r.TableName), string(rrJson))
	if err != nil {
		return err
	}

	spqrlog.Zero.Debug().
		Interface("response", resp).
		Msg("etcdqdb: create reference relation response")

	return nil
}

// GetReferenceRelation implements XQDB.
func (q *EtcdQDB) GetReferenceRelation(ctx context.Context, relName *rfqn.RelationFQN) (*ReferenceRelation, error) {
	tableName := relName.RelationName
	spqrlog.Zero.Debug().
		Str("tablename", tableName).
		Msg("etcdqdb: get reference relation")

	resp, err := q.cli.Get(ctx, referenceRelationNodePath(tableName))
	if err != nil {
		return nil, err
	}

	if len(resp.Kvs) == 0 {
		return nil, spqrerror.Newf(spqrerror.SPQR_OBJECT_NOT_EXIST, "replicated relation \"%s\" not found", tableName)
	}

	var refRel *ReferenceRelation
	if err := json.Unmarshal(resp.Kvs[0].Value, &refRel); err != nil {
		return nil, err
	}

	return refRel, nil
}

// AlterReferenceRelationStorage implements XQDB.
func (q *EtcdQDB) AlterReferenceRelationStorage(ctx context.Context, relName *rfqn.RelationFQN, shs []string) error {
	tableName := relName.RelationName
	spqrlog.Zero.Debug().
		Str("tablename", tableName).
		Strs("shards", shs).
		Msg("etcdqdb: alter reference relation shards")

	nodePath := referenceRelationNodePath(tableName)

	resp, err := q.cli.Get(ctx, nodePath)
	if err != nil {
		return err
	}

	switch len(resp.Kvs) {
	case 0:
		return spqrerror.Newf(spqrerror.SPQR_OBJECT_NOT_EXIST, "reference relation \"%s\" not found", relName.String())
	case 1:

		var rrs *ReferenceRelation
		if err := json.Unmarshal(resp.Kvs[0].Value, &rrs); err != nil {
			return err
		}
		rrs.ShardIds = shs

		rrJson, err := json.Marshal(rrs)
		if err != nil {
			return err
		}

		resp, err := q.cli.Put(ctx, nodePath, string(rrJson))

		spqrlog.Zero.Debug().
			Interface("response", resp).
			Msg("etcdqdb: AlterReferenceRelationStorage done")

		return err
	default:
		return spqrerror.Newf(spqrerror.SPQR_INVALID_REQUEST, "too much reference relations matched: %d", len(resp.Kvs))
	}
}

// DropReferenceRelation implements XQDB.
func (q *EtcdQDB) DropReferenceRelation(ctx context.Context, relName *rfqn.RelationFQN) error {
	tableName := relName.RelationName
	spqrlog.Zero.Debug().
		Str("tablename", tableName).
		Msg("etcdqdb: drop reference relation")

	nodePath := referenceRelationNodePath(tableName)

	resp, err := q.cli.Get(ctx, nodePath)
	if err != nil {
		return err
	}

	switch len(resp.Kvs) {
	case 0:
		return spqrerror.Newf(spqrerror.SPQR_OBJECT_NOT_EXIST, "reference relation \"%s\" not found", relName.String())
	case 1:

		var rrs *ReferenceRelation
		if err := json.Unmarshal(resp.Kvs[0].Value, &rrs); err != nil {
			return err
		}

		resp, err := q.cli.Delete(ctx, nodePath)

		spqrlog.Zero.Debug().
			Interface("response", resp).
			Msg("etcdqdb: drop reference relation")

		if err != nil {
			return err
		}
		/* Drop all related mappings */
		_, err = q.cli.Delete(ctx, relationMappingNodePath(tableName))
		return err
	default:
		return spqrerror.Newf(spqrerror.SPQR_INVALID_REQUEST, "too much reference relations matched: %d", len(resp.Kvs))
	}
}

// ListReferenceRelations implements XQDB.
func (q *EtcdQDB) ListReferenceRelations(ctx context.Context) ([]*ReferenceRelation, error) {
	spqrlog.Zero.Debug().Msg("etcdqdb: list reference relations")

	resp, err := q.cli.Get(ctx, referenceRelationsNamespace, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}

	dds := make([]*ReferenceRelation, 0, len(resp.Kvs))

	for _, kv := range resp.Kvs {
		var refRelation *ReferenceRelation
		err := json.Unmarshal(kv.Value, &refRelation)
		if err != nil {
			return nil, err
		}

		dds = append(dds, refRelation)
	}

	sort.Slice(dds, func(i, j int) bool {
		return dds[i].TableName < dds[j].TableName
	})

	spqrlog.Zero.Debug().
		Interface("response", resp).
		Msg("etcdqdb: list reference relations")
	return dds, nil
}

// ==============================================================================
//                                  DISTRIBUTIONS
// ==============================================================================

// TODO : unit tests
func (q *EtcdQDB) CreateDistribution(ctx context.Context, distribution *Distribution) ([]QdbStatement, error) {
	spqrlog.Zero.Debug().
		Str("id", distribution.ID).
		Msg("etcdqdb: add distribution")

	distrJson, err := json.Marshal(distribution)
	if err != nil {
		return nil, err
	}
	if resp, err := NewQdbStatement(CMD_PUT, distributionNodePath(distribution.ID), string(distrJson)); err != nil {
		return nil, err
	} else {
		spqrlog.Zero.Debug().
			Interface("response", resp).
			Msg("etcdqdb: add distribution")
		return []QdbStatement{*resp}, nil
	}
}

// TODO : unit tests
func (q *EtcdQDB) ListDistributions(ctx context.Context) ([]*Distribution, error) {
	spqrlog.Zero.Debug().Msg("etcdqdb: list distributions")

	resp, err := q.cli.Get(ctx, distributionNamespace, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}

	dds := make([]*Distribution, 0, len(resp.Kvs))

	for _, kv := range resp.Kvs {
		var distr *Distribution
		err := json.Unmarshal(kv.Value, &distr)
		if err != nil {
			return nil, err
		}

		dds = append(dds, distr)
	}

	sort.Slice(dds, func(i, j int) bool {
		return dds[i].ID < dds[j].ID
	})

	spqrlog.Zero.Debug().
		Interface("response", resp).
		Msg("etcdqdb: list distributions")
	return dds, nil
}

// TODO : unit tests
func (q *EtcdQDB) DropDistribution(ctx context.Context, id string) error {
	spqrlog.Zero.Debug().
		Str("id", id).
		Msg("etcdqdb: drop distribution")

	resp, err := q.cli.Get(ctx, distributionNodePath(id))
	if err != nil {
		return err
	}

	switch len(resp.Kvs) {
	case 0:
		return spqrerror.New(spqrerror.SPQR_OBJECT_NOT_EXIST, "no such distribution present in qdb")
	case 1:

		var distrib *Distribution
		if err := json.Unmarshal(resp.Kvs[0].Value, &distrib); err != nil {
			return err
		}

		resp, err := q.cli.Delete(ctx, distributionNodePath(id))

		spqrlog.Zero.Debug().
			Interface("response", resp).
			Msg("etcdqdb: drop distribution")

		if err != nil {
			return err
		}

		for _, r := range distrib.Relations {
			_, err := q.cli.Delete(ctx, relationMappingNodePath(r.Name))
			if err != nil {
				return err
			}
		}

		return nil
	default:
		return spqrerror.Newf(spqrerror.SPQR_INVALID_REQUEST, "too much distributions matched: %d", len(resp.Kvs))
	}
}

// TODO : unit tests
func (q *EtcdQDB) AlterDistributionAttach(ctx context.Context, id string, rels []*DistributedRelation) error {
	spqrlog.Zero.Debug().
		Str("id", id).
		Msg("etcdqdb: attach table to distribution")

	distribution, err := q.GetDistribution(ctx, id)
	if err != nil {
		return err
	}

	for _, rel := range rels {
		if _, ok := distribution.Relations[rel.Name]; ok {
			return spqrerror.Newf(spqrerror.SPQR_INVALID_REQUEST, "relation \"%s\" is already attached", rel.Name)
		}
		distribution.Relations[rel.Name] = rel
		qname := rel.QualifiedName()
		_, err := q.GetRelationDistribution(ctx, qname)
		switch e := err.(type) {
		case *spqrerror.SpqrError:
			if e.ErrorCode != spqrerror.SPQR_OBJECT_NOT_EXIST {
				return spqrerror.Newf(spqrerror.SPQR_INVALID_REQUEST, "relation \"%s\" is already attached", rel.Name)
			}
		default:
			return spqrerror.Newf(spqrerror.SPQR_INVALID_REQUEST, "relation \"%s\" is already attached", rel.Name)
		}

		resp, err := q.cli.Put(ctx, relationMappingNodePath(rel.Name), id)
		spqrlog.Zero.Debug().
			Interface("response", resp).
			Msg("etcdqdb: attach table to distribution")
		if err != nil {
			return err
		}
	}

	if operations, err := q.CreateDistribution(ctx, distribution); err != nil {
		return err
	} else {
		return q.ExecNoTransaction(ctx, operations)
	}
}

// TODO: unit tests
func (q *EtcdQDB) AlterDistributionDetach(ctx context.Context, id string, relName *rfqn.RelationFQN) error {
	spqrlog.Zero.Debug().
		Str("id", id).
		Str("relation", relName.String()).
		Msg("etcdqdb: detach table from distribution")

	distribution, err := q.GetDistribution(ctx, id)
	if err != nil {
		return err
	}

	if err := q.AlterSequenceDetachRelation(ctx, relName); err != nil {
		return err
	}

	delete(distribution.Relations, relName.RelationName)
	var operations []QdbStatement
	if operations, err = q.CreateDistribution(ctx, distribution); err != nil {
		return err
	}
	if err = q.ExecNoTransaction(ctx, operations); err != nil {
		return err
	}
	_, err = q.cli.Delete(ctx, relationMappingNodePath(relName.RelationName))
	return err
}

// TODO : unit tests
func (q *EtcdQDB) AlterDistributedRelation(ctx context.Context, id string, rel *DistributedRelation) error {
	spqrlog.Zero.Debug().
		Str("id", id).
		Msg("etcdqdb: alter distributed table")

	distribution, err := q.GetDistribution(ctx, id)
	if err != nil {
		return err
	}

	if _, ok := distribution.Relations[rel.Name]; !ok {
		return spqrerror.Newf(spqrerror.SPQR_INVALID_REQUEST, "relation \"%s\" is not attached", rel.Name)
	}
	distribution.Relations[rel.Name] = rel
	qname := rel.QualifiedName()
	if ds, err := q.GetRelationDistribution(ctx, qname); err != nil {
		return spqrerror.Newf(spqrerror.SPQR_INVALID_REQUEST, "relation \"%s\" is not attached", rel.Name)
	} else if ds.ID != id {
		return spqrerror.Newf(spqrerror.SPQR_INVALID_REQUEST, "relation \"%s\" is attached to distribution \"%s\", attempt to alter in distribution \"%s\"", rel.Name, ds.ID, id)
	}

	if operations, err := q.CreateDistribution(ctx, distribution); err != nil {
		return err
	} else {
		return q.ExecNoTransaction(ctx, operations)
	}
}

// TODO : unit tests
func (q *EtcdQDB) AlterDistributedRelationSchema(ctx context.Context, id string, relName string, schemaName string) error {
	spqrlog.Zero.Debug().
		Str("id", id).
		Str("relName", relName).
		Str("schemaName", schemaName).
		Msg("etcdqdb: alter distributed relation schema")

	distribution, err := q.GetDistribution(ctx, id)
	if err != nil {
		return err
	}

	if _, ok := distribution.Relations[relName]; !ok {
		return spqrerror.Newf(spqrerror.SPQR_INVALID_REQUEST, "relation \"%s\" is not attached", relName)
	}
	distribution.Relations[relName].SchemaName = schemaName
	if ds, err := q.GetRelationDistribution(ctx, &rfqn.RelationFQN{RelationName: relName}); err != nil {
		return spqrerror.Newf(spqrerror.SPQR_INVALID_REQUEST, "relation \"%s\" is not attached", relName)
	} else if ds.ID != id {
		return spqrerror.Newf(spqrerror.SPQR_INVALID_REQUEST, "relation \"%s\" is attached to distribution \"%s\", attempt to alter in distribution \"%s\"", relName, ds.ID, id)
	}

	if operations, err := q.CreateDistribution(ctx, distribution); err != nil {
		return err
	} else {
		return q.ExecNoTransaction(ctx, operations)
	}
}

// TODO : unit tests
func (q *EtcdQDB) AlterReplicatedRelationSchema(ctx context.Context, dsID string, relName string, schemaName string) error {
	spqrlog.Zero.Debug().
		Str("relName", relName).
		Str("schemaName", schemaName).
		Msg("etcdqdb: alter replicated relation schema")

	distribution, err := q.GetDistribution(ctx, dsID)
	if err != nil {
		return err
	}

	if _, ok := distribution.Relations[relName]; !ok {
		return spqrerror.Newf(spqrerror.SPQR_INVALID_REQUEST, "relation \"%s\" is not attached", relName)
	}
	distribution.Relations[relName].SchemaName = schemaName
	if ds, err := q.GetRelationDistribution(ctx, &rfqn.RelationFQN{RelationName: relName}); err != nil {
		return spqrerror.Newf(spqrerror.SPQR_INVALID_REQUEST, "relation \"%s\" is not attached", relName)
	} else if ds.ID != dsID {
		return spqrerror.Newf(spqrerror.SPQR_INVALID_REQUEST, "relation \"%s\" is attached to distribution \"%s\", attempt to alter in distribution \"%s\"", relName, ds.ID, dsID)
	}
	rel, err := q.GetReferenceRelation(ctx, &rfqn.RelationFQN{RelationName: relName})
	if err != nil {
		return fmt.Errorf("failed to get reference table: %s", err)
	}
	rel.SchemaName = schemaName
	relJson, err := json.Marshal(rel)
	if err != nil {
		return fmt.Errorf("failed to marshal reference table: %s", err)
	}

	distrJson, err := json.Marshal(distribution)
	if err != nil {
		return err
	}
	_, err = q.cli.Txn(ctx).Then(
		clientv3.OpPut(distributionNodePath(distribution.ID), string(distrJson)),
		clientv3.OpPut(referenceRelationNodePath(relName), string(relJson)),
	).Commit()
	return err
}

// TODO : unit tests
func (q *EtcdQDB) AlterDistributedRelationDistributionKey(ctx context.Context, id string, relName string, distributionKey []DistributionKeyEntry) error {
	spqrlog.Zero.Debug().
		Str("id", id).
		Str("relName", relName).
		Msg("etcdqdb: alter distributed relation distribution key")

	distribution, err := q.GetDistribution(ctx, id)
	if err != nil {
		return err
	}

	if _, ok := distribution.Relations[relName]; !ok {
		return spqrerror.Newf(spqrerror.SPQR_INVALID_REQUEST, "relation \"%s\" is not attached", relName)
	}
	distribution.Relations[relName].DistributionKey = distributionKey
	if ds, err := q.GetRelationDistribution(ctx, &rfqn.RelationFQN{RelationName: relName}); err != nil {
		return spqrerror.Newf(spqrerror.SPQR_INVALID_REQUEST, "relation \"%s\" is not attached", relName)
	} else if ds.ID != id {
		return spqrerror.Newf(spqrerror.SPQR_INVALID_REQUEST, "relation \"%s\" is attached to distribution \"%s\", attempt to alter in distribution \"%s\"", relName, ds.ID, id)
	}

	if operations, err := q.CreateDistribution(ctx, distribution); err != nil {
		return err
	} else {
		return q.ExecNoTransaction(ctx, operations)
	}
}

// TODO : unit tests
func (q *EtcdQDB) GetDistribution(ctx context.Context, id string) (*Distribution, error) {
	spqrlog.Zero.Debug().
		Str("id", id).
		Msg("etcdqdb: get distribution by id")

	resp, err := q.cli.Get(ctx, distributionNodePath(id))
	if err != nil {
		return nil, err
	}

	if len(resp.Kvs) == 0 {
		return nil, spqrerror.Newf(spqrerror.SPQR_OBJECT_NOT_EXIST, "distribution \"%s\" not found", id)
	}

	var distrib *Distribution
	if err := json.Unmarshal(resp.Kvs[0].Value, &distrib); err != nil {
		return nil, err
	}

	return distrib, nil
}

// TODO : unit tests
func (q *EtcdQDB) CheckDistribution(ctx context.Context, id string) (bool, error) {
	spqrlog.Zero.Debug().
		Str("id", id).
		Msg("etcdqdb: check for distribution")

	resp, err := q.cli.Get(ctx, distributionNodePath(id), clientv3.WithCountOnly())
	if err != nil {
		return false, err
	}

	return resp.Count == 1, nil
}

// TODO : unit tests
func (q *EtcdQDB) GetRelationDistribution(ctx context.Context, relName *rfqn.RelationFQN) (*Distribution, error) {
	spqrlog.Zero.Debug().
		Str("relation", relName.RelationName).
		Msg("etcdqdb: get distribution for relation")

	resp, err := q.cli.Get(ctx, relationMappingNodePath(relName.RelationName))
	if err != nil {
		return nil, err
	}
	switch len(resp.Kvs) {
	case 0:
		return nil, spqrerror.Newf(spqrerror.SPQR_OBJECT_NOT_EXIST, "distribution for relation \"%s\" not found", relName)

	case 1:
		id := string(resp.Kvs[0].Value)
		return q.GetDistribution(ctx, id)
	default:
		// metadata corruption
		return nil, spqrerror.NewByCode(spqrerror.SPQR_METADATA_CORRUPTION)
	}
}

// ==============================================================================
//                               UNIQUE INDEXES
// ==============================================================================

func (q *EtcdQDB) ListUniqueIndexes(ctx context.Context) (map[string]*UniqueIndex, error) {
	spqrlog.Zero.Debug().
		Msg("etcdqdb: list unique indexes")

	resp, err := q.cli.Get(ctx, uniqueIndexesNamespace, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}
	res := make(map[string]*UniqueIndex)
	for _, kv := range resp.Kvs {
		var idx *UniqueIndex
		if err := json.Unmarshal(kv.Value, &idx); err != nil {
			return nil, err
		}
		res[idx.ID] = idx
	}
	return res, nil
}

func (q *EtcdQDB) CreateUniqueIndex(ctx context.Context, idx *UniqueIndex) error {
	spqrlog.Zero.Debug().
		Msg("etcdqdb: create unique index")

	tx, err := NewTransaction()
	if err != nil {
		return err
	}
	if err := q.BeginTransaction(ctx, tx); err != nil {
		return err
	}

	ds, err := q.GetDistribution(ctx, idx.DistributionId)
	if err != nil {
		return err
	}
	ds.UniqueIndexes[idx.ID] = idx
	idxJson, err := json.Marshal(idx)
	if err != nil {
		return err
	}
	currRelIdxs, _, err := q.listRelationIndexesWithVersion(ctx, idx.Relation.RelationName)
	if err != nil {
		return err
	}
	currRelIdxs[idx.ColumnName] = idx
	idxsByRelJson, err := json.Marshal(currRelIdxs)
	if err != nil {
		return err
	}
	dsCommand, err := q.CreateDistribution(ctx, ds)
	if err != nil {
		return err
	}
	if err = tx.Append(dsCommand); err != nil {
		return err
	}
	idxCommand, err := NewQdbStatement(CMD_PUT, uniqueIndexNodePath(idx.ID), string(idxJson))
	if err != nil {
		return err
	}

	idxByRelCommand, err := NewQdbStatement(CMD_PUT, uniqueIndexesByRelationNodePath(idx.Relation.RelationName), string(idxsByRelJson))
	if err != nil {
		return err
	}

	if err = tx.Append([]QdbStatement{
		*idxCommand,
		*idxByRelCommand,
	}); err != nil {
		return err
	}
	return q.CommitTransaction(ctx, tx)
}

func (q *EtcdQDB) DropUniqueIndex(ctx context.Context, id string) error {
	spqrlog.Zero.Debug().
		Msg("etcdqdb: drop unique index")

	tx, err := NewTransaction()
	if err != nil {
		return err
	}
	if err := q.BeginTransaction(ctx, tx); err != nil {
		return err
	}

	resp, err := q.cli.Get(ctx, uniqueIndexNodePath(id))
	if err != nil {
		return err
	}

	if len(resp.Kvs) == 0 {
		return fmt.Errorf("unique index \"%s\" not found", id)
	}

	var idx *UniqueIndex
	if err = json.Unmarshal(resp.Kvs[0].Value, &idx); err != nil {
		return err
	}
	ds, err := q.GetDistribution(ctx, idx.DistributionId)
	if err != nil {
		return err
	}
	delete(ds.UniqueIndexes, id)

	currRelIdxs, _, err := q.listRelationIndexesWithVersion(ctx, idx.Relation.RelationName)
	if err != nil {
		return err
	}
	delete(currRelIdxs, idx.ColumnName)
	idxsByRelJson, err := json.Marshal(currRelIdxs)
	if err != nil {
		return err
	}

	dsCommand, err := q.CreateDistribution(ctx, ds)
	if err != nil {
		return err
	}
	if err = tx.Append(dsCommand); err != nil {
		return err
	}
	idxCommand, err := NewQdbStatement(CMD_DELETE, uniqueIndexNodePath(idx.ID), "")
	if err != nil {
		return err
	}

	idxByRelCommand, err := NewQdbStatement(CMD_PUT, uniqueIndexesByRelationNodePath(idx.Relation.RelationName), string(idxsByRelJson))
	if err != nil {
		return err
	}

	if err := tx.Append([]QdbStatement{
		*idxCommand,
		*idxByRelCommand,
	}); err != nil {
		return err
	}
	return q.CommitTransaction(ctx, tx)
}

func (q *EtcdQDB) ListRelationIndexes(ctx context.Context, relName string) (map[string]*UniqueIndex, error) {
	spqrlog.Zero.Debug().
		Msg("etcdqdb: list relation unique indexes")

	idxs, _, err := q.listRelationIndexesWithVersion(ctx, relName)
	return idxs, err
}

func (q *EtcdQDB) listRelationIndexesWithVersion(ctx context.Context, relName string) (map[string]*UniqueIndex, int64, error) {
	resp, err := q.cli.Get(ctx, uniqueIndexesByRelationNodePath(relName))
	if err != nil {
		return nil, 0, err
	}
	if resp.Count == 0 {
		return map[string]*UniqueIndex{}, 0, nil
	}

	idxs := make(map[string]*UniqueIndex)
	if err = json.Unmarshal(resp.Kvs[0].Value, &idxs); err != nil {
		return nil, 0, err
	}
	return idxs, resp.Kvs[0].Version, nil
}

// ==============================================================================
//                                    TASKS
// ==============================================================================

func (q *EtcdQDB) ListTaskGroups(ctx context.Context) (map[string]*MoveTaskGroup, error) {
	spqrlog.Zero.Debug().
		Msg("etcdqdb: list task groups")

	t := time.Now()
	res := make(map[string]*MoveTaskGroup)

	resp, err := q.cli.Get(ctx, taskGroupsNamespace, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}
	for _, kv := range resp.Kvs {
		id := string(kv.Key)[len(taskGroupsNamespace):]
		var task *MoveTaskGroup
		if err := json.Unmarshal(kv.Value, &task); err != nil {
			return nil, err
		}
		spqrlog.Zero.Debug().Str("id", id).Msg("got task group")
		res[id] = task
	}

	statistics.RecordQDBOperation("ListTaskGroups", time.Since(t))
	return res, nil
}

// TODO: unit tests
func (q *EtcdQDB) GetMoveTaskGroup(ctx context.Context, id string) (*MoveTaskGroup, error) {
	spqrlog.Zero.Debug().
		Str("id", id).
		Msg("etcdqdb: get task group")

	t := time.Now()

	resp, err := q.cli.Get(ctx, taskGroupNodePath(id), clientv3.WithFirstKey()...)
	if err != nil {
		return nil, fmt.Errorf("failed to get task group: %s", err)
	}

	if len(resp.Kvs) == 0 {
		return nil, nil
	}
	var taskGroup *MoveTaskGroup
	if err := json.Unmarshal(resp.Kvs[0].Value, &taskGroup); err != nil {
		return nil, fmt.Errorf("failed to unmarshal task group: %s", err)
	}

	statistics.RecordQDBOperation("GetMoveTaskGroup", time.Since(t))
	return taskGroup, nil
}

// TODO: unit tests
func (q *EtcdQDB) WriteMoveTaskGroup(ctx context.Context, id string, group *MoveTaskGroup, totalKeys int64, task *MoveTask) error {
	spqrlog.Zero.Debug().
		Str("id", id).
		Msg("etcdqdb: write task group")

	t := time.Now()

	groupJson, err := json.Marshal(group)
	if err != nil {
		return err
	}
	cmp := []clientv3.Cmp{
		clientv3.Compare(clientv3.Version(taskGroupNodePath(id)), "=", 0),
	}

	ops := []clientv3.Op{
		clientv3.OpPut(taskGroupNodePath(id), string(groupJson)),
		clientv3.OpPut(totalKeysNodePath(id), fmt.Sprintf("%d", totalKeys)),
		clientv3.OpDelete(taskGroupStopFlagNodePath(id)),
	}
	if task != nil {
		taskJson, err := json.Marshal(task)
		if err != nil {
			return err
		}

		ops = append(ops,
			clientv3.OpPut(moveTaskNodePath(task.ID), string(taskJson)),
			clientv3.OpPut(moveTaskByGroupNodePath(id), task.ID),
		)
		cmp = append(cmp, clientv3.Compare(clientv3.Version(moveTaskNodePath(task.ID)), "=", 0))
	}
	txResp, err := q.cli.Txn(ctx).If(cmp...).Then(ops...).Commit()
	if err != nil {
		return fmt.Errorf("failed to write move task group metadata: %s", err)
	}
	if !txResp.Succeeded {
		return fmt.Errorf("failed to write move task group: tx precondition failed")
	}
	statistics.RecordQDBOperation("WriteMoveTaskGroup", time.Since(t))
	return nil
}

// TODO: unit tests
func (q *EtcdQDB) GetMoveTaskGroupTotalKeys(ctx context.Context, id string) (int64, error) {
	spqrlog.Zero.Debug().
		Str("id", id).
		Msg("etcdqdb: get move task group total key count")
	resp, err := q.cli.Get(ctx, totalKeysNodePath(id))
	if err != nil {
		return -1, err
	}
	if resp.Count == 0 {
		return 0, nil
	}
	res, err := strconv.ParseInt(string(resp.Kvs[0].Value), 10, 64)
	if err != nil {
		return -1, spqrerror.Newf(spqrerror.SPQR_METADATA_CORRUPTION, "failed to convert current task index to integer: %s", resp.Kvs[0].Value)
	}
	return res, nil
}

// TODO: unit tests
func (q *EtcdQDB) UpdateMoveTaskGroupTotalKeys(ctx context.Context, id string, totalKeys int64) error {
	spqrlog.Zero.Debug().
		Str("id", id).
		Int64("count", totalKeys).
		Msg("etcdqdb: update move task group total key count")
	_, err := q.cli.Put(ctx, totalKeysNodePath(id), strconv.FormatInt(totalKeys, 10))
	return err
}

// TODO: unit tests
// TODO: drop move task
func (q *EtcdQDB) RemoveMoveTaskGroup(ctx context.Context, id string) error {
	spqrlog.Zero.Debug().
		Str("id", id).
		Msg("etcdqdb: remove task group")
	t := time.Now()

	if _, err := q.cli.Txn(ctx).Then(
		clientv3.OpDelete(taskGroupNodePath(id)),
		clientv3.OpDelete(totalKeysNodePath(id)),
		clientv3.OpDelete(taskGroupStopFlagNodePath(id)),
	).Commit(); err != nil {
		return fmt.Errorf("failed to delete move task group metadata: %s", err)
	}

	statistics.RecordQDBOperation("RemoveMoveTaskGroup", time.Since(t))
	return nil
}

func (q *EtcdQDB) GetMoveTaskByGroup(ctx context.Context, taskGroupID string) (*MoveTask, error) {
	spqrlog.Zero.Debug().
		Str("id", taskGroupID).
		Msg("etcdqdb: get move task by group")
	t := time.Now()

	path := moveTaskByGroupNodePath(taskGroupID)
	resp, err := q.cli.Get(ctx, path)
	if err != nil {
		return nil, err
	}
	switch len(resp.Kvs) {
	case 0:
		statistics.RecordQDBOperation("GetMoveTaskIDByGroup", time.Since(t))
		return nil, nil
	case 1:
		statistics.RecordQDBOperation("GetMoveTaskIDByGroup", time.Since(t))
		return q.GetMoveTask(ctx, string(resp.Kvs[0].Value))
	default:
		statistics.RecordQDBOperation("GetMoveTaskIDByGroup", time.Since(t))
		return nil, fmt.Errorf("too many values by \"%s\" key", path)
	}
}

// TODO unit test
func (q *EtcdQDB) AddMoveTaskGroupStopFlag(ctx context.Context, id string) error {
	spqrlog.Zero.Debug().
		Str("id", id).
		Msg("etcdqdb: put task group stop flag")
	t := time.Now()

	_, err := q.cli.Put(ctx, taskGroupStopFlagNodePath(id), "set")
	if err != nil {
		return err
	}
	statistics.RecordQDBOperation("AddMoveTaskGroupStopFlag", time.Since(t))
	return nil
}

// TODO unit test
func (q *EtcdQDB) CheckMoveTaskGroupStopFlag(ctx context.Context, id string) (bool, error) {
	spqrlog.Zero.Debug().
		Str("id", id).
		Msg("etcdqdb: check for task group stop flag")
	t := time.Now()

	resp, err := q.cli.Get(ctx, taskGroupStopFlagNodePath(id), clientv3.WithCountOnly())
	if err != nil {
		return false, err
	}
	statistics.RecordQDBOperation("CheckMoveTaskGroupStopFlag", time.Since(t))
	return resp.Count > 0, nil
}

// TODO unit test
func (q *EtcdQDB) WriteMoveTask(ctx context.Context, task *MoveTask) error {
	spqrlog.Zero.Debug().Str("id", task.ID).Msg("etcdqdb: write move task")

	taskJson, err := json.Marshal(task)
	if err != nil {
		return err
	}
	taskGroupID := task.TaskGroupID
	resp, err := q.cli.Txn(ctx).
		If(
			clientv3.Compare(clientv3.Version(moveTaskNodePath(task.ID)), "=", 0),
			clientv3.Compare(clientv3.Version(taskGroupNodePath(taskGroupID)), "!=", 0),
		).
		Then(
			clientv3.OpPut(moveTaskNodePath(task.ID), string(taskJson)),
			clientv3.OpPut(moveTaskByGroupNodePath(taskGroupID), task.ID),
		).
		Else(
			clientv3.OpGet(moveTaskNodePath(task.ID), clientv3.WithCountOnly()),
			clientv3.OpGet(taskGroupNodePath(taskGroupID), clientv3.WithCountOnly()),
		).
		Commit()

	if err != nil {
		return fmt.Errorf("failed to write move task: %s", err)
	}
	if !resp.Succeeded {
		if len(resp.Responses) != 2 {
			return fmt.Errorf("unexpected response count: write move task \"%s\" response parts count=%d", task.ID, len(resp.Responses))
		}
		if resp.Responses[0].GetResponseRange().Count > 0 {
			return fmt.Errorf("failed to write move task \"%s\": move task already exists", task.ID)
		}
		if resp.Responses[1].GetResponseRange().Count == 0 {
			return fmt.Errorf("failed to write move task \"%s\": task group \"%s\" does not exist", task.ID, taskGroupID)
		}
		return fmt.Errorf("failed to write move task \"%s\": tx precondition failed, reason unknown", task.ID)
	}
	return nil
}

// TODO unit test
func (q *EtcdQDB) UpdateMoveTask(ctx context.Context, task *MoveTask) error {
	spqrlog.Zero.Debug().Str("id", task.ID).Msg("etcdqdb: update move task")

	taskJson, err := json.Marshal(task)
	if err != nil {
		return err
	}
	resp, err := q.cli.Txn(ctx).
		If(clientv3.Compare(clientv3.Version(moveTaskNodePath(task.ID)), "!=", 0)).
		Then(clientv3.OpPut(moveTaskNodePath(task.ID), string(taskJson))).
		Commit()

	if err != nil {
		return fmt.Errorf("failed to update move task: %s", err)
	}
	if !resp.Succeeded {
		return fmt.Errorf("failed to update move task: IDs differ")
	}
	return nil
}

// TODO unit test
func (q *EtcdQDB) ListMoveTasks(ctx context.Context) (map[string]*MoveTask, error) {
	spqrlog.Zero.Debug().Msg("etcdqdb: list move tasks")

	resp, err := q.cli.Get(ctx, moveTaskNamespace, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}
	res := make(map[string]*MoveTask)
	for _, kv := range resp.Kvs {
		var task *MoveTask
		if err := json.Unmarshal(kv.Value, &task); err != nil {
			return nil, err
		}
		res[task.ID] = task
	}

	return res, nil
}

// TODO unit test
func (q *EtcdQDB) GetMoveTask(ctx context.Context, id string) (*MoveTask, error) {
	spqrlog.Zero.Debug().Msg("etcdqdb: get move task")

	resp, err := q.cli.Get(ctx, moveTaskNodePath(id))
	if err != nil {
		return nil, err
	}
	var task *MoveTask
	if len(resp.Kvs) == 0 {
		return nil, nil
	}
	if err := json.Unmarshal(resp.Kvs[0].Value, &task); err != nil {
		return nil, err
	}

	return task, nil
}

// TODO unit test
func (q *EtcdQDB) RemoveMoveTask(ctx context.Context, id string) error {
	spqrlog.Zero.Debug().Msg("etcdqdb: remove move task")

	task, err := q.GetMoveTask(ctx, id)
	if err != nil {
		return err
	}

	_, err = q.cli.Txn(ctx).Then(clientv3.OpDelete(moveTaskNodePath(id)), clientv3.OpDelete(moveTaskByGroupNodePath(task.TaskGroupID))).Commit()
	return err
}

// TODO: unit tests
func (q *EtcdQDB) GetRedistributeTask(ctx context.Context) (*RedistributeTask, error) {
	spqrlog.Zero.Debug().
		Msg("etcdqdb: get redistribute task")

	resp, err := q.cli.Get(ctx, redistributeTaskPath)
	if err != nil {
		return nil, err
	}

	if len(resp.Kvs) == 0 {
		return nil, nil
	}

	var task *RedistributeTask
	if err := json.Unmarshal(resp.Kvs[0].Value, &task); err != nil {
		return nil, err
	}

	return task, nil
}

// TODO: unit tests
func (q *EtcdQDB) WriteRedistributeTask(ctx context.Context, task *RedistributeTask) error {
	spqrlog.Zero.Debug().
		Msg("etcdqdb: write redistribute task")

	taskJson, err := json.Marshal(task)
	if err != nil {
		return err
	}

	_, err = q.cli.Put(ctx, redistributeTaskPath, string(taskJson))
	return err
}

// TODO: unit tests
func (q *EtcdQDB) RemoveRedistributeTask(ctx context.Context) error {
	spqrlog.Zero.Debug().
		Msg("etcdqdb: remove redistribute task")

	_, err := q.cli.Delete(ctx, redistributeTaskPath)
	return err
}

// TODO: unit tests
func (q *EtcdQDB) GetBalancerTask(ctx context.Context) (*BalancerTask, error) {
	spqrlog.Zero.Debug().
		Msg("etcdqdb: get balancer task")

	resp, err := q.cli.Get(ctx, balancerTaskPath)
	if err != nil {
		return nil, err
	}

	if len(resp.Kvs) == 0 {
		return nil, nil
	}

	var task *BalancerTask
	if err := json.Unmarshal(resp.Kvs[0].Value, &task); err != nil {
		return nil, err
	}

	return task, nil
}

// TODO: unit tests
func (q *EtcdQDB) WriteBalancerTask(ctx context.Context, task *BalancerTask) error {
	spqrlog.Zero.Debug().
		Msg("etcdqdb: write balancer task")

	taskJson, err := json.Marshal(task)
	if err != nil {
		return err
	}

	_, err = q.cli.Put(ctx, balancerTaskPath, string(taskJson))
	return err
}

// TODO: unit tests
func (q *EtcdQDB) RemoveBalancerTask(ctx context.Context) error {
	spqrlog.Zero.Debug().
		Msg("etcdqdb: remove balancer task")

	_, err := q.cli.Delete(ctx, balancerTaskPath)
	return err
}

// ==============================================================================
//                              KEY RANGE MOVES
// ==============================================================================

// TODO : unit tests
func (q *EtcdQDB) ListKeyRangeMoves(ctx context.Context) ([]*MoveKeyRange, error) {
	spqrlog.Zero.Debug().Msg("etcdqdb: list move key range operations")
	t := time.Now()

	resp, err := q.cli.Get(ctx, keyRangeMovesNamespace, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}

	moves := make([]*MoveKeyRange, 0, len(resp.Kvs))

	for _, kv := range resp.Kvs {
		// XXX: multi-column routing schemas
		// A sharding rule currently supports only one column
		var rule *MoveKeyRange
		err := json.Unmarshal(kv.Value, &rule)
		if err != nil {
			return nil, err
		}

		moves = append(moves, rule)
	}

	spqrlog.Zero.Debug().
		Interface("response", resp).
		Msg("etcdqdb: list move key range operations")
	statistics.RecordQDBOperation("ListKeyRangeMoves", time.Since(t))
	return moves, nil
}

// TODO : unit tests
func (q *EtcdQDB) RecordKeyRangeMove(ctx context.Context, m *MoveKeyRange) error {
	spqrlog.Zero.Debug().
		Str("id", m.MoveId).
		Msg("etcdqdb: add move key range operation")
	t := time.Now()

	rawMoveKeyRange, err := json.Marshal(m)

	if err != nil {
		return err
	}
	resp, err := q.cli.Put(ctx, keyRangeMovesNodePath(m.MoveId), string(rawMoveKeyRange))
	if err != nil {
		return err
	}

	spqrlog.Zero.Debug().
		Interface("response", resp).
		Msg("etcdqdb: add move key range operation")

	statistics.RecordQDBOperation("RecordKeyRangeMove", time.Since(t))
	return nil
}

// TODO : unit tests
func (q *EtcdQDB) UpdateKeyRangeMoveStatus(ctx context.Context, moveId string, s MoveKeyRangeStatus) error {
	spqrlog.Zero.Debug().
		Str("id", moveId).
		Msg("etcdqdb: update key range move status")
	t := time.Now()

	resp, err := q.cli.Get(ctx, keyRangeMovesNodePath(moveId))
	if err != nil {
		return err
	}
	var moveKr MoveKeyRange
	if err := json.Unmarshal(resp.Kvs[0].Value, &moveKr); err != nil {
		return err
	}
	moveKr.Status = s
	rawMoveKeyRange, err := json.Marshal(moveKr)

	if err != nil {
		return err
	}
	respModify, err := q.cli.Put(ctx, keyRangeMovesNodePath(moveKr.MoveId), string(rawMoveKeyRange))
	if err != nil {
		return err
	}

	spqrlog.Zero.Debug().
		Interface("response", respModify).
		Msg("etcdqdb: update status of move key range operation")

	statistics.RecordQDBOperation("RecordKeyRangeMove", time.Since(t))
	return nil
}

func (q *EtcdQDB) DeleteKeyRangeMove(ctx context.Context, moveId string) error {
	spqrlog.Zero.Debug().
		Str("id", moveId).
		Msg("etcdqdb: delete key range move")
	t := time.Now()

	resp, err := q.cli.Get(ctx, keyRangeMovesNodePath(moveId))
	if err != nil {
		return err
	}
	var moveKr MoveKeyRange
	if err := json.Unmarshal(resp.Kvs[0].Value, &moveKr); err != nil {
		return err
	}
	if moveKr.Status != MoveKeyRangeComplete {
		return fmt.Errorf("cannot remove non-completed key range move")
	}
	_, err = q.cli.Delete(ctx, keyRangeMovesNodePath(moveId))

	statistics.RecordQDBOperation("DeleteKeyRangeMove", time.Since(t))
	return err
}

func (q *EtcdQDB) AlterSequenceAttach(ctx context.Context, seqName string, relName *rfqn.RelationFQN, colName string) error {
	spqrlog.Zero.Debug().
		Str("column", colName).
		Msg("etcdqdb: attach column to sequence")

	resp, err := q.cli.Put(ctx, columnSequenceMappingNodePath(relName.RelationName, colName), seqName)
	spqrlog.Zero.Debug().
		Interface("response", resp).
		Msg("etcdqdb: attach column to sequence")
	return err
}

func (q *EtcdQDB) AlterSequenceDetachRelation(ctx context.Context, relName *rfqn.RelationFQN) error {
	spqrlog.Zero.Debug().
		Str("relation", relName.RelationName).
		Msg("etcdqdb: detach relation from sequence")

	resp, err := q.cli.Delete(ctx, relationSequenceMappingNodePath(relName.RelationName), clientv3.WithPrefix())
	spqrlog.Zero.Debug().
		Interface("response", resp).
		Msg("etcdqdb: detach relation from sequence")
	return err
}

func (q *EtcdQDB) GetRelationSequence(ctx context.Context, relName *rfqn.RelationFQN) (map[string]string, error) {
	spqrlog.Zero.Debug().
		Str("relName", relName.RelationName).
		Msg("etcdqdb: get column sequence")

	key := relationSequenceMappingNodePath(relName.RelationName)
	resp, err := q.cli.Get(ctx, key, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}

	ret := map[string]string{}
	for _, kv := range resp.Kvs {
		_, colName := path.Split(string(kv.Key))
		ret[colName] = string(kv.Value)
	}

	spqrlog.Zero.Debug().
		Interface("response", ret).
		Msg("etcdqdb: get column sequence")

	return ret, nil
}

func (q *EtcdQDB) GetSequenceRelations(ctx context.Context, seqName string) ([]*rfqn.RelationFQN, error) {
	spqrlog.Zero.Debug().
		Str("seqName", seqName).
		Msg("etcdqdb: get columns attached to a sequence")
	resp, err := q.cli.Get(ctx, columnSequenceMappingNamespace, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}

	rels := []*rfqn.RelationFQN{}
	for _, kv := range resp.Kvs {
		if string(kv.Value) != seqName {
			continue
		}

		s := strings.Split(string(kv.Key), "/")
		relName := s[len(s)-2]
		rels = append(rels, &rfqn.RelationFQN{RelationName: relName})
	}

	return rels, nil
}

func (q *EtcdQDB) CreateSequence(ctx context.Context, seqName string, initialValue int64) error {
	spqrlog.Zero.Debug().
		Str("sequence", seqName).
		Msg("etcdqdb: add sequence")

	key := sequenceNodePath(seqName)
	resp, err := q.cli.Get(ctx, key)
	if err != nil {
		return err
	}

	if len(resp.Kvs) == 0 {
		_, err := q.cli.Put(ctx, key, fmt.Sprintf("%d", initialValue))
		if err != nil {
			return err
		}
	}

	return nil
}

func (q *EtcdQDB) DropSequence(ctx context.Context, seqName string, force bool) error {
	spqrlog.Zero.Debug().
		Str("sequence", seqName).
		Bool("force", force).
		Msg("etcdqdb: drop sequence")

	key := sequenceNodePath(seqName)
	_, err := q.cli.Delete(ctx, key)
	return err
}

func (q *EtcdQDB) ListSequences(ctx context.Context) ([]string, error) {
	spqrlog.Zero.Debug().Msg("etcdqdb: list all sequences")

	resp, err := q.cli.Get(ctx, sequenceNamespace, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}

	var ret []string

	for _, e := range resp.Kvs {
		ret = append(ret, strings.TrimPrefix(string(e.Key), sequenceNamespace))
	}

	sort.Strings(ret)

	spqrlog.Zero.Debug().
		Interface("response", resp).
		Msg("etcdqdb: list all sequences")

	return ret, nil
}

func (q *EtcdQDB) NextRange(ctx context.Context, seqName string, rangeSize uint64) (*SequenceIdRange, error) {
	spqrlog.Zero.Debug().Str("seqName", seqName).Uint64("size", rangeSize).Msg("etcdqdb: next id ranges")

	id := sequenceNodePath(seqName)
	sess, err := concurrency.NewSession(q.cli)
	if err != nil {
		return nil, err
	}
	defer closeSession(sess)

	mu := concurrency.NewMutex(sess, sequenceSpace)
	if err = mu.Lock(ctx); err != nil {
		return nil, err
	}
	defer unlockMutex(mu, ctx)

	resp, err := q.cli.Get(ctx, id)
	if err != nil {
		return nil, err
	}

	var nextval int64 = 0
	switch resp.Count {
	case 1:
		var err error
		nextval, err = strconv.ParseInt(string(resp.Kvs[0].Value), 10, 64)
		if err != nil {
			return nil, err
		}
	default:
	}

	nextval++

	if idRange, err := NewRangeBySize(nextval, rangeSize); err != nil {
		return nil, fmt.Errorf("invalid id-range request: current=%d, request for=%d", nextval, rangeSize)
	} else {
		_, err = q.cli.Put(ctx, id, fmt.Sprintf("%d", idRange.Right))
		return idRange, err
	}
}

func (q *EtcdQDB) CurrVal(ctx context.Context, seqName string) (int64, error) {
	spqrlog.Zero.Debug().Str("seqName", seqName).Msg("etcdqdb: curr val")

	id := sequenceNodePath(seqName)
	sess, err := concurrency.NewSession(q.cli)
	if err != nil {
		return -1, err
	}
	defer closeSession(sess)

	resp, err := q.cli.Get(ctx, id)
	if err != nil {
		return -1, err
	}

	var nextval int64 = 0
	switch resp.Count {
	case 1:
		var err error
		nextval, err = strconv.ParseInt(string(resp.Kvs[0].Value), 10, 64)
		if err != nil {
			return -1, err
		}
	default:
	}

	return nextval, err
}

func packEtcdCommands(operations []QdbStatement) ([]clientv3.Op, error) {
	etcdOperations := make([]clientv3.Op, len(operations))
	for index, v := range operations {
		switch v.CmdType {
		case CMD_PUT:
			etcdOperations[index] = clientv3.OpPut(v.Key, v.Value)
		case CMD_DELETE:
			etcdOperations[index] = clientv3.OpDelete(v.Key)
		default:
			return nil, fmt.Errorf("not found operation type: %d", v.CmdType)
		}
	}
	return etcdOperations, nil
}

func (q *EtcdQDB) ExecNoTransaction(ctx context.Context, operations []QdbStatement) error {
	etcdOperations, err := packEtcdCommands(operations)
	if err != nil {
		return err
	}
	_, err = q.cli.Txn(ctx).Then(etcdOperations...).Commit()
	return err
}

func (q *EtcdQDB) CommitTransaction(ctx context.Context, transaction *QdbTransaction) error {
	if transaction == nil {
		return fmt.Errorf("cant't commit empty transaction")
	}
	if err := transaction.Validate(); err != nil {
		return fmt.Errorf("invalid transaction %s: %w", transaction.Id(), err)
	}
	etcdOperations, err := packEtcdCommands(transaction.commands)
	if err != nil {
		return err
	}
	etcdOperations = append(etcdOperations, clientv3.OpDelete(transactionRequest))
	resp, err := q.cli.Txn(ctx).
		If(clientv3.Compare(clientv3.Value(transactionRequest), "=", transaction.transactionId.String())).
		Then(etcdOperations...).
		Commit()

	if err != nil {
		return fmt.Errorf("failed to commit transaction: %s", transaction.Id())
	}
	if !resp.Succeeded {
		return fmt.Errorf("transaction '%s' can't be committed", transaction.Id())
	}
	return nil
}

func (q *EtcdQDB) BeginTransaction(ctx context.Context, transaction *QdbTransaction) error {
	if transaction == nil {
		return fmt.Errorf("cant't begin empty transaction")
	}
	_, err := q.cli.Put(ctx, transactionRequest, transaction.transactionId.String())
	return err
}
