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

	"github.com/pg-sharding/spqr/pkg/spqrlog"

	retry "github.com/sethvargo/go-retry"
)

type EtcdQDB struct {
	cli *clientv3.Client
}

var _ XQDB = &EtcdQDB{}

func NewEtcdQDB(addr string) (*EtcdQDB, error) {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints: []string{addr},
		DialOptions: []grpc.DialOption{ // TODO remove WithInsecure
			grpc.WithInsecure(), //nolint:all
		},
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
	keyRangesNamespace             = "/keyranges/"
	distributionNamespace          = "/distributions/"
	keyRangeMovesNamespace         = "/krmoves/"
	routersNamespace               = "/routers/"
	shardsNamespace                = "/shards/"
	relationMappingNamespace       = "/relation_mappings/"
	taskGroupPath                  = "/move_task_group"
	moveTasksNamespace             = "/move_tasks/"
	currentTaskIndexPath           = "/current_task_index"
	moveTasksCountPath             = "/total_move_tasks"
	redistributeTaskPath           = "/redistribute_task/"
	balancerTaskPath               = "/balancer_task/"
	transactionNamespace           = "/transfer_txs/"
	sequenceNamespace              = "/sequences/"
	referenceRelationsNamespace    = "/reference_relations"
	columnSequenceMappingNamespace = "/column_sequence_mappings/"

	CoordKeepAliveTtl = 3
	keyspace          = "key_space"
	coordLockKey      = "coordinator_exists"
	sequenceSpace     = "sequence_space"
)

func keyLockPath(key string) string {
	return path.Join("/lock", key)
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

func moveTaskNodePath(id string) string {
	return path.Join(moveTasksNamespace, id)
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

	rawKeyRange, err := json.Marshal(keyRange)

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

	statistics.RecordQDBOperation("CreateKeyRange", time.Since(t))
	return err
}

// TODO : unit tests
func (q *EtcdQDB) fetchKeyRange(ctx context.Context, nodePath string) (*KeyRange, error) {
	// caller ensures key is locked
	raw, err := q.cli.Get(ctx, nodePath)
	if err != nil {
		return nil, err
	}

	switch len(raw.Kvs) {
	case 0:
		return nil, spqrerror.Newf(spqrerror.SPQR_KEYRANGE_ERROR, "no key range found at %v", nodePath)

	case 1:
		ret := KeyRange{}
		if err := json.Unmarshal(raw.Kvs[0].Value, &ret); err != nil {
			return nil, err
		}
		return &ret, nil

	default:
		return nil, spqrerror.Newf(spqrerror.SPQR_METADATA_CORRUPTION, "possible data corruption: multiple key-value pairs found for %v", nodePath)
	}
}

// TODO : unit tests
func (q *EtcdQDB) GetKeyRange(ctx context.Context, id string) (*KeyRange, error) {
	spqrlog.Zero.Debug().
		Str("id", id).
		Msg("etcdqdb: get key range")

	t := time.Now()

	ret, err := q.fetchKeyRange(ctx, keyRangeNodePath(id))

	spqrlog.Zero.Debug().
		Interface("ret", ret).
		Msg("etcdqdb: get key range")
	statistics.RecordQDBOperation("GetKeyRange", time.Since(t))
	return ret, err
}

// TODO : unit tests
func (q *EtcdQDB) UpdateKeyRange(ctx context.Context, keyRange *KeyRange) error {
	spqrlog.Zero.Debug().
		Interface("key-range", keyRange).
		Msg("etcdqdb: update key range")

	t := time.Now()

	rawKeyRange, err := json.Marshal(keyRange)
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

	t := time.Now()

	resp, err := q.cli.Get(ctx, keyRangesNamespace, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}

	keyRanges := make([]*KeyRange, 0, len(resp.Kvs))

	for _, e := range resp.Kvs {
		var kr *KeyRange
		if err := json.Unmarshal(e.Value, &kr); err != nil {
			return nil, err
		}

		if distribution == kr.DistributionId {
			keyRanges = append(keyRanges, kr)
		}
	}

	sort.Slice(keyRanges, func(i, j int) bool {
		return keyRanges[i].KeyRangeID < keyRanges[j].KeyRangeID
	})

	spqrlog.Zero.Debug().
		Interface("response", resp).
		Str("distribution", distribution).
		Msg("etcdqdb: list key ranges")

	statistics.RecordQDBOperation("ListKeyRanges", time.Since(t))
	return keyRanges, nil
}

// TODO : unit tests
func (q *EtcdQDB) ListAllKeyRanges(ctx context.Context) ([]*KeyRange, error) {
	spqrlog.Zero.Debug().Msg("etcdqdb: list all key ranges")

	t := time.Now()

	resp, err := q.cli.Get(ctx, keyRangesNamespace, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}

	var ret []*KeyRange

	for _, e := range resp.Kvs {
		var krCurr KeyRange

		if err := json.Unmarshal(e.Value, &krCurr); err != nil {
			return nil, err
		}
		ret = append(ret, &krCurr)
	}

	sort.Slice(ret, func(i, j int) bool {
		return ret[i].KeyRangeID < ret[j].KeyRangeID
	})

	spqrlog.Zero.Debug().
		Interface("response", resp).
		Msg("etcdqdb: list all key ranges")

	statistics.RecordQDBOperation("ListAllKeyRanges", time.Since(t))
	return ret, nil
}

// TODO : unit tests
func (q *EtcdQDB) LockKeyRange(ctx context.Context, id string) (*KeyRange, error) {
	spqrlog.Zero.Debug().
		Str("id", id).
		Msg("etcdqdb: lock key range")

	t := time.Now()

	if err := retry.Do(ctx, retry.WithMaxRetries(7, retry.NewFibonacci(500*time.Millisecond)), func(ctx context.Context) error {
		resp, err := q.cli.Get(ctx, keyLockPath(keyRangeNodePath(id)), clientv3.WithCountOnly())
		if err != nil {
			return retry.RetryableError(err)
		}
		switch resp.Count {
		case 0:
			_, err := q.cli.Put(ctx, keyLockPath(keyRangeNodePath(id)), "locked")
			if err != nil {
				return retry.RetryableError(err)
			}

			return nil
		case 1:
			return retry.RetryableError(spqrerror.Newf(spqrerror.SPQR_KEYRANGE_ERROR, "key range %v is locked", id))
		default:
			return spqrerror.Newf(spqrerror.SPQR_KEYRANGE_ERROR, "too much key ranges matched: %d", len(resp.Kvs))
		}
	}); err != nil {
		statistics.RecordQDBOperation("LockKeyRange", time.Since(t))
		return nil, err
	}
	statistics.RecordQDBOperation("LockKeyRange", time.Since(t))
	return q.GetKeyRange(ctx, id)
}

// TODO : unit tests
func (q *EtcdQDB) UnlockKeyRange(ctx context.Context, id string) error {
	spqrlog.Zero.Debug().
		Str("id", id).
		Msg("etcdqdb: unlock key range")

	t := time.Now()

	err := retry.Do(ctx, retry.NewFibonacci(500*time.Millisecond), func(ctx context.Context) error {
		resp, err := q.cli.Get(ctx, keyLockPath(keyRangeNodePath(id)), clientv3.WithCountOnly())
		if err != nil {
			return retry.RetryableError(err)
		}
		switch resp.Count {
		case 0:
			return spqrerror.Newf(spqrerror.SPQR_KEYRANGE_ERROR, "key range with id %v unlocked", id)
		case 1:
			_, err := q.cli.Delete(ctx, keyLockPath(keyRangeNodePath(id)))
			if err != nil {
				return retry.RetryableError(err)
			}
			return nil
		default:
			return spqrerror.Newf(spqrerror.SPQR_KEYRANGE_ERROR, "too much key ranges matched: %d", len(resp.Kvs))
		}
	})
	statistics.RecordQDBOperation("UnlockKeyRange", time.Since(t))
	return err
}

// TODO : unit tests
func (q *EtcdQDB) CheckLockedKeyRange(ctx context.Context, id string) (*KeyRange, error) {
	spqrlog.Zero.Debug().
		Str("id", id).
		Msg("etcdqdb: check locked key range")

	t := time.Now()

	resp, err := q.cli.Get(ctx, keyLockPath(keyRangeNodePath(id)))
	if err != nil {
		return nil, err
	}

	switch len(resp.Kvs) {
	case 0:
		statistics.RecordQDBOperation("CheckLockedKeyRange", time.Since(t))
		return nil, spqrerror.Newf(spqrerror.SPQR_KEYRANGE_ERROR, "key range %v not locked", id)
	case 1:
		statistics.RecordQDBOperation("CheckLockedKeyRange", time.Since(t))
		return q.GetKeyRange(ctx, id)
	default:
		return nil, spqrerror.Newf(spqrerror.SPQR_KEYRANGE_ERROR, "too much key ranges matched: %d", len(resp.Kvs))
	}
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

	if _, err = q.cli.Delete(ctx, keyRangeNodePath(krId)); err != nil {
		return err
	}

	_, err = q.cli.Delete(ctx, keyLockPath(keyRangeNodePath(krId)))
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
	resp, err := q.cli.Delete(ctx, routerNodePath(id), clientv3.WithPrefix())
	if err != nil {
		return err
	}

	spqrlog.Zero.Debug().
		Interface("response", resp).
		Msg("etcdqdb: drop router")

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
	resp, err := q.cli.Put(ctx, shardNodePath(shard.ID), string(bytes))
	if err != nil {
		return err
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

// TODO : unit tests
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
		return nil, spqrerror.Newf(spqrerror.SPQR_NO_DATASHARD, "shard \"%s\" not found", id)
	}

	shardInfo := &Shard{
		ID: id,
	}

	for _, shard := range resp.Kvs {
		// The Port field is always for a while.
		shardInfo.RawHosts = append(shardInfo.RawHosts, string(shard.Value))
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
		return nil, spqrerror.Newf(spqrerror.SPQR_OBJECT_NOT_EXIST, "distribution \"%s\" not found", tableName)
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

	resp, err := q.cli.Get(ctx, nodePath, clientv3.WithPrefix())
	if err != nil {
		return err
	}

	switch len(resp.Kvs) {
	case 0:
		return spqrerror.New(spqrerror.SPQR_SHARDING_RULE_ERROR, "no such reference relation present in qdb")
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
		return spqrerror.Newf(spqrerror.SPQR_SHARDING_RULE_ERROR, "too much reference relations matched: %d", len(resp.Kvs))
	}
}

// DropReferenceRelation implements XQDB.
func (q *EtcdQDB) DropReferenceRelation(ctx context.Context, relName *rfqn.RelationFQN) error {
	tableName := relName.RelationName
	spqrlog.Zero.Debug().
		Str("tablename", tableName).
		Msg("etcdqdb: drop reference relation")

	nodePath := referenceRelationNodePath(tableName)

	resp, err := q.cli.Get(ctx, nodePath, clientv3.WithPrefix())
	if err != nil {
		return err
	}

	switch len(resp.Kvs) {
	case 0:
		return spqrerror.New(spqrerror.SPQR_SHARDING_RULE_ERROR, "no such reference relation present in qdb")
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
		return spqrerror.Newf(spqrerror.SPQR_SHARDING_RULE_ERROR, "too much reference relations matched: %d", len(resp.Kvs))
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
func (q *EtcdQDB) CreateDistribution(ctx context.Context, distribution *Distribution) error {
	spqrlog.Zero.Debug().
		Str("id", distribution.ID).
		Msg("etcdqdb: add distribution")

	distrJson, err := json.Marshal(distribution)
	if err != nil {
		return err
	}
	resp, err := q.cli.Put(ctx, distributionNodePath(distribution.ID), string(distrJson))
	if err != nil {
		return err
	}

	spqrlog.Zero.Debug().
		Interface("response", resp).
		Msg("etcdqdb: add distribution")

	return nil
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

	resp, err := q.cli.Get(ctx, distributionNodePath(id), clientv3.WithPrefix())
	if err != nil {
		return err
	}

	switch len(resp.Kvs) {
	case 0:
		return spqrerror.New(spqrerror.SPQR_SHARDING_RULE_ERROR, "no such distribution present in qdb")
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
		return spqrerror.Newf(spqrerror.SPQR_SHARDING_RULE_ERROR, "too much distributions matched: %d", len(resp.Kvs))
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

	err = q.CreateDistribution(ctx, distribution)

	return err
}

// TODO: unit tests
func (q *EtcdQDB) AlterDistributionDetach(ctx context.Context, id string, relName *rfqn.RelationFQN) error {
	spqrlog.Zero.Debug().
		Str("id", id).
		Msg("etcdqdb: detach table from distribution")

	distribution, err := q.GetDistribution(ctx, id)
	if err != nil {
		return err
	}

	if err := q.AlterSequenceDetachRelation(ctx, relName); err != nil {
		return err
	}

	delete(distribution.Relations, relName.RelationName)
	if err = q.CreateDistribution(ctx, distribution); err != nil {
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

	err = q.CreateDistribution(ctx, distribution)

	return err
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
//                                    TASKS
// ==============================================================================

// TODO: unit tests
func (q *EtcdQDB) GetMoveTaskGroup(ctx context.Context) (*MoveTaskGroup, error) {
	spqrlog.Zero.Debug().
		Msg("etcdqdb: get task group")

	t := time.Now()

	resp, err := q.cli.Get(ctx, taskGroupPath)
	if err != nil {
		return nil, err
	}

	if len(resp.Kvs) == 0 {
		return &MoveTaskGroup{
			TaskIDs: []string{},
		}, nil
	}

	var taskGroup *MoveTaskGroup
	if err := json.Unmarshal(resp.Kvs[0].Value, &taskGroup); err != nil {
		return nil, err
	}

	resp, err = q.cli.Get(ctx, currentTaskIndexPath)
	if err != nil {
		return nil, err
	}
	taskGroup.CurrentTaskInd, err = strconv.Atoi(string(resp.Kvs[0].Value))
	if err != nil {
		return nil, spqrerror.Newf(spqrerror.SPQR_METADATA_CORRUPTION, "failed to convert current task index to integer: %s", resp.Kvs[0].Value)
	}

	statistics.RecordQDBOperation("GetMoveTaskGroup", time.Since(t))
	return taskGroup, nil
}

// TODO: unit tests
func (q *EtcdQDB) WriteMoveTaskGroup(ctx context.Context, group *MoveTaskGroup) error {
	spqrlog.Zero.Debug().
		Msg("etcdqdb: write task group")

	t := time.Now()

	groupJson, err := json.Marshal(group)
	if err != nil {
		return err
	}

	if _, err = q.cli.Put(ctx, taskGroupPath, string(groupJson)); err != nil {
		return err
	}
	if _, err = q.cli.Put(ctx, moveTasksCountPath, fmt.Sprintf("%d", len(group.TaskIDs))); err != nil {
		return err
	}
	_, err = q.cli.Put(ctx, currentTaskIndexPath, fmt.Sprintf("%d", group.CurrentTaskInd))
	statistics.RecordQDBOperation("WriteMoveTaskGroup", time.Since(t))
	return err
}

// TODO: unit tests
func (q *EtcdQDB) UpdateMoveTaskGroupSetCurrentTask(ctx context.Context, taskIndex int) error {
	_, err := q.cli.Put(ctx, currentTaskIndexPath, fmt.Sprintf("%d", taskIndex))
	return err
}

// TODO: unit tests
func (q *EtcdQDB) GetCurrentMoveTaskIndex(ctx context.Context) (int, error) {
	resp, err := q.cli.Get(ctx, currentTaskIndexPath)
	if err != nil {
		return -1, err
	}
	res, err := strconv.Atoi(string(resp.Kvs[0].Value))
	if err != nil {
		return -1, spqrerror.Newf(spqrerror.SPQR_METADATA_CORRUPTION, "failed to convert current task index to integer: %s", resp.Kvs[0].Value)
	}
	return res, nil
}

// TODO: unit tests
func (q *EtcdQDB) RemoveMoveTaskGroup(ctx context.Context) error {
	spqrlog.Zero.Debug().
		Msg("etcdqdb: remove task group")
	t := time.Now()

	if _, err := q.cli.Delete(ctx, currentTaskIndexPath); err != nil {
		return err
	}
	if _, err := q.cli.Delete(ctx, moveTasksCountPath); err != nil {
		return err
	}
	_, err := q.cli.Delete(ctx, taskGroupPath)
	statistics.RecordQDBOperation("RemoveMoveTaskGroup", time.Since(t))
	return err
}

func (q *EtcdQDB) CreateMoveTask(ctx context.Context, task *MoveTask) error {
	spqrlog.Zero.Debug().Str("id", task.ID).Msg("etcdqdb: write move task")

	res, err := q.cli.Get(ctx, moveTaskNodePath(task.ID), clientv3.WithCountOnly())
	if err != nil {
		return err
	}
	if res.Count != 0 {
		return spqrerror.Newf(spqrerror.SPQR_METADATA_CORRUPTION, "move task \"%s\" already exists", task.ID)
	}

	taskJson, err := json.Marshal(task)
	if err != nil {
		return err
	}

	_, err = q.cli.Put(ctx, moveTaskNodePath(task.ID), string(taskJson))
	return err
}

func (q *EtcdQDB) UpdateMoveTask(ctx context.Context, task *MoveTask) error {
	spqrlog.Zero.Debug().Str("id", task.ID).Msg("etcdqdb: write move task")

	res, err := q.cli.Get(ctx, moveTaskNodePath(task.ID), clientv3.WithCountOnly())
	if err != nil {
		return err
	}
	if res.Count == 0 {
		return spqrerror.Newf(spqrerror.SPQR_METADATA_CORRUPTION, "move task \"%s\" not found", task.ID)
	}

	taskJson, err := json.Marshal(task)
	if err != nil {
		return err
	}

	_, err = q.cli.Put(ctx, moveTaskNodePath(task.ID), string(taskJson))
	return err
}

func (q *EtcdQDB) GetMoveTask(ctx context.Context, id string) (*MoveTask, error) {
	spqrlog.Zero.Debug().Str("id", id).Msg("etcdqdb: get move task")

	resp, err := q.cli.Get(ctx, moveTaskNodePath(id))
	if err != nil {
		return nil, err
	}
	var task *MoveTask
	if err := json.Unmarshal(resp.Kvs[0].Value, &task); err != nil {
		return nil, err
	}

	return task, nil
}

func (q *EtcdQDB) RemoveMoveTask(ctx context.Context, id string) error {
	spqrlog.Zero.Debug().Str("id", id).Msg("etcdqdb: remove move task")

	_, err := q.cli.Delete(ctx, moveTaskNodePath(id))
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

func (q *EtcdQDB) getSequenceColumns(ctx context.Context, seqName string) ([]string, error) {
	resp, err := q.cli.Get(ctx, columnSequenceMappingNamespace, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}

	cols := []string{}
	for _, kv := range resp.Kvs {
		if string(kv.Value) != seqName {
			continue
		}

		s := strings.Split(string(kv.Key), "/")
		colName := s[len(s)-1]
		relName := s[len(s)-2]
		cols = append(cols, fmt.Sprintf("%s.%s", relName, colName))
	}

	return cols, nil
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

func (q *EtcdQDB) DropSequence(ctx context.Context, seqName string) error {
	depends, err := q.getSequenceColumns(ctx, seqName)
	if err != nil {
		return err
	}
	if len(depends) != 0 {
		return spqrerror.Newf(spqrerror.SPQR_SEQUENCE_ERROR, "column %q is attached to sequence", depends[0])
	}

	key := sequenceNodePath(seqName)
	_, err = q.cli.Delete(ctx, key)
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

func (q *EtcdQDB) NextVal(ctx context.Context, seqName string) (int64, error) {
	spqrlog.Zero.Debug().Msg("etcdqdb: next val")

	id := sequenceNodePath(seqName)
	sess, err := concurrency.NewSession(q.cli)
	if err != nil {
		return -1, err
	}
	defer closeSession(sess)

	mu := concurrency.NewMutex(sess, sequenceSpace)
	if err = mu.Lock(ctx); err != nil {
		return -1, err
	}
	defer unlockMutex(mu, ctx)

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

	nextval++
	_, err = q.cli.Put(ctx, id, fmt.Sprintf("%d", nextval))

	return nextval, err
}

func (q *EtcdQDB) CurrVal(ctx context.Context, seqName string) (int64, error) {
	spqrlog.Zero.Debug().Msg("etcdqdb: curr val")

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
