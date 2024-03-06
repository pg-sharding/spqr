package qdb

import (
	"context"
	"encoding/json"
	"fmt"
	"path"
	"sort"
	"sync"
	"time"

	"github.com/pg-sharding/spqr/pkg/models/spqrerror"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/clientv3util"
	"go.etcd.io/etcd/client/v3/concurrency"
	"google.golang.org/grpc"

	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/spqrlog"
)

type EtcdQDB struct {
	cli *clientv3.Client
	mu  sync.Mutex
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
	keyRangesNamespace       = "/keyranges/"
	distributionNamespace    = "/distributions/"
	keyRangeMovesNamespace   = "/krmoves/"
	routersNamespace         = "/routers/"
	shardsNamespace          = "/shards/"
	relationMappingNamespace = "/relation_mappings/"

	CoordKeepAliveTtl = 3
	keyspace          = "key_space"
	coordLockKey      = "coordinator_exists"
)

func keyLockPath(key string) string {
	return path.Join("lock", key)
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

func relationMappingNodePath(key string) string {
	return path.Join(relationMappingNamespace, key)
}

func keyRangeMovesNodePath(key string) string {
	return path.Join(keyRangeMovesNamespace, key)
}

// ==============================================================================
//                                 KEY RANGES
// ==============================================================================

// TODO : unit tests
func (q *EtcdQDB) AddKeyRange(ctx context.Context, keyRange *KeyRange) error {
	spqrlog.Zero.Debug().
		Bytes("lower-bound", keyRange.LowerBound).
		Str("shard-id", keyRange.ShardID).
		Str("distribution-id", keyRange.DistributionId).
		Str("key-range-id", keyRange.KeyRangeID).
		Msg("etcdqdb: add key range")

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
	case 1:
		ret := KeyRange{}
		if err := json.Unmarshal(raw.Kvs[0].Value, &ret); err != nil {
			return nil, err
		}
		return &ret, nil

	default:
		return nil, spqrerror.Newf(spqrerror.SPQR_KEYRANGE_ERROR, "failed to fetch key range at %v", nodePath)
	}
}

// TODO : unit tests
func (q *EtcdQDB) GetKeyRange(ctx context.Context, id string) (*KeyRange, error) {
	spqrlog.Zero.Debug().
		Str("id", id).
		Msg("etcdqdb: get key range")

	ret, err := q.fetchKeyRange(ctx, keyRangeNodePath(id))

	spqrlog.Zero.Debug().
		Interface("ret", ret).
		Msg("etcdqdb: get key range")
	return ret, err
}

// TODO : unit tests
func (q *EtcdQDB) UpdateKeyRange(ctx context.Context, keyRange *KeyRange) error {
	spqrlog.Zero.Debug().
		Bytes("lower-bound", keyRange.LowerBound).
		Str("shard-id", keyRange.ShardID).
		Str("distribution-id", keyRange.KeyRangeID).
		Str("key-range-id", keyRange.KeyRangeID).
		Msg("etcdqdb: add key range")

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

	resp, err := q.cli.Delete(ctx, keyRangeNodePath(id))

	spqrlog.Zero.Debug().
		Interface("response", resp).
		Msg("etcdqdb: drop key range")

	return err
}

// TODO : unit tests
func (q *EtcdQDB) ListKeyRanges(ctx context.Context, distribution string) ([]*KeyRange, error) {
	spqrlog.Zero.Debug().
		Str("distribution", distribution).
		Msg("etcdqdb: list key ranges")

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

	return keyRanges, nil
}

// TODO : unit tests
func (q *EtcdQDB) ListAllKeyRanges(ctx context.Context) ([]*KeyRange, error) {
	spqrlog.Zero.Debug().Msg("etcdqdb: list all key ranges")

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

	return ret, nil
}

// TODO : unit tests
func (q *EtcdQDB) LockKeyRange(ctx context.Context, id string) (*KeyRange, error) {
	spqrlog.Zero.Debug().
		Str("id", id).
		Msg("etcdqdb: lock key range")

	q.mu.Lock()
	defer q.mu.Unlock()

	sess, err := concurrency.NewSession(q.cli)
	if err != nil {
		return nil, err
	}
	defer closeSession(sess)

	fetcher := func(ctx context.Context, sess *concurrency.Session, keyRangeID string) (*KeyRange, error) {
		mu := concurrency.NewMutex(sess, keyspace)
		if err = mu.Lock(ctx); err != nil {
			return nil, err
		}
		defer unlockMutex(mu, ctx)

		resp, err := q.cli.Get(ctx, keyLockPath(keyRangeNodePath(keyRangeID)))
		if err != nil {
			return nil, err
		}
		switch len(resp.Kvs) {
		case 0:
			_, err := q.cli.Put(ctx, keyLockPath(keyRangeNodePath(keyRangeID)), "locked")
			if err != nil {
				return nil, err
			}

			return q.GetKeyRange(ctx, keyRangeID)
		case 1:
			return nil, spqrerror.Newf(spqrerror.SPQR_KEYRANGE_ERROR, "key range with id %v locked", keyRangeID)
		default:
			return nil, spqrerror.Newf(spqrerror.SPQR_KEYRANGE_ERROR, "too much key ranges matched: %d", len(resp.Kvs))
		}
	}

	timer := time.NewTimer(time.Second)

	fetchCtx, cf := context.WithTimeout(ctx, 15*time.Second)
	defer cf()

	for {
		select {
		case <-timer.C:
			val, err := fetcher(ctx, sess, id)
			if err != nil {
				spqrlog.Zero.Error().
					Err(err).
					Msg("error while fetching")
				continue
			}

			return val, nil

		case <-fetchCtx.Done():
			return nil, spqrerror.New(spqrerror.SPQR_KEYRANGE_ERROR, "lock key range deadlines exceeded")
		}
	}
}

// TODO : unit tests
func (q *EtcdQDB) UnlockKeyRange(ctx context.Context, id string) error {
	spqrlog.Zero.Debug().
		Str("id", id).
		Msg("etcdqdb: unlock key range")

	q.mu.Lock()
	defer q.mu.Unlock()

	sess, err := concurrency.NewSession(q.cli)
	if err != nil {
		return err
	}
	defer closeSession(sess)

	unlocker := func(ctx context.Context, sess *concurrency.Session, keyRangeID string) error {
		mu := concurrency.NewMutex(sess, keyspace)
		if err = mu.Lock(ctx); err != nil {
			return err
		}
		defer unlockMutex(mu, ctx)

		resp, err := q.cli.Get(ctx, keyLockPath(keyRangeNodePath(keyRangeID)))
		if err != nil {
			return err
		}
		switch len(resp.Kvs) {
		case 0:
			return spqrerror.Newf(spqrerror.SPQR_KEYRANGE_ERROR, "key range with id %v unlocked", keyRangeID)
		case 1:
			_, err := q.cli.Delete(ctx, keyLockPath(keyRangeNodePath(keyRangeID)))
			return err
		default:
			return spqrerror.Newf(spqrerror.SPQR_KEYRANGE_ERROR, "too much key ranges matched: %d", len(resp.Kvs))
		}
	}

	fetchCtx, cf := context.WithTimeout(ctx, 15*time.Second)
	defer cf()

	for {
		select {
		case <-time.After(time.Second):
			if err := unlocker(ctx, sess, id); err != nil {
				return nil
			}
		case <-fetchCtx.Done():
			return spqrerror.New(spqrerror.SPQR_KEYRANGE_ERROR, "lock key range deadlines exceeded")
		}
	}
}

// TODO : unit tests
func (q *EtcdQDB) CheckLockedKeyRange(ctx context.Context, id string) (*KeyRange, error) {
	spqrlog.Zero.Debug().
		Str("id", id).
		Msg("etcdqdb: check locked key range")

	resp, err := q.cli.Get(ctx, keyLockPath(keyRangeNodePath(id)))
	if err != nil {
		return nil, err
	}

	switch len(resp.Kvs) {
	case 0:
		return nil, spqrerror.Newf(spqrerror.SPQR_KEYRANGE_ERROR, "key range %v not locked", id)
	case 1:
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

// ==============================================================================
//                           Transfer transactions
// ==============================================================================

// TODO : unit tests
func (q *EtcdQDB) RecordTransferTx(ctx context.Context, key string, info *DataTransferTransaction) error {
	bts, err := json.Marshal(info)
	if err != nil {
		spqrlog.Zero.Error().Err(err).Msg("Failed to marshal transaction")
		return err
	}

	_, err = q.cli.Put(ctx, key, string(bts))
	if err != nil {
		spqrlog.Zero.Error().Err(err).Msg("Failed to write transaction")
		return err
	}

	return nil
}

// TODO : unit tests
func (q *EtcdQDB) GetTransferTx(ctx context.Context, key string) (*DataTransferTransaction, error) {
	resp, err := q.cli.Get(ctx, key, clientv3.WithPrefix())
	if err != nil {
		spqrlog.Zero.Error().Err(err).Msg("Failed to get transaction")
		return nil, err
	}

	var st DataTransferTransaction

	for _, e := range resp.Kvs {
		if err := json.Unmarshal(e.Value, &st); err != nil {
			spqrlog.Zero.Error().Err(err).Msg("Failed to unmarshal transaction")
			return nil, err
		}
		if st.ToStatus == "" {
			continue
		}
	}
	if st.ToStatus == "" {
		return nil, spqrerror.Newf(spqrerror.SPQR_TRANSFER_ERROR, "no transaction in qdb with key %s", key)
	}
	return &st, nil
}

// TODO : unit tests
func (q *EtcdQDB) RemoveTransferTx(ctx context.Context, key string) error {
	_, err := q.cli.Delete(ctx, key)
	if err != nil {
		spqrlog.Zero.Error().Err(err).Msg("Failed to delete transaction")
		return err
	}
	return nil
}

// ==============================================================================
//	                           COORDINATOR LOCK
// ==============================================================================

// TODO : unit tests
func (q *EtcdQDB) TryCoordinatorLock(ctx context.Context) error {
	spqrlog.Zero.Debug().
		Str("address", config.CoordinatorConfig().Host).
		Msg("etcdqdb: try coordinator lock")

	leaseGrantResp, err := q.cli.Lease.Grant(ctx, CoordKeepAliveTtl)
	if err != nil {
		spqrlog.Zero.Error().Err(err).Msg("etcdqdb: lease grant failed")
		return err
	}

	// KeepAlive attempts to keep the given lease alive forever. If the keepalive responses posted
	// to the channel are not consumed promptly the channel may become full. When full, the lease
	// client will continue sending keep alive requests to the etcd server, but will drop responses
	// until there is capacity on the channel to send more responses.

	keepAliveCh, err := q.cli.Lease.KeepAlive(ctx, leaseGrantResp.ID)
	if err != nil {
		spqrlog.Zero.Error().Err(err).Msg("etcdqdb: lease keep alive failed")
		return err
	}

	go func() {
		for resp := range keepAliveCh {
			spqrlog.Zero.Debug().
				Uint64("raft-term", resp.RaftTerm).
				Int64("lease-id", int64(resp.ID)).
				Msg("etcd keep alive channel")
		}
	}()

	op := clientv3.OpPut(coordLockKey, config.CoordinatorConfig().Host, clientv3.WithLease(clientv3.LeaseID(leaseGrantResp.ID)))
	tx := q.cli.Txn(ctx).If(clientv3util.KeyMissing(coordLockKey)).Then(op)
	stat, err := tx.Commit()
	if err != nil {
		spqrlog.Zero.Error().Err(err).Msg("etcdqdb: failed to commit coordinator lock")
		return err
	}

	if !stat.Succeeded {
		return spqrerror.New(spqrerror.SPQR_UNEXPECTED, "qdb is already in use")
	}

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

	return ret, nil
}

// ==============================================================================
//                                  SHARDS
// ==============================================================================

// TODO : unit tests
func (q *EtcdQDB) AddShard(ctx context.Context, shard *Shard) error {
	spqrlog.Zero.Debug().
		Str("id", shard.ID).
		Strs("hosts", shard.Hosts).
		Msg("etcdqdb: add shard")

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

	return nil
}

// TODO : unit tests
func (q *EtcdQDB) ListShards(ctx context.Context) ([]*Shard, error) {
	spqrlog.Zero.Debug().Msg("etcdqdb: list shards")

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

	return shards, nil
}

// TODO : unit tests
func (q *EtcdQDB) GetShard(ctx context.Context, id string) (*Shard, error) {
	spqrlog.Zero.Debug().
		Str("id", id).
		Msg("etcdqdb: get shard")

	nodePath := shardNodePath(id)
	resp, err := q.cli.Get(ctx, nodePath)
	if err != nil {
		return nil, err
	}

	shardInfo := &Shard{
		ID: id,
	}

	for _, shard := range resp.Kvs {
		// The Port field is always for a while.
		shardInfo.Hosts = append(shardInfo.Hosts, string(shard.Value))
	}

	return shardInfo, nil
}

// TODO : unit tests
func (q *EtcdQDB) DropShard(ctx context.Context, id string) error {
	spqrlog.Zero.Debug().
		Str("id", id).
		Msg("etcdqdb: drop shard")

	nodePath := shardNodePath(id)
	_, err := q.cli.Delete(ctx, nodePath)
	return err
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

	rules := make([]*Distribution, 0, len(resp.Kvs))

	for _, kv := range resp.Kvs {
		var rule *Distribution
		err := json.Unmarshal(kv.Value, &rule)
		if err != nil {
			return nil, err
		}

		rules = append(rules, rule)
	}

	sort.Slice(rules, func(i, j int) bool {
		return rules[i].ID < rules[j].ID
	})

	spqrlog.Zero.Debug().
		Interface("response", resp).
		Msg("etcdqdb: list distributions")
	return rules, nil
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
		resp, err := q.cli.Delete(ctx, distributionNodePath(id))

		spqrlog.Zero.Debug().
			Interface("response", resp).
			Msg("etcdqdb: drop distribution")

		return err
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

		_, err := q.GetRelationDistribution(ctx, rel.Name)
		switch e := err.(type) {
		case *spqrerror.SpqrError:
			if e.ErrorCode != spqrerror.SPQR_NO_DISTRIBUTION {
				return spqrerror.Newf(spqrerror.SPQR_INVALID_REQUEST, "relation \"%s\" is already attached", rel.Name)
			}
		default:
			return spqrerror.Newf(spqrerror.SPQR_INVALID_REQUEST, "relation \"%s\" is already attached", rel.Name)
		}

		resp, err := q.cli.Put(ctx, relationMappingNodePath(rel.Name), id)
		spqrlog.Zero.Debug().
			Interface("responce", resp).
			Msg("etcdqdb: attach table to distribution")
		if err != nil {
			return err
		}
	}

	err = q.CreateDistribution(ctx, distribution)

	return err
}

// TODO: unit tests
func (q *EtcdQDB) AlterDistributionDetach(ctx context.Context, id string, relName string) error {
	spqrlog.Zero.Debug().
		Str("id", id).
		Msg("etcdqdb: detach table from distribution")

	distribution, err := q.GetDistribution(ctx, id)
	if err != nil {
		return err
	}

	delete(distribution.Relations, relName)
	if err = q.CreateDistribution(ctx, distribution); err != nil {
		return err
	}

	_, err = q.cli.Delete(ctx, relationMappingNodePath(relName))
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
		return nil, spqrerror.Newf(spqrerror.SPQR_NO_DISTRIBUTION, "distribution \"%s\" not found", id)
	}

	var distrib *Distribution
	if err := json.Unmarshal(resp.Kvs[0].Value, &distrib); err != nil {
		return nil, err
	}

	return distrib, nil
}

// TODO : unit tests
func (q *EtcdQDB) GetRelationDistribution(ctx context.Context, relName string) (*Distribution, error) {
	spqrlog.Zero.Debug().
		Str("relation", relName).
		Msg("etcdqdb: get distribution for relation")

	resp, err := q.cli.Get(ctx, relationMappingNodePath(relName))
	if err != nil {
		return nil, err
	}
	switch len(resp.Kvs) {
	case 0:
		return nil, spqrerror.Newf(spqrerror.SPQR_NO_DISTRIBUTION, "distribution for relation \"%s\" not found", relName)

	case 1:
		id := string(resp.Kvs[0].Value)
		return q.GetDistribution(ctx, id)
	default:
		// metadata corruption
		return nil, spqrerror.NewByCode(spqrerror.SPQR_METADATA_CORRUPTION)
	}
}

// ==============================================================================
//                              KEY RANGE MOVES
// ==============================================================================

// TODO : unit tests
func (q *EtcdQDB) ListKeyRangeMoves(ctx context.Context) ([]*MoveKeyRange, error) {
	spqrlog.Zero.Debug().Msg("etcdqdb: list move key range operations")

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
		Msg("etcdqdb: list move key range oeprations")
	return moves, nil
}

// TODO : unit tests
func (q *EtcdQDB) RecordKeyRangeMove(ctx context.Context, m *MoveKeyRange) error {
	spqrlog.Zero.Debug().
		Str("id", m.MoveId).
		Msg("etcdqdb: add move key range operation")

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

	return nil
}

// TODO : unit tests
func (q *EtcdQDB) UpdateKeyRangeMoveStatus(ctx context.Context, moveId string, s MoveKeyRangeStatus) error {
	spqrlog.Zero.Debug().
		Str("id", moveId).
		Msg("etcdqdb: update key range")

	resp, err := q.cli.Get(ctx, keyRangeMovesNodePath(moveId), clientv3.WithPrefix())
	if err != nil {
		return err
	}
	if len(resp.Kvs) != 1 {
		return spqrerror.Newf(spqrerror.SPQR_KEYRANGE_ERROR, "failed to update move key range operation by id %s", moveId)
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

	return nil
}
