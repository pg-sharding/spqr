package etcdqdb

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"path"
	"sync"
	"time"

	"github.com/pg-sharding/spqr/pkg/models/shrule"
	"github.com/pg-sharding/spqr/pkg/spqrlog"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
	"golang.org/x/xerrors"
	"google.golang.org/grpc"

	"github.com/pg-sharding/spqr/qdb"
)

type EtcdQDB struct {
	qdb.QrouterDB

	cli *clientv3.Client

	mu        sync.Mutex
	etcdLocks map[string]*concurrency.Mutex
}

const (
	keyRangesNamespace     = "/keyranges"
	routersNamespace       = "/routers"
	shardingRulesNamespace = "/sharding_rules"
	shardsNamespace        = "/shards"
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

func shardingRuleNodePath(key string) string {
	return path.Join(shardingRulesNamespace, key)
}

func shardNodePath(key string) string {
	return path.Join(shardsNamespace, key)
}

func (q *EtcdQDB) GetKeyRange(ctx context.Context, KeyRangeID string) (*qdb.KeyRange, error) {
	kr, err := q.fetchKeyRange(ctx, KeyRangeID)
	spqrlog.Logger.Printf(spqrlog.DEBUG3, "get key range responce %v %v", kr, err)
	return kr, err
}

func (q *EtcdQDB) DropKeyRange(ctx context.Context, KeyRangeID string) error {
	resp, err := q.cli.Delete(ctx, keyRangeNodePath(KeyRangeID))

	spqrlog.Logger.Printf(spqrlog.DEBUG4, "delete resp %v", resp)
	return err
}

func (q *EtcdQDB) DropKeyRangeAll(ctx context.Context) ([]*qdb.KeyRange, error) {
	krids, err := q.ListKeyRanges(ctx)
	if err != nil {
		return nil, err
	}
	for _, krcurr := range krids {
		resp, err := q.cli.Delete(ctx, keyRangeNodePath(krcurr.KeyRangeID))
		spqrlog.Logger.Printf(spqrlog.DEBUG4, "delete resp %v", resp)
		if err != nil {
			return nil, err
		}
	}
	return krids, nil
}

func (q *EtcdQDB) ListRouters(ctx context.Context) ([]*qdb.Router, error) {
	resp, err := q.cli.Get(ctx, routersNamespace, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}

	spqrlog.Logger.Printf(spqrlog.DEBUG3, "got resp %v", resp)
	var ret []*qdb.Router

	for _, e := range resp.Kvs {
		_, keyRangeID := path.Split(string(e.Key))
		ret = append(ret,
			qdb.NewRouter(
				string(e.Value),
				keyRangeID,
			),
		)
	}

	return ret, nil
}

func (q *EtcdQDB) Watch(krid string, status *qdb.KeyRangeStatus, notifyio chan<- interface{}) error {
	return nil
}

const keyspace = "key_space"

func NewEtcdQDB(addr string) (*EtcdQDB, error) {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints: []string{addr},
		DialOptions: []grpc.DialOption{
			grpc.WithInsecure(),
		},
	})
	if err != nil {
		spqrlog.Logger.PrintError(err)
		return nil, err
	}

	spqrlog.Logger.Printf(spqrlog.DEBUG1, "qdb service, %s %#v", addr, cli)

	return &EtcdQDB{
		cli: cli,
	}, nil
}

func (q *EtcdQDB) AddRouter(ctx context.Context, r *qdb.Router) error {
	resp, err := q.cli.Put(ctx, routerNodePath(r.ID()), r.Addr())
	if err != nil {
		spqrlog.Logger.PrintError(err)
		return err
	}

	spqrlog.Logger.Printf(spqrlog.DEBUG1, "put resp %v", resp)
	return nil
}

func (q *EtcdQDB) DeleteRouter(ctx context.Context, rID string) error {
	resp, err := q.cli.Delete(ctx, routerNodePath(rID))
	if err != nil {
		spqrlog.Logger.PrintError(err)
		return err
	}

	spqrlog.Logger.Printf(spqrlog.DEBUG1, "del resp %v", resp)
	return nil
}

func (q *EtcdQDB) Lock(ctx context.Context, keyRangeID string) (*qdb.KeyRange, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	sess, err := concurrency.NewSession(q.cli)
	if err != nil {
		return nil, err
	}

	defer sess.Close()

	fetcher := func(ctx context.Context, sess *concurrency.Session, keyRangeID string) (*qdb.KeyRange, error) {
		mu := concurrency.NewMutex(sess, keyspace)
		err = mu.Lock(ctx)
		if err != nil {
			return nil, err
		}

		defer mu.Unlock(ctx)

		resp, err := q.cli.Get(ctx, keyLockPath(keyRangeID))
		if err != nil {
			return nil, err
		}
		switch len(resp.Kvs) {
		case 0:
			_, err := q.cli.Put(ctx, keyLockPath(keyRangeNodePath(keyRangeID)), "locked")
			if err != nil {
				return nil, err
			}

			return q.fetchKeyRange(ctx, keyRangeNodePath(keyRangeID))
		case 1:
			return nil, fmt.Errorf("key range with id %v locked", keyRangeID)
		default:
			return nil, fmt.Errorf("too much key ranges matched: %d", len(resp.Kvs))
		}
	}

	timer := time.NewTimer(time.Second)

	fetchCtx, cf := context.WithTimeout(ctx, 15*time.Second)
	defer cf()

	for {
		select {
		case <-timer.C:
			val, err := fetcher(ctx, sess, keyRangeID)
			if err != nil {
				spqrlog.Logger.Printf(spqrlog.ERROR, "Error while fetching %v", err)
				continue
			}

			return val, nil

		case <-fetchCtx.Done():
			return nil, xerrors.Errorf("deadlines exceeded")
		}
	}
}

func (q *EtcdQDB) fetchKeyRange(ctx context.Context, nodePath string) (*qdb.KeyRange, error) {
	// caller ensures key is locked
	raw, err := q.cli.Get(ctx, nodePath)
	if err != nil {
		return nil, err
	}

	switch len(raw.Kvs) {
	case 1:

		ret := qdb.KeyRange{}

		if err := json.Unmarshal(raw.Kvs[0].Value, &ret); err != nil {
			return nil, err
		}
		return &ret, nil

	default:
		return nil, xerrors.Errorf("failed to fetch key range with id %v", nodePath)
	}
}

func (q *EtcdQDB) Unlock(ctx context.Context, keyRangeID string) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	sess, err := concurrency.NewSession(q.cli)
	if err != nil {
		return err
	}

	defer sess.Close()

	unlocker := func(ctx context.Context, sess *concurrency.Session, keyRangeID string) error {
		mu := concurrency.NewMutex(sess, keyspace)
		err = mu.Lock(ctx)
		if err != nil {
			return err
		}

		defer func(mu *concurrency.Mutex, ctx context.Context) {
			err := mu.Unlock(ctx)
			if err != nil {
				spqrlog.Logger.PrintError(err)
			}
		}(mu, ctx)

		resp, err := q.cli.Get(ctx, keyLockPath(keyRangeID))
		if err != nil {
			return err
		}
		switch len(resp.Kvs) {
		case 0:
			return fmt.Errorf("key range with id %v unlocked", keyRangeID)
		case 1:
			_, err := q.cli.Delete(ctx, keyLockPath(keyRangeNodePath(keyRangeID)))
			return err
		default:
			return fmt.Errorf("too much key ranges matched: %d", len(resp.Kvs))
		}
	}

	fetchCtx, cf := context.WithTimeout(ctx, 15*time.Second)
	defer cf()

	for {
		select {
		case <-time.After(time.Second):
			if err := unlocker(ctx, sess, keyRangeID); err != nil {
				return nil
			}
		case <-fetchCtx.Done():
			return fmt.Errorf("deadlines exceeded")
		}
	}
}

func (q *EtcdQDB) AddKeyRange(ctx context.Context, keyRange *qdb.KeyRange) error {

	rawKeyRange, err := json.Marshal(keyRange)

	if err != nil {
		return err
	}

	spqrlog.Logger.Printf(spqrlog.DEBUG3, "send req to qdb")

	resp, err := q.cli.Put(ctx, keyRangeNodePath(keyRange.KeyRangeID), string(rawKeyRange))
	if err != nil {
		return err
	}

	spqrlog.Logger.Printf(spqrlog.DEBUG3, "put kr to qdb resp %v", resp)
	return err
}

func (q *EtcdQDB) UpdateKeyRange(ctx context.Context, keyRange *qdb.KeyRange) error {
	rawKeyRange, err := json.Marshal(keyRange)

	if err != nil {
		return err
	}

	resp, err := q.cli.Put(ctx, keyRangeNodePath(keyRange.KeyRangeID), string(rawKeyRange))
	if err != nil {
		return err
	}

	spqrlog.Logger.Printf(spqrlog.DEBUG3, "put resp %v", resp)
	return err
}

func (q *EtcdQDB) ListKeyRanges(ctx context.Context) ([]*qdb.KeyRange, error) {
	resp, err := q.cli.Get(ctx, keyRangesNamespace, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}

	spqrlog.Logger.Printf(spqrlog.DEBUG4, "got resp %v", resp)
	var ret []*qdb.KeyRange

	for _, e := range resp.Kvs {
		var kr qdb.KeyRange

		if err := json.Unmarshal(e.Value, &kr); err != nil {
			return nil, err
		}

		ret = append(ret, &kr)
	}

	return ret, nil
}

func (q *EtcdQDB) Check(ctx context.Context, kr *qdb.KeyRange) bool {
	return true
}

func (q *EtcdQDB) AddShardingRule(ctx context.Context, shRule *shrule.ShardingRule) error {
	ops := make([]clientv3.Op, len(shRule.Columns()))
	for i, key := range shRule.Columns() {
		ops[i] = clientv3.OpPut(shardingRuleNodePath(key), "")
	}

	resp, err := q.cli.Txn(ctx).Then(ops...).Commit()

	if err != nil {
		return err
	}

	spqrlog.Logger.Printf(spqrlog.DEBUG3, "put sharding rules resp %v", resp)
	return nil
}

func (q *EtcdQDB) ListShardingRules(ctx context.Context) ([]*shrule.ShardingRule, error) {
	namespacePrefix := shardingRulesNamespace + "/"
	resp, err := q.cli.Get(ctx, namespacePrefix, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}

	rules := make([]*shrule.ShardingRule, 0, len(resp.Kvs))

	for _, kv := range resp.Kvs {
		// A sharding rule supports no more than one column for a while.
		rules = append(rules, shrule.NewShardingRule([]string{string(bytes.TrimPrefix(kv.Key, []byte(namespacePrefix)))}))
	}

	spqrlog.Logger.Printf(spqrlog.DEBUG3, "list sharding rules resp %v", resp)
	return rules, nil
}

func (q *EtcdQDB) AddShard(ctx context.Context, shard *qdb.Shard) error {
	resp, err := q.cli.Put(ctx, shardNodePath(shard.ID), shard.Addr)
	if err != nil {
		return err
	}

	spqrlog.Logger.Printf(spqrlog.DEBUG3, "put resp %v", resp)
	return nil
}

func (q *EtcdQDB) ListShards(ctx context.Context) ([]*qdb.Shard, error) {
	namespacePrefix := shardsNamespace + "/"
	resp, err := q.cli.Get(ctx, namespacePrefix, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}

	rules := make([]*qdb.Shard, 0, len(resp.Kvs))

	for _, kv := range resp.Kvs {
		rules = append(rules, &qdb.Shard{
			ID:   string(bytes.TrimPrefix(kv.Key, []byte(namespacePrefix))),
			Addr: string(kv.Value),
		})
	}

	return rules, nil
}

func (q *EtcdQDB) GetShardInfo(ctx context.Context, shardID string) (*qdb.ShardInfo, error) {
	nodePath := shardNodePath(shardID)

	resp, err := q.cli.Get(ctx, nodePath)
	if err != nil {
		return nil, err
	}

	shardInfo := &qdb.ShardInfo{
		ID: shardID,
	}

	for _, shard := range resp.Kvs {
		// The Port field is always for a while.
		shardInfo.Hosts = append(shardInfo.Hosts, string(shard.Value))
	}

	return shardInfo, nil
}

var _ qdb.QrouterDB = &EtcdQDB{}
