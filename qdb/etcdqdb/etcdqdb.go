package etcdqdb

import (
	"bytes"
	"context"
	"encoding/json"
	"path"
	"sync"
	"time"

	"github.com/wal-g/tracelog"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
	"golang.org/x/xerrors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/pg-sharding/spqr/qdb"
)

type notifier struct {
	pch chan interface{}
	sch chan chan interface{}
}

func newnotifier() *notifier {
	return &notifier{
		pch: make(chan interface{}, 1),
		sch: make(chan chan interface{}, 1),
	}
}

func (n *notifier) run() {
	subs := map[chan interface{}]struct{}{}
	for {
		select {
		case msgCh := <-n.sch:
			subs[msgCh] = struct{}{}
		case msg := <-n.pch:
			for msgCh := range subs {
				select {
				case msgCh <- msg:
				default:
				}
			}
		}
	}
}

func (n *notifier) Subscribe() chan interface{} {
	msgCh := make(chan interface{}, 0)
	n.sch <- msgCh
	return msgCh
}

func (n *notifier) nofity(msg interface{}) {
	n.pch <- msg
}

type EtcdQDB struct {
	qdb.QrouterDB

	cli *clientv3.Client

	mu        sync.Mutex
	locks     map[string]*notifier
	etcdLocks map[string]*concurrency.Mutex
}

const (
	keyRangesNamespace     = "/keyranges"
	routersRangesNamespace = "/routers"
	shardingRulesNamespace = "/sharding_rules"
	shardsNamespace        = "/shards"
)

func keyLockPath(key string) string {
	return path.Join(key, "lock")
}

func keyRangeNodePath(key string) string {
	return path.Join(keyRangesNamespace, key)
}

func routerNodePath(key string) string {
	return path.Join(routersRangesNamespace, key)
}

func shardingRuleNodePath(key string) string {
	return path.Join(shardingRulesNamespace, key)
}

func shardNodePath(key string) string {
	return path.Join(shardsNamespace, key)
}

func (q *EtcdQDB) DropKeyRange(ctx context.Context, keyRange *qdb.KeyRange) error {
	resp, err := q.cli.Delete(ctx, keyRangeNodePath(keyRange.KeyRangeID))

	tracelog.InfoLogger.Printf("delete resp %v", resp)
	return err
}

func (q *EtcdQDB) ListRouters(ctx context.Context) ([]*qdb.Router, error) {
	resp, err := q.cli.Get(ctx, routersRangesNamespace, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}

	tracelog.InfoLogger.Printf("got resp %v", resp)
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
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		},
	})
	tracelog.InfoLogger.Printf("Coordinator Service, %s %#v", addr, cli)

	if err != nil {
		return nil, err
	}

	return &EtcdQDB{
		cli:   cli,
		locks: map[string]*notifier{},
	}, nil
}

func (q *EtcdQDB) AddRouter(ctx context.Context, r *qdb.Router) error {
	resp, err := q.cli.Put(ctx, routerNodePath(r.ID()), r.Addr())
	if err != nil {
		tracelog.ErrorLogger.PrintError(err)
		return err
	}

	tracelog.InfoLogger.Printf("put resp %v", resp)
	return nil
}

func (q *EtcdQDB) DeleteRouter(ctx context.Context, rID string) error {
	resp, err := q.cli.Delete(ctx, routerNodePath(rID))
	if err != nil {
		tracelog.ErrorLogger.PrintError(err)
		return err
	}

	tracelog.InfoLogger.Printf("del resp %v", resp)
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
			return nil, xerrors.Errorf("key range with id %v locked", keyRangeID)
		default:
			return nil, xerrors.Errorf("too much key ranges matched: %d", len(resp.Kvs))
		}
	}

	timer := time.NewTimer(time.Second)

	fetchCtx, cf := context.WithTimeout(ctx, 15*time.Second)
	defer cf()

	for {
		select {
		case <-timer.C:
			if val, err := fetcher(ctx, sess, keyRangeID); err != nil {
				return val, nil
			}
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

func (q *EtcdQDB) UnLock(ctx context.Context, keyRangeID string) error {
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

		defer mu.Unlock(ctx)

		resp, err := q.cli.Get(ctx, keyLockPath(keyRangeID))
		if err != nil {
			return err
		}
		switch len(resp.Kvs) {
		case 0:

			return xerrors.Errorf("key range with id %v unlocked", keyRangeID)

		case 1:
			_, err := q.cli.Delete(ctx, keyLockPath(keyRangeNodePath(keyRangeID)))
			return err
		default:
			return xerrors.Errorf("too much key ranges matched: %d", len(resp.Kvs))
		}
	}

	timer := time.NewTimer(time.Second)

	fetchCtx, cf := context.WithTimeout(ctx, 15*time.Second)
	defer cf()

	for {
		select {
		case <-timer.C:
			if err := unlocker(ctx, sess, keyRangeID); err != nil {
				return nil
			}
		case <-fetchCtx.Done():
			return xerrors.Errorf("deadlines exceeded")
		}
	}
}

func (q *EtcdQDB) AddKeyRange(ctx context.Context, keyRange *qdb.KeyRange) error {
	rawKeyRange, err := json.Marshal(keyRange)

	if err != nil {
		return err
	}

	resp, err := q.cli.Put(ctx, keyRangeNodePath(keyRange.KeyRangeID), string(rawKeyRange))
	if err != nil {
		return err
	}

	tracelog.InfoLogger.Printf("put resp %v", resp)
	return err
}

func (q *EtcdQDB) ListKeyRange(ctx context.Context) ([]*qdb.KeyRange, error) {
	resp, err := q.cli.Get(ctx, keyRangesNamespace, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}

	tracelog.InfoLogger.Printf("got resp %v", resp)
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

func (q *EtcdQDB) AddShardingRule(ctx context.Context, shRule *qdb.ShardingRule) error {
	ops := make([]clientv3.Op, len(shRule.Columns))
	for i, key := range shRule.Columns {
		ops[i] = clientv3.OpPut(shardingRuleNodePath(key), "")
	}

	resp, err := q.cli.Txn(ctx).Then(ops...).Commit()

	if err != nil {
		return err
	}

	tracelog.InfoLogger.Printf("put sharding rules resp %v", resp)
	return err
}

func (q *EtcdQDB) ListShardingRules(ctx context.Context) ([]*qdb.ShardingRule, error) {
	namespacePrefix := shardingRulesNamespace + "/"
	resp, err := q.cli.Get(ctx, namespacePrefix, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}

	rules := make([]*qdb.ShardingRule, 0, len(resp.Kvs))

	for _, kv := range resp.Kvs {
		// A sharding rule supports no more than one column for a while.
		rules = append(rules, &qdb.ShardingRule{
			Columns: []string{string(bytes.TrimPrefix(kv.Key, []byte(namespacePrefix)))},
		})
	}

	tracelog.InfoLogger.Printf("list sharding rules resp %v", resp)
	return rules, nil
}

func (q *EtcdQDB) AddShard(ctx context.Context, shard *qdb.Shard) error {
	resp, err := q.cli.Put(ctx, shardNodePath(shard.ID), shard.Addr)
	if err != nil {
		return err
	}

	tracelog.InfoLogger.Printf("put resp %v", resp)
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
