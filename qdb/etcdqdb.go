package qdb

import (
	"context"
	"encoding/json"
	"fmt"
	"path"
	"sort"
	"sync"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
	"google.golang.org/grpc"

	"github.com/pg-sharding/spqr/pkg/spqrlog"
)

type EtcdQDB struct {
	cli *clientv3.Client
	mu  sync.Mutex
}

var _ QDB = &EtcdQDB{}

func NewEtcdQDB(addr string) (*EtcdQDB, error) {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints: []string{addr},
		DialOptions: []grpc.DialOption{
			grpc.WithInsecure(),
		},
	})
	if err != nil {
		return nil, err
	}

	spqrlog.Logger.Printf(spqrlog.LOG, "etcdqdb: NewEtcdQDB, %s %#v", addr, cli)

	return &EtcdQDB{
		cli: cli,
	}, nil
}

const (
	keyRangesNamespace     = "/keyranges"
	dataspaceNamespace     = "/dataspaces"
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

func dataspaceNodePath(key string) string {
	return path.Join(dataspaceNamespace, key)
}

// ==============================================================================
//                               SHARDING RULES
// ==============================================================================

func (q *EtcdQDB) AddShardingRule(ctx context.Context, rule *ShardingRule) error {
	spqrlog.Logger.Printf(spqrlog.LOG, "etcdqdb: adding sharding rule %v", rule.Entries[0].Column)
	rawShardingRule, err := json.Marshal(rule)
	if err != nil {
		return err
	}

	spqrlog.Logger.Printf(spqrlog.LOG, "etcdqdb: send req to qdb")
	resp, err := q.cli.Put(ctx, shardingRuleNodePath(rule.ID), string(rawShardingRule))
	if err != nil {
		return err
	}

	spqrlog.Logger.Printf(spqrlog.DEBUG1, "put sharding rule to qdb resp %v", resp)
	return err
}

func (q *EtcdQDB) DropShardingRule(ctx context.Context, id string) error {
	spqrlog.Logger.Printf(spqrlog.LOG, "etcdqdb: drop sharding rule %v", id)
	resp, err := q.cli.Delete(ctx, shardingRuleNodePath(id))
	if err != nil {
		return err
	}

	spqrlog.Logger.Printf(spqrlog.DEBUG1, "put sharding rule to qdb resp %+v", resp.PrevKvs)
	return nil
}

func (q *EtcdQDB) DropShardingRuleAll(ctx context.Context) ([]*ShardingRule, error) {
	spqrlog.Logger.Printf(spqrlog.LOG, "etcdqdb: drop all sharding rules")
	resp, err := q.cli.Delete(ctx, shardingRulesNamespace, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}

	spqrlog.Logger.Printf(spqrlog.DEBUG1, "put sharding rule to qdb resp %v", resp.PrevKvs)
	return nil, nil
}

func (q *EtcdQDB) GetShardingRule(ctx context.Context, id string) (*ShardingRule, error) {
	spqrlog.Logger.Printf(spqrlog.LOG, "etcdqdb: get sharding rule %v", id)
	resp, err := q.cli.Get(ctx, shardingRuleNodePath(id), clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}
	var rule ShardingRule
	err = json.Unmarshal(resp.Kvs[0].Value, &rule)
	if err != nil {
		return nil, err
	}

	spqrlog.Logger.Printf(spqrlog.DEBUG1, "get sharding rules resp %v", resp)
	return &rule, nil
}

func (q *EtcdQDB) ListShardingRules(ctx context.Context) ([]*ShardingRule, error) {
	spqrlog.Logger.Printf(spqrlog.LOG, "etcdqdb: list sharding rules")
	namespacePrefix := shardingRulesNamespace + "/"
	resp, err := q.cli.Get(ctx, namespacePrefix, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}

	rules := make([]*ShardingRule, 0, len(resp.Kvs))

	for _, kv := range resp.Kvs {
		// A sharding rule supports no more than one column for a while.
		var rule *ShardingRule
		err := json.Unmarshal(kv.Value, &rule)
		if err != nil {
			return nil, err
		}

		rules = append(rules, rule)
	}

	sort.Slice(rules, func(i, j int) bool {
		return rules[i].ID < rules[j].ID
	})

	spqrlog.Logger.Printf(spqrlog.DEBUG1, "list sharding rules resp %v", resp)
	return rules, nil
}

// ==============================================================================
//                                 KEY RANGES
// ==============================================================================

func (q *EtcdQDB) AddKeyRange(ctx context.Context, keyRange *KeyRange) error {
	spqrlog.Logger.Printf(spqrlog.LOG, "etcdqdb: add key range %+v", keyRange)

	rawKeyRange, err := json.Marshal(keyRange)

	if err != nil {
		return err
	}

	resp, err := q.cli.Put(ctx, keyRangeNodePath(keyRange.KeyRangeID), string(rawKeyRange))
	if err != nil {
		return err
	}

	spqrlog.Logger.Printf(spqrlog.DEBUG1, "put kr to qdb resp %v", resp)
	return err
}

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
		spqrlog.Logger.Printf(spqrlog.DEBUG1, "got kvs list: %+v", raw.Kvs)
		return nil, fmt.Errorf("failed to fetch key range with id %v", nodePath)
	}
}

func (q *EtcdQDB) GetKeyRange(ctx context.Context, id string) (*KeyRange, error) {
	spqrlog.Logger.Printf(spqrlog.LOG, "etcdqdb: get key range %+v", id)
	krret, err := q.fetchKeyRange(ctx, keyRangeNodePath(id))
	spqrlog.Logger.Printf(spqrlog.DEBUG1, "get key range responce %v %v", krret, err)
	return krret, err
}

func (q *EtcdQDB) UpdateKeyRange(ctx context.Context, keyRange *KeyRange) error {
	spqrlog.Logger.Printf(spqrlog.LOG, "etcdqdb: update key range %+v", keyRange)
	rawKeyRange, err := json.Marshal(keyRange)

	if err != nil {
		return err
	}

	resp, err := q.cli.Put(ctx, keyRangeNodePath(keyRange.KeyRangeID), string(rawKeyRange))
	if err != nil {
		return err
	}

	spqrlog.Logger.Printf(spqrlog.DEBUG1, "put resp %v", resp)
	return err
}

func (q *EtcdQDB) DropKeyRangeAll(ctx context.Context) error {
	spqrlog.Logger.Printf(spqrlog.LOG, "etcdqdb: drop all key ranges")
	resp, err := q.cli.Delete(ctx, keyRangesNamespace, clientv3.WithPrefix())
	if err != nil {
		return err
	}
	spqrlog.Logger.Printf(spqrlog.DEBUG1, "got delete with prefix reps %+v", resp)
	return nil
}

const keyspace = "key_space"

func (q *EtcdQDB) DropKeyRange(ctx context.Context, id string) error {
	spqrlog.Logger.Printf(spqrlog.LOG, "etcdqdb: drop key range %+v", id)
	resp, err := q.cli.Delete(ctx, keyRangeNodePath(id))
	spqrlog.Logger.Printf(spqrlog.DEBUG4, "delete resp %v", resp)
	return err
}

func (q *EtcdQDB) ListKeyRanges(ctx context.Context) ([]*KeyRange, error) {
	spqrlog.Logger.Printf(spqrlog.LOG, "etcdqdb: list all key ranges")
	resp, err := q.cli.Get(ctx, keyRangesNamespace, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}

	spqrlog.Logger.Printf(spqrlog.DEBUG4, "got resp %v", resp)
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

	return ret, nil
}

func (q *EtcdQDB) LockKeyRange(ctx context.Context, id string) (*KeyRange, error) {
	spqrlog.Logger.Printf(spqrlog.LOG, "etcdqdb: lock key range %+v", id)
	q.mu.Lock()
	defer q.mu.Unlock()

	sess, err := concurrency.NewSession(q.cli)
	if err != nil {
		return nil, err
	}

	defer func(sess *concurrency.Session) {
		err := sess.Close()
		if err != nil {

		}
	}(sess)

	fetcher := func(ctx context.Context, sess *concurrency.Session, keyRangeID string) (*KeyRange, error) {
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

			return q.GetKeyRange(ctx, keyRangeID)
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
			val, err := fetcher(ctx, sess, id)
			if err != nil {
				spqrlog.Logger.Printf(spqrlog.ERROR, "Error while fetching %v", err)
				continue
			}

			return val, nil

		case <-fetchCtx.Done():
			return nil, fmt.Errorf("deadlines exceeded")
		}
	}
}

func (q *EtcdQDB) UnlockKeyRange(ctx context.Context, id string) error {
	spqrlog.Logger.Printf(spqrlog.LOG, "etcdqdb: unlock key range %+v", id)
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
			if err := unlocker(ctx, sess, id); err != nil {
				return nil
			}
		case <-fetchCtx.Done():
			return fmt.Errorf("deadlines exceeded")
		}
	}
}

func (q *EtcdQDB) CheckLockedKeyRange(ctx context.Context, id string) (*KeyRange, error) {
	spqrlog.Logger.Printf(spqrlog.LOG, "etcdqdb: check locked key range %+v", id)
	// implement or remove me
	return nil, nil
}

func (q *EtcdQDB) ShareKeyRange(id string) error {
	spqrlog.Logger.Printf(spqrlog.LOG, "etcdqdb: share key range %+v", id)
	// implement or remove me
	return nil
}

// ==============================================================================
//                                  ROUTERS
// ==============================================================================

func (q *EtcdQDB) AddRouter(ctx context.Context, r *Router) error {
	spqrlog.Logger.Printf(spqrlog.DEBUG2, "etcdqdb: add router %v", r)
	bts, err := json.Marshal(r)
	if err != nil {
		return err
	}
	resp, err := q.cli.Put(ctx, routerNodePath(r.ID), string(bts))
	if err != nil {
		return err
	}

	spqrlog.Logger.Printf(spqrlog.DEBUG1, "put resp %v", resp)
	return nil
}

func (q *EtcdQDB) DeleteRouter(ctx context.Context, id string) error {
	spqrlog.Logger.Printf(spqrlog.DEBUG2, "etcdqdb: delete router %v", id)
	resp, err := q.cli.Delete(ctx, routerNodePath(id))
	if err != nil {
		spqrlog.Logger.PrintError(err)
		return err
	}

	spqrlog.Logger.Printf(spqrlog.DEBUG1, "etcdqdb: del resp %v", resp)
	return nil
}

func (q *EtcdQDB) ListRouters(ctx context.Context) ([]*Router, error) {
	spqrlog.Logger.Printf(spqrlog.DEBUG2, "etcdqdb: list routers")
	resp, err := q.cli.Get(ctx, routersNamespace, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}

	spqrlog.Logger.Printf(spqrlog.DEBUG3, "etcdqdb: list routers: got resp %v", resp)
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

	return ret, nil
}

func (q *EtcdQDB) LockRouter(ctx context.Context, id string) error {
	spqrlog.Logger.Printf(spqrlog.LOG, "etcdqdb: lock router %v", id)
	return nil
}

// ==============================================================================
//                                  SHARDS
// ==============================================================================

func (q *EtcdQDB) AddShard(ctx context.Context, shard *Shard) error {
	spqrlog.Logger.Printf(spqrlog.LOG, "etcdqdb: add shard %+v", shard)
	bytes, err := json.Marshal(shard)
	if err != nil {
		return err
	}
	resp, err := q.cli.Put(ctx, shardNodePath(shard.ID), string(bytes))
	if err != nil {
		return err
	}

	spqrlog.Logger.Printf(spqrlog.DEBUG1, "put resp %v", resp)
	return nil
}

func (q *EtcdQDB) ListShards(ctx context.Context) ([]*Shard, error) {
	spqrlog.Logger.Printf(spqrlog.LOG, "etcdqdb: list shards")
	namespacePrefix := shardsNamespace + "/"
	resp, err := q.cli.Get(ctx, namespacePrefix, clientv3.WithPrefix())
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

func (q *EtcdQDB) GetShard(ctx context.Context, id string) (*Shard, error) {
	spqrlog.Logger.Printf(spqrlog.LOG, "etcdqdb: get shard %v", id)
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

// ==============================================================================
//                                  DATASPACES
// ==============================================================================

func (q *EtcdQDB) AddDataspace(ctx context.Context, dataspace *Dataspace) error {
	spqrlog.Logger.Printf(spqrlog.LOG, "etcdqdb: add shard %+v", dataspace)
	resp, err := q.cli.Put(ctx, dataspaceNodePath(dataspace.ID), dataspace.ID)
	if err != nil {
		return err
	}

	spqrlog.Logger.Printf(spqrlog.DEBUG1, "put resp %v", resp)
	return nil
}

func (q *EtcdQDB) ListDataspaces(ctx context.Context) ([]*Dataspace, error) {
	spqrlog.Logger.Printf(spqrlog.LOG, "etcdqdb: list dataspaces")
	namespacePrefix := dataspaceNamespace + "/"
	resp, err := q.cli.Get(ctx, namespacePrefix, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}

	rules := make([]*Dataspace, 0, len(resp.Kvs))

	for _, kv := range resp.Kvs {
		// A sharding rule supports no more than one column for a while.
		var rule *Dataspace
		err := json.Unmarshal(kv.Value, &rule)
		if err != nil {
			return nil, err
		}

		rules = append(rules, rule)
	}

	sort.Slice(rules, func(i, j int) bool {
		return rules[i].ID < rules[j].ID
	})

	spqrlog.Logger.Printf(spqrlog.DEBUG1, "list dataspace resp %v", resp)
	return rules, nil
}
