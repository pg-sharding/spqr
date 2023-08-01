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
	keyspace               = "key_space"
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
	spqrlog.Zero.Debug().
		Str("id", rule.ID).
		Str("table", rule.TableName).
		Str("column", rule.Entries[0].Column).
		Msg("etcdqdb: add sharding rule")

	rawShardingRule, err := json.Marshal(rule)
	if err != nil {
		return err
	}

	spqrlog.Zero.Debug().Msg("etcdqdb: send req to qdb")
	resp, err := q.cli.Put(ctx, shardingRuleNodePath(rule.ID), string(rawShardingRule))
	if err != nil {
		return err
	}

	spqrlog.Zero.Debug().
		Interface("response", resp).
		Msg("etcdqdb: put sharding rule to qdb")

	return err
}

func (q *EtcdQDB) DropShardingRule(ctx context.Context, id string) error {
	spqrlog.Zero.Debug().
		Str("id", id).
		Msg("etcdqdb: drop sharding rule")

	resp, err := q.cli.Delete(ctx, shardingRuleNodePath(id))
	if err != nil {
		return err
	}

	spqrlog.Zero.Debug().
		Interface("response", resp).
		Msg("etcdqdb: drop sharding rule")

	return nil
}

func (q *EtcdQDB) DropShardingRuleAll(ctx context.Context) ([]*ShardingRule, error) {
	spqrlog.Zero.Debug().Msg("etcdqdb: drop all sharding rules")

	resp, err := q.cli.Delete(ctx, shardingRulesNamespace, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}

	spqrlog.Zero.Debug().
		Interface("response", resp).
		Msg("etcdqdb: put sharding rule to qdb")

	return nil, nil
}

func (q *EtcdQDB) GetShardingRule(ctx context.Context, id string) (*ShardingRule, error) {
	spqrlog.Zero.Debug().
		Str("id", id).
		Msg("etcdqdb: get sharding rule")

	resp, err := q.cli.Get(ctx, shardingRuleNodePath(id), clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}
	var rule ShardingRule
	if err := json.Unmarshal(resp.Kvs[0].Value, &rule); err != nil {
		return nil, err
	}

	spqrlog.Zero.Debug().
		Interface("response", resp).
		Msg("etcdqdb: get sharding rule")

	return &rule, nil
}

func (q *EtcdQDB) ListShardingRules(ctx context.Context) ([]*ShardingRule, error) {
	spqrlog.Zero.Debug().Msg("etcdqdb: list all sharding rules")

	namespacePrefix := shardingRulesNamespace + "/"
	resp, err := q.cli.Get(ctx, namespacePrefix, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}

	rules := make([]*ShardingRule, 0, len(resp.Kvs))

	for _, kv := range resp.Kvs {
		// A sharding rule supports no more than one column for a while.
		var rule *ShardingRule
		if err := json.Unmarshal(kv.Value, &rule); err != nil {
			return nil, err
		}

		rules = append(rules, rule)
	}

	sort.Slice(rules, func(i, j int) bool {
		return rules[i].ID < rules[j].ID
	})

	spqrlog.Zero.Debug().
		Interface("response", resp).
		Msg("etcdqdb: list sharding rules")

	return rules, nil
}

// ==============================================================================
//                                 KEY RANGES
// ==============================================================================

func (q *EtcdQDB) AddKeyRange(ctx context.Context, keyRange *KeyRange) error {
	spqrlog.Zero.Debug().
		Bytes("lower-bound", keyRange.LowerBound).
		Bytes("upper-bound", keyRange.UpperBound).
		Str("shard-id", keyRange.ShardID).
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
		return nil, fmt.Errorf("failed to fetch key range with id %v", nodePath)
	}
}

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

func (q *EtcdQDB) UpdateKeyRange(ctx context.Context, keyRange *KeyRange) error {
	spqrlog.Zero.Debug().
		Bytes("lower-bound", keyRange.LowerBound).
		Bytes("upper-bound", keyRange.UpperBound).
		Str("shard-id", keyRange.ShardID).
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

func (q *EtcdQDB) MatchShardingRules(ctx context.Context, m func(shrules map[string]*ShardingRule) error) error {
	/* TODO: support */
	return nil
}

func (q *EtcdQDB) ListKeyRanges(ctx context.Context) ([]*KeyRange, error) {
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
		Msg("etcdqdb: list key ranges")

	return ret, nil
}

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
				spqrlog.Zero.Error().
					Err(err).
					Msg("error while fetching")
				continue
			}

			return val, nil

		case <-fetchCtx.Done():
			return nil, fmt.Errorf("deadlines exceeded")
		}
	}
}

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
	spqrlog.Zero.Debug().
		Str("id", id).
		Msg("etcdqdb: check locked key range")
	return nil, fmt.Errorf("implement CheckLockedKeyRange")
}

func (q *EtcdQDB) ShareKeyRange(id string) error {
	spqrlog.Zero.Debug().
		Str("id", id).
		Msg("etcdqdb: share key range")
	return fmt.Errorf("implement ShareKeyRange")
}

// ==============================================================================
//                           Transfer transactions
// ==============================================================================

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
	}
	return &st, nil
}

func (q *EtcdQDB) RemoveTransferTx(ctx context.Context, key string) error {
	_, err := q.cli.Delete(ctx, key)
	if err != nil {
		spqrlog.Zero.Error().Err(err).Msg("Failed to delete transaction")
		return err
	}
	return nil
}

// ==============================================================================
//                                  ROUTERS
// ==============================================================================

func (q *EtcdQDB) AddRouter(ctx context.Context, r *Router) error {
	spqrlog.Zero.Debug().
		Str("id", r.ID).
		Str("address", r.Address).
		Str("state", string(r.State)).
		Msg("etcdqdb: add router")

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

func (q *EtcdQDB) DeleteRouter(ctx context.Context, id string) error {
	spqrlog.Zero.Debug().
		Str("id", id).
		Msg("etcdqdb: drop router")

	resp, err := q.cli.Delete(ctx, routerNodePath(id))
	if err != nil {
		return err
	}

	spqrlog.Zero.Debug().
		Interface("response", resp).
		Msg("etcdqdb: drop router")

	return nil
}

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

func (q *EtcdQDB) LockRouter(ctx context.Context, id string) error {
	spqrlog.Zero.Debug().
		Str("id", id).
		Msg("etcdqdb: lock router")

	return nil
}

// ==============================================================================
//                                  SHARDS
// ==============================================================================

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

func (q *EtcdQDB) ListShards(ctx context.Context) ([]*Shard, error) {
	spqrlog.Zero.Debug().Msg("etcdqdb: list shards")

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

// ==============================================================================
//                                  DATASPACES
// ==============================================================================

func (q *EtcdQDB) AddDataspace(ctx context.Context, dataspace *Dataspace) error {
	spqrlog.Zero.Debug().
		Str("id", dataspace.ID).
		Msg("etcdqdb: add dataspace")

	resp, err := q.cli.Put(ctx, dataspaceNodePath(dataspace.ID), dataspace.ID)
	if err != nil {
		return err
	}

	spqrlog.Zero.Debug().
		Interface("response", resp).
		Msg("etcdqdb: add dataspace")

	return nil
}

func (q *EtcdQDB) ListDataspaces(ctx context.Context) ([]*Dataspace, error) {
	spqrlog.Zero.Debug().Msg("etcdqdb: list dataspaces")

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

	spqrlog.Zero.Debug().
		Interface("response", resp).
		Msg("etcdqdb: list dataspaces")
	return rules, nil
}
