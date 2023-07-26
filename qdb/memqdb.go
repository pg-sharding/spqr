package qdb

import (
	"context"
	"fmt"
	"sort"
	"sync"

	"github.com/pg-sharding/spqr/pkg/spqrlog"
)

type MemQDB struct {
	mu sync.RWMutex

	freq         map[string]bool
	krs          map[string]*KeyRange
	locks        map[string]*sync.RWMutex
	shards       map[string]*Shard
	shrules      map[string]*ShardingRule
	dataspaces   map[string]*Dataspace
	routers      map[string]*Router
	transactions map[string]*DataTransferTransaction

	/* caches */
}

var _ QDB = &MemQDB{}

func NewMemQDB() (*MemQDB, error) {
	return &MemQDB{
		freq:         map[string]bool{},
		krs:          map[string]*KeyRange{},
		locks:        map[string]*sync.RWMutex{},
		shards:       map[string]*Shard{},
		shrules:      map[string]*ShardingRule{},
		dataspaces:   map[string]*Dataspace{},
		routers:      map[string]*Router{},
		transactions: map[string]*DataTransferTransaction{},
	}, nil
}

// ==============================================================================
//                               SHARDING RULES
// ==============================================================================

func (q *MemQDB) AddShardingRule(ctx context.Context, rule *ShardingRule) error {
	spqrlog.Zero.Debug().Interface("rule", rule).Msg("memqdb: add sharding rule")
	q.mu.Lock()
	defer q.mu.Unlock()

	q.shrules[rule.ID] = rule
	return nil
}

func (q *MemQDB) DropShardingRule(ctx context.Context, id string) error {
	spqrlog.Zero.Debug().Str("rule", id).Msg("memqdb: drop sharding rule")
	q.mu.Lock()
	defer q.mu.Unlock()

	delete(q.shrules, id)

	return nil
}

func (q *MemQDB) DropShardingRuleAll(ctx context.Context) ([]*ShardingRule, error) {
	spqrlog.Zero.Debug().Msg("memqdb: drop sharding rule all")
	q.mu.Lock()
	defer q.mu.Unlock()

	var ret []*ShardingRule
	for _, v := range q.shrules {
		ret = append(ret, v)
	}

	sort.Slice(ret, func(i, j int) bool {
		return ret[i].ID < ret[j].ID
	})

	q.shrules = make(map[string]*ShardingRule)

	return ret, nil
}

func (q *MemQDB) GetShardingRule(ctx context.Context, id string) (*ShardingRule, error) {
	spqrlog.Zero.Debug().Str("rule", id).Msg("memqdb: get sharding rule")
	rule, ok := q.shrules[id]
	if ok {
		return rule, nil
	}
	return nil, fmt.Errorf("rule with id %s not found", id)
}

func (q *MemQDB) ListShardingRules(ctx context.Context) ([]*ShardingRule, error) {
	spqrlog.Zero.Debug().Msg("memqdb: list sharding rules")
	q.mu.RLock()
	defer q.mu.RUnlock()
	var ret []*ShardingRule
	for _, v := range q.shrules {
		ret = append(ret, v)
	}

	sort.Slice(ret, func(i, j int) bool {
		return ret[i].ID < ret[j].ID
	})

	return ret, nil
}

func (q *MemQDB) MatchShardingRules(ctx context.Context, m func(shrules map[string]*ShardingRule) error) error {
	spqrlog.Zero.Debug().Msg("memqdb: list sharding rules")
	q.mu.RLock()
	defer q.mu.RUnlock()
	return m(q.shrules)
}

// ==============================================================================
//                                 KEY RANGES
// ==============================================================================

func (q *MemQDB) AddKeyRange(ctx context.Context, keyRange *KeyRange) error {
	spqrlog.Zero.Debug().Interface("key-range", keyRange).Msg("memqdb: add key range")
	q.mu.Lock()
	defer q.mu.Unlock()

	q.krs[keyRange.KeyRangeID] = keyRange
	q.locks[keyRange.KeyRangeID] = &sync.RWMutex{}
	q.freq[keyRange.KeyRangeID] = false

	return nil
}

func (q *MemQDB) GetKeyRange(ctx context.Context, id string) (*KeyRange, error) {
	spqrlog.Zero.Debug().Str("key-range", id).Msg("memqdb: get key range")
	q.mu.Lock()
	defer q.mu.Unlock()

	krs, ok := q.krs[id]
	if !ok {
		return nil, fmt.Errorf("there is no key range %s", id)
	}

	return krs, nil
}

func (q *MemQDB) UpdateKeyRange(ctx context.Context, keyRange *KeyRange) error {
	spqrlog.Zero.Debug().Interface("key-range", keyRange).Msg("memqdb: update key range")
	q.mu.Lock()
	defer q.mu.Unlock()

	q.krs[keyRange.KeyRangeID] = keyRange

	return nil
}

func (q *MemQDB) DropKeyRange(ctx context.Context, id string) error {
	spqrlog.Zero.Debug().Str("key-range", id).Msg("memqdb: drop key range")
	q.mu.Lock()
	defer q.mu.Unlock()

	delete(q.krs, id)
	delete(q.freq, id)
	delete(q.locks, id)

	return nil
}

func (q *MemQDB) DropKeyRangeAll(ctx context.Context) error {
	spqrlog.Zero.Debug().Msg("memqdb: drop all key ranges")
	q.mu.Lock()
	defer q.mu.Unlock()

	var locks []*sync.RWMutex
	for _, l := range q.locks {
		l.Lock()
		locks = append(locks, l)
	}

	spqrlog.Zero.Debug().Msg("memqdb: acquired all locks")

	q.krs = map[string]*KeyRange{}
	q.locks = map[string]*sync.RWMutex{}

	for _, l := range locks {
		l.Unlock()
	}

	return nil
}

func (q *MemQDB) ListKeyRanges(_ context.Context) ([]*KeyRange, error) {
	spqrlog.Zero.Debug().Msg("memqdb: list all key ranges")
	q.mu.RLock()
	defer q.mu.RUnlock()

	var ret []*KeyRange

	for _, el := range q.krs {
		ret = append(ret, el)
	}

	sort.Slice(ret, func(i, j int) bool {
		return ret[i].KeyRangeID < ret[j].KeyRangeID
	})

	return ret, nil
}

func (q *MemQDB) LockKeyRange(_ context.Context, id string) (*KeyRange, error) {
	spqrlog.Zero.Debug().Str("key-range", id).Msg("memqdb: lock key range")
	q.mu.Lock()
	defer q.mu.Unlock()

	krs, ok := q.krs[id]
	if !ok {
		return nil, fmt.Errorf("key range '%s' does not exist", id)
	}

	q.freq[id] = true
	q.locks[id].Lock()

	return krs, nil
}

func (q *MemQDB) UnlockKeyRange(_ context.Context, id string) error {
	spqrlog.Zero.Debug().Str("key-range", id).Msg("memqdb: lock key range")
	q.mu.Lock()
	defer q.mu.Unlock()

	if !q.freq[id] {
		return fmt.Errorf("key range %v not locked", id)
	}

	q.freq[id] = false

	q.locks[id].Unlock()

	return nil
}

func (q *MemQDB) CheckLockedKeyRange(ctx context.Context, id string) (*KeyRange, error) {
	spqrlog.Zero.Debug().Str("key-range", id).Msg("memqdb: check locked key range")
	q.mu.Lock()
	defer q.mu.Unlock()

	krs, ok := q.krs[id]
	if !ok {
		return nil, fmt.Errorf("no sush krid")
	}

	if !q.freq[id] {
		return nil, fmt.Errorf("key range %v not locked", id)
	}

	return krs, nil
}

func (q *MemQDB) ShareKeyRange(id string) error {
	spqrlog.Zero.Debug().Str("key-range", id).Msg("memqdb: sharing key with key")

	q.locks[id].RLock()
	defer q.locks[id].RUnlock()

	return nil
}

// ==============================================================================
//                           Transfer transactions
// ==============================================================================

func (q *MemQDB) RecordTransferTx(ctx context.Context, key string, info *DataTransferTransaction) error {
	q.transactions[key] = info
	return nil
}

func (q *MemQDB) GetTransferTx(ctx context.Context, key string) (*DataTransferTransaction, error) {
	return q.transactions[key], nil
}

func (q *MemQDB) RemoveTransferTx(ctx context.Context, key string) error {
	delete(q.transactions, key)
	return nil
}

// ==============================================================================
//                                  ROUTERS
// ==============================================================================

func (q *MemQDB) AddRouter(ctx context.Context, r *Router) error {
	spqrlog.Zero.Debug().Interface("router", r).Msg("memqdb: add router")
	q.mu.Lock()
	defer q.mu.Unlock()

	q.routers[r.ID] = r
	return nil
}

func (q *MemQDB) DeleteRouter(ctx context.Context, id string) error {
	spqrlog.Zero.Debug().Str("router", id).Msg("memqdb: delete router")
	q.mu.Lock()
	defer q.mu.Unlock()

	delete(q.routers, id)
	return nil
}

func (q *MemQDB) ListRouters(ctx context.Context) ([]*Router, error) {
	spqrlog.Zero.Debug().Msg("memqdb: list routers")
	q.mu.Lock()
	defer q.mu.Unlock()

	var ret []*Router
	for _, v := range q.routers {
		// TODO replace with new
		ret = append(ret, v)
	}

	sort.Slice(ret, func(i, j int) bool {
		return ret[i].ID < ret[j].ID
	})

	return ret, nil
}

func (q *MemQDB) LockRouter(ctx context.Context, id string) error {
	return nil
}

// ==============================================================================
//                                  SHARDS
// ==============================================================================

func (q *MemQDB) AddShard(ctx context.Context, shard *Shard) error {
	spqrlog.Zero.Debug().Interface("shard", shard).Msg("memqdb: add shard")
	q.mu.Lock()
	defer q.mu.Unlock()

	q.shards[shard.ID] = shard
	return nil
}

func (q *MemQDB) ListShards(ctx context.Context) ([]*Shard, error) {
	spqrlog.Zero.Debug().Msg("memqdb: list shards")
	q.mu.Lock()
	defer q.mu.Unlock()

	var ret []*Shard
	for _, v := range q.shards {
		// TODO replace with new
		ret = append(ret, v)
	}

	sort.Slice(ret, func(i, j int) bool {
		return ret[i].ID < ret[j].ID
	})

	return ret, nil
}

func (q *MemQDB) GetShard(ctx context.Context, id string) (*Shard, error) {
	spqrlog.Zero.Debug().Str("shard", id).Msg("memqdb: get shard")
	q.mu.Lock()
	defer q.mu.Unlock()

	if _, ok := q.shards[id]; ok {
		return &Shard{ID: id}, nil
	}

	return nil, fmt.Errorf("unknown shard %s", id)
}

// ==============================================================================
//                                 DATASPACES
// ==============================================================================

func (q *MemQDB) AddDataspace(ctx context.Context, dataspace *Dataspace) error {
	spqrlog.Zero.Debug().Interface("dataspace", dataspace).Msg("memqdb: add dataspace")
	q.mu.Lock()
	defer q.mu.Unlock()
	q.dataspaces[dataspace.ID] = dataspace

	return nil
}

func (q *MemQDB) ListDataspaces(ctx context.Context) ([]*Dataspace, error) {
	spqrlog.Zero.Debug().Msg("memqdb: list dataspaces")
	q.mu.RLock()
	defer q.mu.RUnlock()
	var ret []*Dataspace
	for _, v := range q.dataspaces {
		ret = append(ret, v)
	}

	sort.Slice(ret, func(i, j int) bool {
		return ret[i].ID < ret[j].ID
	})

	return ret, nil
}
