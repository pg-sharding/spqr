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

	freq       map[string]bool
	krs        map[string]*KeyRange
	locks      map[string]*sync.RWMutex
	shards     map[string]*Shard
	shrules    map[string]*ShardingRule
	dataspaces map[string]*Dataspace
}

var _ QDB = &MemQDB{}

func NewMemQDB() (*MemQDB, error) {
	return &MemQDB{
		freq:       map[string]bool{},
		krs:        map[string]*KeyRange{},
		locks:      map[string]*sync.RWMutex{},
		shards:     map[string]*Shard{},
		shrules:    map[string]*ShardingRule{},
		dataspaces: map[string]*Dataspace{},
	}, nil
}

// ==============================================================================
//                               SHARDING RULES
// ==============================================================================

func (q *MemQDB) AddShardingRule(ctx context.Context, rule *ShardingRule) error {
	spqrlog.Logger.Printf(spqrlog.DEBUG1, "add sharding rule %v", rule.Entries[0].Column)
	q.mu.Lock()
	defer q.mu.Unlock()

	q.shrules[rule.ID] = rule
	return nil
}

func (q *MemQDB) DropShardingRule(ctx context.Context, id string) error {
	//TODO implement me
	q.mu.Lock()
	defer q.mu.Unlock()

	delete(q.shrules, id)

	return nil
}

func (q *MemQDB) DropShardingRuleAll(ctx context.Context) ([]*ShardingRule, error) {

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
	spqrlog.Logger.Printf(spqrlog.LOG, "memqdb: get sharding rule %v", id)
	rule, ok := q.shrules[id]
	if ok {
		return rule, nil
	}
	return nil, fmt.Errorf("rule with id %s not found", id)
}

func (q *MemQDB) ListShardingRules(ctx context.Context) ([]*ShardingRule, error) {
	spqrlog.Logger.Printf(spqrlog.LOG, "memqdb: list sharding rules")
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

// ==============================================================================
//                                 KEY RANGES
// ==============================================================================

func (q *MemQDB) AddKeyRange(ctx context.Context, keyRange *KeyRange) error {
	spqrlog.Logger.Printf(spqrlog.LOG, "memqdb: add key range %+v", keyRange)
	q.mu.Lock()
	defer q.mu.Unlock()

	q.krs[keyRange.KeyRangeID] = keyRange
	q.locks[keyRange.KeyRangeID] = &sync.RWMutex{}
	q.freq[keyRange.KeyRangeID] = false

	return nil
}

func (q *MemQDB) GetKeyRange(ctx context.Context, id string) (*KeyRange, error) {
	spqrlog.Logger.Printf(spqrlog.LOG, "memqdb: get key range %v", id)
	q.mu.Lock()
	defer q.mu.Unlock()

	krs, ok := q.krs[id]
	if !ok {
		return nil, fmt.Errorf("there is no key range %s", id)
	}

	return krs, nil
}

func (q *MemQDB) UpdateKeyRange(ctx context.Context, keyRange *KeyRange) error {
	spqrlog.Logger.Printf(spqrlog.LOG, "memqdb: update key range %+v", keyRange)
	q.mu.Lock()
	defer q.mu.Unlock()

	q.krs[keyRange.KeyRangeID] = keyRange

	return nil
}

func (q *MemQDB) DropKeyRange(ctx context.Context, KeyRangeID string) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	delete(q.krs, KeyRangeID)
	delete(q.freq, KeyRangeID)
	delete(q.locks, KeyRangeID)
	return nil
}

func (q *MemQDB) DropKeyRangeAll(ctx context.Context) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	var locks []*sync.RWMutex
	for _, l := range q.locks {
		l.Lock()
		locks = append(locks, l)
	}

	spqrlog.Logger.Printf(spqrlog.DEBUG3, "acquired all locks")

	q.krs = map[string]*KeyRange{}
	q.locks = map[string]*sync.RWMutex{}

	for _, l := range locks {
		l.Unlock()
	}

	return nil
}

func (q *MemQDB) ListKeyRanges(_ context.Context) ([]*KeyRange, error) {
	spqrlog.Logger.Printf(spqrlog.LOG, "memqdb: list all key ranges")
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
	spqrlog.Logger.Printf(spqrlog.LOG, "memqdb: lock key range %+v", id)
	q.mu.Lock()
	defer q.mu.Unlock()

	krs, ok := q.krs[id]
	if !ok {
		return nil, fmt.Errorf("no sush krid")
	}

	q.freq[id] = true
	q.locks[id].Lock()

	return krs, nil
}

func (q *MemQDB) UnlockKeyRange(_ context.Context, id string) error {
	spqrlog.Logger.Printf(spqrlog.LOG, "memqdb: lock key range %+v", id)
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
	spqrlog.Logger.Printf(spqrlog.LOG, "memqdb: check locked key range %+v", id)
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
	spqrlog.Logger.Printf(spqrlog.LOG, "memqdb: sharing key with key %v", id)

	q.locks[id].RLock()
	defer q.locks[id].RUnlock()

	return nil
}

// ==============================================================================
//                                  ROUTERS
// ==============================================================================

func (q *MemQDB) AddRouter(ctx context.Context, r *Router) error {
	panic("implement me")
}

func (q *MemQDB) DeleteRouter(ctx context.Context, id string) error {
	panic("implement me")
}

func (q *MemQDB) ListRouters(ctx context.Context) ([]*Router, error) {
	panic("implement me")
}

func (q *MemQDB) LockRouter(ctx context.Context, id string) error {
	return nil
}

// ==============================================================================
//                                  SHARDS
// ==============================================================================

func (q *MemQDB) AddShard(ctx context.Context, shard *Shard) error {
	spqrlog.Logger.Printf(spqrlog.LOG, "memqdb: add shard %+v", shard)
	q.mu.Lock()
	defer q.mu.Unlock()

	q.shards[shard.ID] = shard
	return nil
}

func (q *MemQDB) ListShards(ctx context.Context) ([]*Shard, error) {
	spqrlog.Logger.Printf(spqrlog.LOG, "memqdb: list shards")
	q.mu.Lock()
	defer q.mu.Unlock()

	var ret []*Shard
	for k := range q.shards {
		// TODO replace with new
		ret = append(ret, &Shard{
			ID: k,
		})
	}

	sort.Slice(ret, func(i, j int) bool {
		return ret[i].ID < ret[j].ID
	})

	return ret, nil
}

func (q *MemQDB) GetShard(ctx context.Context, id string) (*Shard, error) {
	spqrlog.Logger.Printf(spqrlog.LOG, "memqdb: get shard %v", id)
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
	spqrlog.Logger.Printf(spqrlog.LOG, "memqdb: add dataspace %+v", dataspace)
	q.mu.Lock()
	defer q.mu.Unlock()
	q.dataspaces[dataspace.ID] = dataspace

	return nil
}

func (q *MemQDB) ListDataspaces(ctx context.Context) ([]*Dataspace, error) {
	spqrlog.Logger.Printf(spqrlog.LOG, "memqdb: list dataspaces")
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
