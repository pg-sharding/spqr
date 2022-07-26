package mem

import (
	"context"
	"fmt"
	"sync"

	"github.com/pg-sharding/spqr/pkg/spqrlog"

	"github.com/pg-sharding/spqr/qdb"
)

type QrouterDBMem struct {
	mu   sync.RWMutex
	txmu sync.Mutex

	freq  map[string]bool
	krs   map[string]*qdb.KeyRange
	locks map[string]*sync.RWMutex

	shards map[string]*qdb.Shard

	shrules []*qdb.ShardingRule
}

func (q *QrouterDBMem) DropShardingRule(ctx context.Context, id string) error {
	//TODO implement me
	panic("implement me")
}

func (q *QrouterDBMem) GetKeyRange(ctx context.Context, id string) (*qdb.KeyRange, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	krs, ok := q.krs[id]
	if !ok {
		return nil, fmt.Errorf("no sush krid")
	}

	return krs, nil
}

func (q *QrouterDBMem) DropKeyRangeAll(ctx context.Context) ([]*qdb.KeyRange, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	var locks []*sync.RWMutex
	for _, l := range q.locks {
		l.Lock()
		locks = append(locks, l)
	}

	spqrlog.Logger.Printf(spqrlog.DEBUG3, "acquired all locks")

	q.krs = map[string]*qdb.KeyRange{}
	q.locks = map[string]*sync.RWMutex{}

	for _, l := range locks {
		l.Unlock()
	}

	return nil, nil
}

func (q *QrouterDBMem) AddShardingRule(ctx context.Context, rule *qdb.ShardingRule) error {
	//TODO implement me
	q.mu.Lock()
	defer q.mu.Unlock()
	spqrlog.Logger.Printf(spqrlog.DEBUG1, "adding sharding rule %v", rule.Colnames)

	q.shrules = append(q.shrules, rule)
	return nil
}

func (q *QrouterDBMem) Share(key *qdb.KeyRange) error {
	spqrlog.Logger.Printf(spqrlog.DEBUG1, "sharing key with key %v", key.KeyRangeID)

	q.locks[key.KeyRangeID].RLock()
	q.locks[key.KeyRangeID].RUnlock()

	return nil
}

func (q *QrouterDBMem) DropKeyRange(ctx context.Context, KeyRangeID string) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	delete(q.krs, KeyRangeID)
	delete(q.freq, KeyRangeID)
	delete(q.locks, KeyRangeID)
	return nil
}

func (q *QrouterDBMem) AddRouter(ctx context.Context, r *qdb.Router) error {
	//TODO implement me
	panic("implement me")
}

func (q *QrouterDBMem) DeleteRouter(ctx context.Context, rID string) error {
	//TODO implement me
	panic("implement me")
}

func (q *QrouterDBMem) ListRouters(ctx context.Context) ([]*qdb.Router, error) {
	//TODO implement me
	panic("implement me")
}

func (q *QrouterDBMem) ListShardingRules(ctx context.Context) ([]*qdb.ShardingRule, error) {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return q.shrules, nil
}

func (q *QrouterDBMem) Watch(krid string, status *qdb.KeyRangeStatus, notifyio chan<- interface{}) error {
	panic("implement me")
}

func (q *QrouterDBMem) AddKeyRange(ctx context.Context, keyRange *qdb.KeyRange) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	q.krs[keyRange.KeyRangeID] = keyRange
	q.locks[keyRange.KeyRangeID] = &sync.RWMutex{}

	return nil
}

func (q *QrouterDBMem) UpdateKeyRange(_ context.Context, keyRange *qdb.KeyRange) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	q.krs[keyRange.KeyRangeID] = keyRange

	return nil
}

func (q *QrouterDBMem) Check(_ context.Context, kr *qdb.KeyRange) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	_, ok := q.krs[kr.KeyRangeID]
	return !ok
}

func NewQrouterDBMem() (*QrouterDBMem, error) {
	return &QrouterDBMem{
		freq:   map[string]bool{},
		krs:    map[string]*qdb.KeyRange{},
		locks:  map[string]*sync.RWMutex{},
		shards: map[string]*qdb.Shard{},
	}, nil
}

func (q *QrouterDBMem) Lock(_ context.Context, KeyRangeID string) (*qdb.KeyRange, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	krs, ok := q.krs[KeyRangeID]
	if !ok {
		return nil, fmt.Errorf("no sush krid")
	}

	q.freq[KeyRangeID] = true
	q.locks[KeyRangeID].Lock()

	return krs, nil
}

func (q *QrouterDBMem) CheckLocked(ctx context.Context, KeyRangeID string) (*qdb.KeyRange, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	krs, ok := q.krs[KeyRangeID]
	if !ok {
		return nil, fmt.Errorf("no sush krid")
	}

	if !q.freq[KeyRangeID] {
		return nil, fmt.Errorf("key range %v not locked", KeyRangeID)
	}

	return krs, nil
}

func (q *QrouterDBMem) Unlock(_ context.Context, KeyRangeID string) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	if !q.freq[KeyRangeID] {
		return fmt.Errorf("key range %v not locked", KeyRangeID)
	}

	q.locks[KeyRangeID].Unlock()

	return nil
}

func (q *QrouterDBMem) ListKeyRanges(_ context.Context) ([]*qdb.KeyRange, error) {
	q.mu.RLock()
	defer q.mu.RUnlock()
	var ret []*qdb.KeyRange

	for _, el := range q.krs {
		ret = append(ret, el)
	}

	return ret, nil
}

func (q *QrouterDBMem) ListShards(ctx context.Context) ([]*qdb.Shard, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	var ret []*qdb.Shard
	for k := range q.shards {
		ret = append(ret, &qdb.Shard{
			ID: k,
		})
	}
	return ret, nil
}

func (q *QrouterDBMem) AddShard(ctx context.Context, shard *qdb.Shard) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	q.shards[shard.ID] = shard
	return nil
}

func (q *QrouterDBMem) GetShardInfo(ctx context.Context, shardID string) (*qdb.Shard, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if _, ok := q.shards[shardID]; ok {
		return &qdb.Shard{ID: shardID}, nil
	}

	return nil, fmt.Errorf("unknown shard %s", shardID)
}

var _ qdb.QrouterDB = &QrouterDBMem{}
