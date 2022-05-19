package mem

import (
	"context"
	"fmt"
	"sync"

	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/pkg/models/shrule"
	"github.com/pg-sharding/spqr/pkg/spqrlog"

	"golang.org/x/xerrors"

	"github.com/pg-sharding/spqr/qdb"
)

type WaitPool struct {
	stopCh    chan struct{}
	publishCh chan interface{}
	subCh     chan chan<- interface{}
	unsubCh   chan chan<- interface{}
}

func NewWaitPool() *WaitPool {
	return &WaitPool{
		stopCh:    make(chan struct{}),
		publishCh: make(chan interface{}, 1),
		subCh:     make(chan chan<- interface{}, 1),
		unsubCh:   make(chan chan<- interface{}, 1),
	}
}

func (wp *WaitPool) Start() {
	waiters := map[chan<- interface{}]struct{}{}

	for {
		select {
		case <-wp.stopCh:
			// notify all cl
			return
		case msgCh := <-wp.subCh:
			waiters[msgCh] = struct{}{}
		case msgCh := <-wp.unsubCh:
			delete(waiters, msgCh)
		case msg := <-wp.publishCh:
			for msgCh := range waiters {
				select {
				case msgCh <- msg:
				default:
				}
			}
		}
	}
}

func (wg *WaitPool) Subscribe(status *qdb.KeyRangeStatus, notifyio chan<- interface{}) error {
	wg.subCh <- notifyio
	return nil
}

func (wg *WaitPool) Unsubscribe(msgCh chan interface{}) {
	wg.unsubCh <- msgCh
}

func (wg *WaitPool) Publish(msg interface{}) {
	wg.publishCh <- msg
}

type QrouterDBMem struct {
	mu   sync.RWMutex
	txmu sync.Mutex

	freq  map[string]int
	krs   map[string]*qdb.KeyRange
	locks map[string]*sync.RWMutex

	shrules []*shrule.ShardingRule

	krWaiters map[string]*WaitPool
}

func (q *QrouterDBMem) AddShardingRule(ctx context.Context, rule *shrule.ShardingRule) error {
	//TODO implement me
	q.mu.Lock()
	defer q.mu.Unlock()
	spqrlog.Logger.Printf(spqrlog.DEBUG1, "adding sharding rule %v", rule.Columns())

	q.shrules = append(q.shrules, rule)
	return nil
}

func (q *QrouterDBMem) Share(key *qdb.KeyRange) error {
	spqrlog.Logger.Printf(spqrlog.DEBUG1, "sharing key with key %v", key.KeyRangeID)

	q.locks[key.KeyRangeID].RLock()
	q.locks[key.KeyRangeID].RUnlock()

	return nil
}

func (q *QrouterDBMem) CheckShardingRule(ctx context.Context, colnames []string) bool {
	//TODO implement me
	q.mu.Lock()
	defer q.mu.Unlock()

	spqrlog.Logger.Printf(spqrlog.DEBUG5, "checking with %d rules", len(q.shrules))

	for _, rule := range q.shrules {
		spqrlog.Logger.Printf(spqrlog.DEBUG5, "checking %+v against %+v", rule.Columns(), colnames)
		if len(rule.Columns()) != len(colnames) {
			continue
		}
		ok := true

		for j := 0; j < len(colnames); j++ {
			if rule.Columns()[j] != colnames[j] {
				ok = false
				break
			}
		}

		if ok {
			return true
		}
	}

	return false
}

//func (q *QrouterDBMem) Split(ctx context.Context, req *qdb.SplitKeyRange) error {
//	//TODO implement me
//	panic("implement me")
//}
//
//func (q *QrouterDBMem) Unite(ctx context.Context, req *kr.UniteKeyRange) error {
//	//TODO implement me
//	panic("implement me")
//}

func (q *QrouterDBMem) DropKeyRange(ctx context.Context, krs *qdb.KeyRange) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	delete(q.krs, krs.KeyRangeID)
	delete(q.freq, krs.KeyRangeID)
	delete(q.locks, krs.KeyRangeID)
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

func (q *QrouterDBMem) ListShardingRules(ctx context.Context) ([]*shrule.ShardingRule, error) {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return q.shrules, nil
}

func (q *QrouterDBMem) Watch(krid string, status *qdb.KeyRangeStatus, notifyio chan<- interface{}) error {
	return q.krWaiters[krid].Subscribe(status, notifyio)
}

func (q *QrouterDBMem) AddKeyRange(ctx context.Context, keyRange *qdb.KeyRange) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	spqrlog.Logger.Printf(spqrlog.DEBUG1, "adding key range %+v", keyRange)

	if _, ok := q.krs[keyRange.KeyRangeID]; ok {
		return fmt.Errorf("key range %v already present in qdb", keyRange.KeyRangeID)
	}

	for _, v := range q.krs {

		if kr.CmpRanges(keyRange.LowerBound, v.LowerBound) && kr.CmpRanges(v.LowerBound, keyRange.UpperBound) || kr.CmpRanges(keyRange.LowerBound, v.UpperBound) && kr.CmpRanges(v.UpperBound, keyRange.UpperBound) {
			return fmt.Errorf("key range %v intersects with %v present in qdb", keyRange.KeyRangeID, v.KeyRangeID)
		}
	}

	q.freq[keyRange.KeyRangeID] = 1
	q.krs[keyRange.KeyRangeID] = keyRange
	q.locks[keyRange.KeyRangeID] = &sync.RWMutex{}

	return nil
}

func (q *QrouterDBMem) UpdateKeyRange(_ context.Context, keyRange *qdb.KeyRange) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	for _, v := range q.krs {
		if kr.CmpRanges(keyRange.LowerBound, v.LowerBound) && kr.CmpRanges(v.LowerBound, keyRange.UpperBound) || kr.CmpRanges(keyRange.LowerBound, v.UpperBound) && kr.CmpRanges(v.UpperBound, keyRange.UpperBound) {
			return fmt.Errorf("key range %v intersects with %v present in qdb", keyRange.KeyRangeID, v.KeyRangeID)
		}
	}

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
		freq:      map[string]int{},
		krs:       map[string]*qdb.KeyRange{},
		locks:     map[string]*sync.RWMutex{},
		krWaiters: map[string]*WaitPool{},
	}, nil
}

func (q *QrouterDBMem) Lock(_ context.Context, KeyRangeID string) (*qdb.KeyRange, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	krs, ok := q.krs[KeyRangeID]
	if !ok {
		return nil, fmt.Errorf("no sush krid")
	}

	if cnt, ok := q.freq[KeyRangeID]; ok {
		q.freq[KeyRangeID] = cnt + 1
	} else {
		q.freq[KeyRangeID] = 1
	}
	q.locks[KeyRangeID].Lock()

	return krs, nil
}

func (q *QrouterDBMem) Unlock(_ context.Context, KeyRangeID string) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	if cnt, ok := q.freq[KeyRangeID]; !ok {
		return xerrors.Errorf("key range %v not locked", KeyRangeID)
	} else if cnt > 1 {
		q.freq[KeyRangeID] = cnt - 1
	} else {
		delete(q.freq, KeyRangeID)
		delete(q.krs, KeyRangeID)
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
	//TODO implement me
	panic("implement me")
}

func (q *QrouterDBMem) AddShard(ctx context.Context, shard *qdb.Shard) error {
	//TODO implement me
	panic("implement me")
}

func (q *QrouterDBMem) GetShardInfo(ctx context.Context, shardID string) (*qdb.ShardInfo, error) {
	//TODO implement me
	panic("implement me")
}

var _ qdb.QrouterDB = &QrouterDBMem{}
