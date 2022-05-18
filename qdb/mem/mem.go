package mem

import (
	"context"
	"fmt"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/pkg/spqrlog"
	"sync"

	"github.com/pg-sharding/spqr/qdb"
	"golang.org/x/xerrors"
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
	qdb.QrouterDB

	mu   sync.Mutex
	txmu sync.Mutex

	freq map[string]int
	krs  map[string]*qdb.KeyRange

	krWaiters map[string]*WaitPool
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
		krWaiters: map[string]*WaitPool{},
	}, nil
}

func (q *QrouterDBMem) Lock(_ context.Context, KeyRangeID string) (*qdb.KeyRange, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	krs := q.krs[KeyRangeID]

	if cnt, ok := q.freq[KeyRangeID]; ok {
		q.freq[KeyRangeID] = cnt + 1
	} else {
		q.freq[KeyRangeID] = 1
	}

	return krs, nil
}

func (q *QrouterDBMem) UnLock(_ context.Context, KeyRangeID string) error {
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

	return nil
}

func (q *QrouterDBMem) ListKeyRanges(_ context.Context) ([]*qdb.KeyRange, error) {
	var ret []*qdb.KeyRange

	for _, el := range q.krs {
		ret = append(ret, el)
	}

	return ret, nil
}

var _ qdb.QrouterDB = &QrouterDBMem{}
