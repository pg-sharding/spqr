package mem

import (
	"sync"

	"github.com/pg-sharding/spqr/internal/qdb"
	"golang.org/x/xerrors"
)

type QrouterDBMem struct {
	mu   sync.Mutex
	txmu sync.Mutex

	freq map[string]int
	krs  map[string]qdb.KeyRange
}

func (q *QrouterDBMem) Begin() error {
	q.txmu.Lock()

	return nil
}

func (q *QrouterDBMem) Commit() error {
	q.txmu.Unlock()
	return nil
}

func (q *QrouterDBMem) Add(keyRange qdb.KeyRange) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	if _, ok := q.krs[keyRange.KeyRangeID]; ok {
		return xerrors.Errorf("key range %v already present in qdb", keyRange.KeyRangeID)
	}

	q.freq[keyRange.KeyRangeID] = 1
	q.krs[keyRange.KeyRangeID] = keyRange

	return nil
}

func (q *QrouterDBMem) Update(keyRange qdb.KeyRange) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	q.krs[keyRange.KeyRangeID] = keyRange

	return nil
}

func (q *QrouterDBMem) Check(kr qdb.KeyRange) bool {
	q.mu.Lock()
	defer q.mu.Unlock()

	_, ok := q.krs[kr.KeyRangeID]
	return !ok
}

func NewQrouterDBMem() (*QrouterDBMem, error) {
	return &QrouterDBMem{
		freq: map[string]int{},
		krs:  map[string]qdb.KeyRange{},
	}, nil
}

func (q *QrouterDBMem) Lock(keyRange qdb.KeyRange) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	if cnt, ok := q.freq[keyRange.KeyRangeID]; ok {
		q.freq[keyRange.KeyRangeID] = cnt + 1
	} else {
		q.freq[keyRange.KeyRangeID] = 1
	}

	q.krs[keyRange.KeyRangeID] = keyRange

	return nil
}

func (q *QrouterDBMem) UnLock(keyRange qdb.KeyRange) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	if cnt, ok := q.freq[keyRange.KeyRangeID]; !ok {
		return xerrors.Errorf("key range %v not locked", keyRange)
	} else if cnt > 1 {
		q.freq[keyRange.KeyRangeID] = cnt - 1
	} else {
		delete(q.freq, keyRange.KeyRangeID)
		delete(q.krs, keyRange.KeyRangeID)
	}

	return nil
}

var _ qdb.QrouterDB = &QrouterDBMem{}
