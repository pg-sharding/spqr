package mem

import (
	"sync"

	"github.com/pg-sharding/spqr/internal/qrouterdb"
	"github.com/pg-sharding/spqr/yacc/spqrparser"
	"golang.org/x/xerrors"
)

type QrouterDBMem struct {
	mu sync.Mutex

	mp map[spqrparser.KeyRange]int
}

func (q *QrouterDBMem) Check(key int) bool {
	q.mu.Lock()
	defer q.mu.Unlock()

	for kr := range q.mp {
		if kr.From <= key && key <= kr.To {
			return true
		}
	}

	return false
}

func NewQrouterDBMem() *QrouterDBMem {
	return &QrouterDBMem{
		mp: map[spqrparser.KeyRange]int{},
	}
}

func (q *QrouterDBMem) Lock(keyRange spqrparser.KeyRange) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	if cnt, ok := q.mp[keyRange]; ok {
		q.mp[keyRange] = cnt + 1
	} else {
		q.mp[keyRange] = 1
	}

	return nil
}

func (q *QrouterDBMem) UnLock(keyRange spqrparser.KeyRange) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	if cnt, ok := q.mp[keyRange]; !ok {
		return xerrors.Errorf("key range %v not locked", keyRange)
	} else if cnt > 1 {
		q.mp[keyRange] = cnt - 1
	} else {
		delete(q.mp, keyRange)
	}

	return nil
}

var _ qrouterdb.QrouterDB = &QrouterDBMem{}
