package qdb

import (
	"sync"

	"github.com/jackc/pgx/v5/pgxpool"
)

type PgQDB struct {
	mu sync.Mutex

	Shards map[string]*Shard
	pooler map[string]*pgxpool.Pool
}

// AcquireTxOwnership implements [DCStateKeeper].
func (q *PgQDB) AcquireTxOwnership(gid string) bool {
	panic("unimplemented")
}

// ChangeTxStatus implements [DCStateKeeper].
func (q *PgQDB) ChangeTxStatus(gid string, state string) error {
	panic("unimplemented")
}

// RecordTwoPhaseMembers implements [DCStateKeeper].
func (q *PgQDB) RecordTwoPhaseMembers(gid string, shards []string) error {
	panic("unimplemented")
}

// ReleaseTxOwnership implements [DCStateKeeper].
func (q *PgQDB) ReleaseTxOwnership(gid string) {
	panic("unimplemented")
}

// TXCohortShards implements [DCStateKeeper].
func (q *PgQDB) TXCohortShards(gid string) []string {
	panic("unimplemented")
}

// TXStatus implements [DCStateKeeper].
func (q *PgQDB) TXStatus(gid string) string {
	panic("unimplemented")
}

func NewPgQDB(shards map[string]*Shard) *PgQDB {
	return &PgQDB{
		Shards: shards,
		pooler: make(map[string]*pgxpool.Pool),
	}
}

var _ DCStateKeeper = &PgQDB{}
