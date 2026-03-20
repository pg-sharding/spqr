package qdb

import (
	"context"
	"fmt"
)

type MemPgQDB struct {
	*MemQDB

	pgDb *PgQDB
}

var _ XQDB = &MemPgQDB{}
var _ XDCStateKeeper = &MemPgQDB{}

func NewMemPgQDB(backupPath string) (*MemPgQDB, error) {
	memQDB, err := NewMemQDB(backupPath)
	if err != nil {
		return nil, err
	}
	return &MemPgQDB{
		MemQDB: memQDB,
		pgDb:   NewPgQDB([]*Shard{}),
	}, nil
}

// AcquireTxOwnership implements [DCStateKeeper].
func (q *MemPgQDB) AcquireTxOwnership(txid string) bool {
	return q.pgDb.AcquireTxOwnership(txid)
}

// ChangeTxStatus implements [DCStateKeeper].
func (q *MemPgQDB) ChangeTxStatus(txid string, state TwoPhaseTxState) error {
	return q.pgDb.ChangeTxStatus(txid, state)
}

// RecordTwoPhaseMembers implements [DCStateKeeper].
func (q *MemPgQDB) RecordTwoPhaseMembers(txid string, shards []string) error {
	return q.pgDb.RecordTwoPhaseMembers(txid, shards)
}

// ReleaseTxOwnership implements [DCStateKeeper].
func (q *MemPgQDB) ReleaseTxOwnership(txid string) {
	q.pgDb.ReleaseTxOwnership(txid)
}

// TXCohortShards implements [DCStateKeeper].
func (q *MemPgQDB) TXCohortShards(txid string) ([]string, error) {
	return q.pgDb.TXCohortShards(txid)
}

// TXStatus implements [DCStateKeeper].
func (q *MemPgQDB) TXStatus(txid string) (TwoPhaseTxState, error) {
	return q.TXStatus(txid)
}

// TODO : unit tests
func (q *MemPgQDB) AddShard(_ context.Context, shard *Shard) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	if _, ok := q.Shards[shard.ID]; ok {
		return fmt.Errorf("shard with id %s already exists", shard.ID)
	}

	if err := q.pgDb.AddShard(shard); err != nil {
		return err
	}

	return ExecuteCommands(q.DumpState, NewUpdateCommand(q.Shards, shard.ID, shard))
}

func (q *MemPgQDB) DropShard(_ context.Context, id string) error {
	return fmt.Errorf("method DropShard ubsupported in MemPgQDB")
}
