package qdb

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	"github.com/pg-sharding/spqr/pkg/spqrlog"
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

func RestoreMemPgQDB(backupPath string) (*MemPgQDB, error) {
	qdb, err := NewMemPgQDB(backupPath)
	if err != nil {
		return nil, err
	}
	if backupPath == "" {
		return qdb, nil
	}
	if _, err := os.Stat(backupPath); err != nil {
		spqrlog.Zero.Info().Err(err).Msg("mempgqdb backup file not exists. Creating new one")
		f, err := os.Create(backupPath)
		if err != nil {
			return nil, err
		}
		defer func(file *os.File) {
			err := file.Close()
			if err != nil {
				spqrlog.Zero.Debug().Err(err).Msg("failed to close file")
			}
		}(f)
		return qdb, nil
	}
	data, err := os.ReadFile(backupPath)
	if err != nil {
		return nil, err
	}
	err = json.Unmarshal(data, qdb)

	for kr, locked := range qdb.Freq {
		if locked {
			qdb.Locks[kr].Lock()
		}
	}

	if err != nil {
		return nil, err
	}
	return qdb, nil
}

// AcquireTxOwnership implements [DCStateKeeper].
func (q *MemPgQDB) AcquireTxOwnership(txid string) (bool, error) {
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
func (q *MemPgQDB) ReleaseTxOwnership(txid string) error {
	return q.pgDb.ReleaseTxOwnership(txid)
}

// TXCohortShards implements [DCStateKeeper].
func (q *MemPgQDB) TXCohortShards(txid string) ([]string, error) {
	return q.pgDb.TXCohortShards(txid)
}

// TXStatus implements [DCStateKeeper].
func (q *MemPgQDB) TXStatus(txid string) (TwoPhaseTxState, error) {
	return q.TXStatus(txid)
}

func (q *MemPgQDB) ListTXNames() ([]string, error) {
	return q.pgDb.ListTXNames()
}

// TODO : unit tests
func (q *MemPgQDB) AddShard(_ context.Context, shard *Shard) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	if _, ok := q.Shards[shard.ID]; ok {
		return fmt.Errorf("shard with id %s already exists", shard.ID)
	}

	return ExecuteCommands(q.DumpState, NewUpdateCommand(q.Shards, shard.ID, shard), NewAppendCommand(q.pgDb.Shards, shard))
}

func (q *MemPgQDB) DropShard(_ context.Context, id string) error {
	return fmt.Errorf("method DropShard ubsupported in MemPgQDB")
}
