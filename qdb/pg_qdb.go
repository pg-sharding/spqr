package qdb

import (
	"context"
	"fmt"
	"sync"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Table declaration
// CREATE TYPE tx_status AS ENUM ('planned', 'committing', 'committed', 'rejected');
// CREATE TABLE spqr_metadata.spqr_tx_status (
//     id TEXT PRIMARY KEY,
//     members TEXT[],
//     status tx_status,
//     created_at TIMESTAMPTZ,
//     updated_at TIMESTAMPTZ
// );

type PgQDB struct {
	mu sync.Mutex

	Shards  []*Shard
	TxLocks map[string]struct{}
	pooler  map[string]*pgxpool.Pool
}

func (q *PgQDB) getShardMasterConn(ctx context.Context, shard *Shard) (*pgxpool.Conn, error) {
	for _, dsn := range shard.RawHosts {
		conn, err := q.getHostConn(ctx, dsn)
		if err != nil {
			continue
		}
		var isMaster bool
		row := conn.QueryRow(ctx, "SELECT NOT pg_is_in_recovery() as is_master;")
		if err = row.Scan(&isMaster); err != nil {
			return nil, err
		}
		if isMaster {
			return conn, nil
		}
	}
	return nil, fmt.Errorf("unable to find master")
}

func (q *PgQDB) getHostConn(ctx context.Context, dsn string) (*pgxpool.Conn, error) {
	var pool *pgxpool.Pool
	ok := false
	if pool, ok = q.pooler[dsn]; !ok {
		var err error
		pool, err = pgxpool.New(context.TODO(), dsn)
		if err != nil {
			return nil, err
		}
	}
	return pool.Acquire(ctx)
}

// AcquireTxOwnership implements [DCStateKeeper].
func (q *PgQDB) AcquireTxOwnership(txid string) bool {
	q.mu.Lock()
	defer q.mu.Unlock()

	if _, ok := q.TxLocks[txid]; ok {
		return false
	}
	q.TxLocks[txid] = struct{}{}
	return true
}

// ChangeTxStatus implements [DCStateKeeper].
func (q *PgQDB) ChangeTxStatus(txid string, state TwoPhaseTxState) error {
	ctx := context.TODO()
	conn, err := q.getShardMasterConn(ctx, q.Shards[0])
	if err != nil {
		return err
	}
	// TODO: convert status
	_, err = conn.Exec(ctx, "UPDATE spqr_metadata.spqr_tx_status SET status=$1 WHERE id = $2", state, txid)
	if err != nil {
		return err
	}
	return nil
}

// RecordTwoPhaseMembers implements [DCStateKeeper].
func (q *PgQDB) RecordTwoPhaseMembers(txid string, shards []string) error {
	ctx := context.TODO()
	conn, err := q.getShardMasterConn(ctx, q.Shards[0])
	if err != nil {
		return err
	}
	_, err = conn.Exec(ctx, "UPDATE spqr_metadata.spqr_tx_status SET members=$1 WHERE id = $2", shards, txid)
	if err != nil {
		return err
	}
	return nil
}

// ReleaseTxOwnership implements [DCStateKeeper].
func (q *PgQDB) ReleaseTxOwnership(txid string) {
	q.mu.Lock()
	defer q.mu.Unlock()

	delete(q.TxLocks, txid)
}

// TXCohortShards implements [DCStateKeeper].
func (q *PgQDB) TXCohortShards(txid string) ([]string, error) {
	ctx := context.TODO()
	conn, err := q.getShardMasterConn(ctx, q.Shards[0])
	if err != nil {
		return nil, err
	}
	row := conn.QueryRow(ctx, "SELECT members FROM spqr_metadata.spqr_tx_status id = $1", txid)
	var members []string
	if err := row.Scan(&members); err != nil {
		return nil, err
	}
	return members, nil
}

// TXStatus implements [DCStateKeeper].
func (q *PgQDB) TXStatus(txid string) (TwoPhaseTxState, error) {
	ctx := context.TODO()
	conn, err := q.getShardMasterConn(ctx, q.Shards[0])
	if err != nil {
		return "", err
	}
	row := conn.QueryRow(ctx, "SELECT status FROM spqr_metadata.spqr_tx_status WHERE id = $1", txid)
	status := ""
	if err = row.Scan(&status); err != nil {
		return "", err
	}
	// TODO: convert with value check
	return TwoPhaseTxState(status), nil
}

func NewPgQDB(shards []*Shard) *PgQDB {
	return &PgQDB{
		Shards: shards,
		pooler: make(map[string]*pgxpool.Pool),
	}
}

var _ DCStateKeeper = &PgQDB{}
