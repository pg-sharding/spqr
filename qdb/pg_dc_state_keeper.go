package qdb

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/pg-sharding/spqr/pkg/config"
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

const (
	pgStatePlanned    = "planned"
	pgStateCommitting = "committing"
	pgStateCommitted  = "committed"
	pgStateRejected   = "rejected"
)

type PgDCStateKeeper struct {
	mu sync.RWMutex

	shards  *config.DatatransferConnections
	txs     map[string]*pgx.Tx `json:"-"`
	storage []string
	pooler  map[string]*pgxpool.Pool
}

func (q *PgDCStateKeeper) getShardMasterConn(ctx context.Context, shard *config.ShardConnect) (*pgxpool.Conn, error) {
	errs := make([]string, 0)
	for _, dsn := range shard.GetConnStrings() {
		conn, err := q.getHostConn(ctx, dsn)
		if err != nil {
			errs = append(errs, fmt.Sprintf("\"%s\": %s", dsn, err))
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
	return nil, fmt.Errorf("unable to find master: %s", strings.Join(errs, ", "))
}

func (q *PgDCStateKeeper) getHostConn(ctx context.Context, dsn string) (*pgxpool.Conn, error) {
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

func (q *PgDCStateKeeper) getStorageShardConnect() (*config.ShardConnect, error) {
	if len(q.storage) == 0 {
		return nil, fmt.Errorf("could not lock transaction on shard: no shards found")
	}
	if cfg, ok := q.shards.ShardsData[q.storage[0]]; ok {
		return cfg, nil
	} else {
		return nil, fmt.Errorf("shard \"%s\" not found in config", q.storage[0])
	}
}

func (q *PgDCStateKeeper) getTx(ctx context.Context, txid string) (*pgx.Tx, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if tx, ok := q.txs[txid]; ok {
		return tx, nil
	}

	shardCfg, err := q.getStorageShardConnect()
	if err != nil {
		return nil, err
	}
	conn, err := q.getShardMasterConn(context.Background(), shardCfg)
	if err != nil {
		return nil, err
	}
	tx, err := conn.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return nil, err
	}
	_, err = tx.Exec(ctx, "SELECT id FROM spqr_metadata.spqr_tx_status WHERE id = $1 FOR UPDATE", txid)
	if err != nil {
		return nil, err
	}
	q.txs[txid] = &tx
	return &tx, nil
}

// AcquireTxOwnership implements [DCStateKeeper].
func (q *PgDCStateKeeper) AcquireTxOwnership(ctx context.Context, txid string) (bool, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if _, ok := q.txs[txid]; ok {
		return false, nil
	}

	_, err := q.getTx(ctx, txid)
	if err != nil {
		return false, err
	}
	return true, nil
}

// ChangeTxStatus implements [DCStateKeeper].
func (q *PgDCStateKeeper) ChangeTxStatus(ctx context.Context, txid string, state TwoPhaseTxState) error {
	tx, err := q.getTx(ctx, txid)
	if err != nil {
		return err
	}
	pgState := ""
	switch state {
	case TwoPhaseInitState:
		pgState = pgStatePlanned
	case TwoPhaseP1:
		pgState = pgStateCommitting
	case TwoPhaseP2:
		pgState = pgStateCommitted
	case TwoPhaseP2Rejected:
		pgState = pgStateRejected
	default:
		return fmt.Errorf("unknown tx state value \"%s\"", state)
	}
	_, err = (*tx).Exec(ctx, "UPDATE spqr_metadata.spqr_tx_status SET status=$1 WHERE id = $2", pgState, txid)
	if err != nil {
		return err
	}
	return nil
}

// RecordTwoPhaseMembers implements [DCStateKeeper].
func (q *PgDCStateKeeper) RecordTwoPhaseMembers(ctx context.Context, txid string, shards []string) error {
	tx, err := q.getTx(ctx, txid)
	if err != nil {
		return err
	}
	_, err = (*tx).Exec(ctx, "UPDATE spqr_metadata.spqr_tx_status SET members=$1 WHERE id = $2", shards, txid)
	if err != nil {
		return err
	}
	return nil
}

// ReleaseTxOwnership implements [DCStateKeeper].
func (q *PgDCStateKeeper) ReleaseTxOwnership(_ context.Context, txid string) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	if tx, ok := q.txs[txid]; ok {
		if err := (*tx).Commit(context.TODO()); err != nil {
			return err
		}
		delete(q.txs, txid)
	}
	return nil
}

// TXCohortShards implements [DCStateKeeper].
func (q *PgDCStateKeeper) TXCohortShards(ctx context.Context, txid string) ([]string, error) {
	tx, err := q.getTx(ctx, txid)
	if err != nil {
		return nil, err
	}
	row := (*tx).QueryRow(ctx, "SELECT members FROM spqr_metadata.spqr_tx_status id = $1", txid)
	var members []string
	if err := row.Scan(&members); err != nil {
		return nil, err
	}
	return members, nil
}

// TXStatus implements [DCStateKeeper].
func (q *PgDCStateKeeper) TXStatus(ctx context.Context, txid string) (TwoPhaseTxState, error) {
	tx, err := q.getTx(ctx, txid)
	if err != nil {
		return "", err
	}
	row := (*tx).QueryRow(ctx, "SELECT status FROM spqr_metadata.spqr_tx_status WHERE id = $1", txid)
	status := ""
	if err = row.Scan(&status); err != nil {
		return "", err
	}
	switch status {
	case pgStatePlanned:
		return TwoPhaseInitState, nil
	case pgStateCommitting:
		return TwoPhaseP1, nil
	case pgStateCommitted:
		return TwoPhaseP2, nil
	case pgStateRejected:
		return TwoPhaseP2Rejected, nil
	default:
		return "", fmt.Errorf("unknown tx state in postgres: \"%s\"", status)
	}
}

func (q *PgDCStateKeeper) ListTXNames(_ context.Context) ([]string, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	res := make([]string, 0, len(q.txs))
	for id := range q.txs {
		res = append(res, id)
	}

	return res, nil
}

func (q *PgDCStateKeeper) GetTxMetaStorage() []string {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return q.storage
}

func (q *PgDCStateKeeper) SetTxMetaStorage(storage []string) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.storage != nil {
		return fmt.Errorf("could not set two-phase tx meta storage twice")
	}
	q.storage = storage
	return nil
}

func NewPgQDB(shards *config.DatatransferConnections) *PgDCStateKeeper {
	return &PgDCStateKeeper{
		mu:     sync.RWMutex{},
		shards: shards,
		pooler: make(map[string]*pgxpool.Pool),
		txs:    map[string]*pgx.Tx{},
	}
}

var _ DCStateKeeper = &PgDCStateKeeper{}
