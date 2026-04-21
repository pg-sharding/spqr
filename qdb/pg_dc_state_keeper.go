package qdb

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/spqrlog"
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
	storage []string
	pooler  map[string]*pgxpool.Pool
	locks   map[string]any
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
		pool, err = pgxpool.New(context.Background(), dsn)
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
	conn, err := q.getConn(ctx)
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
	return &tx, nil
}

func (q *PgDCStateKeeper) getConn(ctx context.Context) (*pgxpool.Conn, error) {
	shardCfg, err := q.getStorageShardConnect()
	if err != nil {
		return nil, err
	}
	conn, err := q.getShardMasterConn(ctx, shardCfg)
	if err != nil {
		return nil, err
	}
	return conn, nil
}

// AcquireTxOwnership implements [DCStateKeeper].
func (q *PgDCStateKeeper) AcquireTxOwnership(_ context.Context, txid string) (bool, error) {
	spqrlog.Zero.Debug().Str("gid", txid).Msg("pg dc state keeper: acquire tx ownership")
	q.mu.Lock()
	defer q.mu.Unlock()

	if _, ok := q.locks[txid]; ok {
		return false, nil
	}

	q.locks[txid] = true
	return true, nil
}

// ChangeTxStatus implements [DCStateKeeper].
func (q *PgDCStateKeeper) ChangeTxStatus(ctx context.Context, txid string, state TwoPhaseTxState) error {
	spqrlog.Zero.Debug().Str("gid", txid).Str("state", string(state)).Msg("pg dc state keeper: change tx state")
	tx, err := q.getTx(ctx, txid)
	defer func() { _ = (*tx).Rollback(ctx) }()
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
	if err = (*tx).Commit(ctx); err != nil {
		return err
	}
	return nil
}

// RecordTwoPhaseMembers implements [DCStateKeeper].
func (q *PgDCStateKeeper) RecordTwoPhaseMembers(ctx context.Context, txid string, shards []string) error {
	spqrlog.Zero.Debug().Str("gid", txid).Strs("shards", shards).Msg("pg dc state keeper: record tx members")
	tx, err := q.getTx(ctx, txid)
	defer func() { _ = (*tx).Rollback(ctx) }()
	if err != nil {
		return err
	}
	row := (*tx).QueryRow(ctx, "SELECT count(*) FROM spqr_metadata.spqr_tx_status WHERE id = $1", txid)
	count := 0
	if err = row.Scan(&count); err != nil {
		return err
	}
	if count > 0 {
		_, err = (*tx).Exec(ctx, "UPDATE spqr_metadata.spqr_tx_status SET members=$1 WHERE id = $2", shards, txid)
	} else {
		_, err = (*tx).Exec(ctx, "INSERT INTO spqr_metadata.spqr_tx_status (id, members, status) VALUES ($1, $2, $3)", txid, shards, pgStatePlanned)
	}
	if err != nil {
		return err
	}
	if err = (*tx).Commit(ctx); err != nil {
		return err
	}
	return nil
}

// ReleaseTxOwnership implements [DCStateKeeper].
func (q *PgDCStateKeeper) ReleaseTxOwnership(_ context.Context, txid string) error {
	spqrlog.Zero.Debug().Str("gid", txid).Msg("pg dc state keeper: release tx ownership")
	q.mu.Lock()
	defer q.mu.Unlock()

	delete(q.locks, txid)
	return nil
}

// TXCohortShards implements [DCStateKeeper].
func (q *PgDCStateKeeper) TXCohortShards(ctx context.Context, txid string) ([]string, error) {
	spqrlog.Zero.Debug().Str("gid", txid).Msg("pg dc state keeper: get tx cohort shards")
	tx, err := q.getTx(ctx, txid)
	defer func() { _ = (*tx).Rollback(ctx) }()
	if err != nil {
		return nil, err
	}
	row := (*tx).QueryRow(ctx, "SELECT members FROM spqr_metadata.spqr_tx_status WHERE id = $1", txid)
	var members []string
	if err := row.Scan(&members); err != nil {
		return nil, err
	}
	return members, nil
}

// TXStatus implements [DCStateKeeper].
func (q *PgDCStateKeeper) TXStatus(ctx context.Context, txid string) (TwoPhaseTxState, error) {
	spqrlog.Zero.Debug().Str("gid", txid).Msg("pg dc state keeper: get tx status")
	tx, err := q.getTx(ctx, txid)
	defer func() { _ = (*tx).Rollback(ctx) }()
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

func (q *PgDCStateKeeper) ListTXNames(ctx context.Context) ([]string, error) {
	conn, err := q.getConn(ctx)
	if err != nil {
		return nil, err
	}
	rows, err := conn.Query(ctx, "SELECT id FROM spqr_metadata.spqr_tx_status")
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (string, error) {
		id := ""
		if err := row.Scan(&id); err != nil {
			return "", err
		}
		return id, nil
	})
}

func (q *PgDCStateKeeper) GetTXs(ctx context.Context) (map[string]*TwoPCInfo, error) {
	conn, err := q.getConn(ctx)
	if err != nil {
		return nil, err
	}

	tx, err := conn.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = tx.Rollback(ctx)
	}()
	_, err = tx.Exec(ctx, "LOCK TABLE spqr_metadata.spqr_tx_status IN ACCESS EXCLUSIVE MODE")
	if err != nil {
		return nil, err
	}

	rows, err := tx.Query(ctx, "SELECT (id, status, members, updated_at) FROM spqr_metadat.spqr_tx_status")
	if err != nil {
		return nil, err
	}
	txInfoSlice, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (*TwoPCInfo, error) {
		info := &TwoPCInfo{
			SHardsIds: []string{},
		}
		if err := row.Scan(&info.Gid, &info.State, &info.SHardsIds, &info.UpdatedAt); err != nil {
			return nil, err
		}
		return info, err
	})
	if err != nil {
		return nil, err
	}
	txInfo := map[string]*TwoPCInfo{}
	for _, tx := range txInfoSlice {
		txInfo[tx.Gid] = tx
	}
	return txInfo, tx.Commit(ctx)
}

func (q *PgDCStateKeeper) GetTxMetaStorage() []string {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return q.storage
}

func (q *PgDCStateKeeper) SetTxMetaStorage(storage []string) error {
	spqrlog.Zero.Debug().Strs("storage", storage).Msg("pg_dcs_keeper: set tx meta storage")
	q.mu.Lock()
	defer q.mu.Unlock()

	if len(q.storage) > 0 {
		return fmt.Errorf("could not set two-phase tx meta storage twice")
	}
	q.storage = storage
	return nil
}

func (q *PgDCStateKeeper) ClearTxStatuses(ctx context.Context) error {
	conn, err := q.getConn(ctx)
	if err != nil {
		return err
	}
	_, err = conn.Exec(ctx, "DELETE FROM spqr_metadata.spqr_tx_status")
	return err
}

func NewPgQDB(shards *config.DatatransferConnections) *PgDCStateKeeper {
	return &PgDCStateKeeper{
		mu:     sync.RWMutex{},
		shards: shards,
		pooler: make(map[string]*pgxpool.Pool),
		locks:  map[string]any{},
	}
}

var _ DCStateKeeper = &PgDCStateKeeper{}
