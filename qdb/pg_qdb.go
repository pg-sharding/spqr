package qdb

import (
	"context"
	"fmt"
	"slices"
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

type ShardConn struct {
	Id  string               `json:"id"`
	Cfg *config.ShardConnect `json:"cfg"`
}

type PgQDB struct {
	mu sync.Mutex

	Shards []*ShardConn       `json:"shards"`
	Txs    map[string]*pgx.Tx `json:"-"`
	pooler map[string]*pgxpool.Pool
}

func (q *PgQDB) getShardMasterConn(ctx context.Context, shard *ShardConn) (*pgxpool.Conn, error) {
	errs := make([]string, 0)
	for _, dsn := range shard.Cfg.GetConnStrings() {
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

func (q *PgQDB) getTx(ctx context.Context, txid string) (*pgx.Tx, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if tx, ok := q.Txs[txid]; ok {
		return tx, nil
	}
	if len(q.Shards) == 0 {
		return nil, fmt.Errorf("could not lock transaction on shard: no shards found")
	}

	conn, err := q.getShardMasterConn(context.Background(), q.Shards[0])
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
	q.Txs[txid] = &tx
	return &tx, nil
}

// AcquireTxOwnership implements [DCStateKeeper].
func (q *PgQDB) AcquireTxOwnership(txid string) (bool, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if _, ok := q.Txs[txid]; ok {
		return false, nil
	}

	ctx := context.Background()

	_, err := q.getTx(ctx, txid)
	if err != nil {
		return false, err
	}
	return true, nil
}

// ChangeTxStatus implements [DCStateKeeper].
func (q *PgQDB) ChangeTxStatus(txid string, state TwoPhaseTxState) error {
	ctx := context.TODO()
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
func (q *PgQDB) RecordTwoPhaseMembers(txid string, shards []string) error {
	ctx := context.TODO()
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
func (q *PgQDB) ReleaseTxOwnership(txid string) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	if tx, ok := q.Txs[txid]; ok {
		if err := (*tx).Commit(context.TODO()); err != nil {
			return err
		}
		delete(q.Txs, txid)
	}
	return nil
}

// TXCohortShards implements [DCStateKeeper].
func (q *PgQDB) TXCohortShards(txid string) ([]string, error) {
	ctx := context.TODO()
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
func (q *PgQDB) TXStatus(txid string) (TwoPhaseTxState, error) {
	ctx := context.TODO()
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

func (q *PgQDB) ListTXNames() ([]string, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	res := make([]string, 0, len(q.Txs))
	for id := range q.Txs {
		res = append(res, id)
	}

	return res, nil
}

func (q *PgQDB) GetTxMetaStorage() []string {
	res := make([]string, len(q.Shards))
	for i, sh := range q.Shards {
		res[i] = sh.Id
	}
	return res
}

func NewPgQDB(shardsMap *config.DatatransferConnections) *PgQDB {
	shardsArr := make([]*ShardConn, 0, len(shardsMap.ShardsData))

	for id, shardConn := range shardsMap.ShardsData {
		shardsArr = append(shardsArr, &ShardConn{
			Id:  id,
			Cfg: shardConn,
		})
	}

	slices.SortFunc(shardsArr, func(l, r *ShardConn) int { return strings.Compare(l.Id, r.Id) })

	return &PgQDB{
		Shards: shardsArr,
		pooler: make(map[string]*pgxpool.Pool),
		Txs:    map[string]*pgx.Tx{},
	}
}

// TODO: do we need this?
func NewPgQDBFromShardArray(shardsArr []*ShardConn) *PgQDB {
	return &PgQDB{
		Shards: shardsArr,
		pooler: make(map[string]*pgxpool.Pool),
		Txs:    map[string]*pgx.Tx{},
	}
}

var _ DCStateKeeper = &PgQDB{}
