package datatransfers

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/pg-sharding/spqr/qdb"
	"github.com/stretchr/testify/assert"
)

func TestQdbSavesTransactions(t *testing.T) {
	assert := assert.New(t)

	mConn := &mockConn{}
	err := beginTransactions(context.TODO(), mConn, mConn)
	assert.NoError(err)

	db, err := qdb.NewQDB("mem")
	assert.NoError(err)
	err = commitTransactions(context.TODO(), "sh1", "sh2", "krid", &db)
	assert.NoError(err)

	tx, err := db.GetTransferTx(context.TODO(), "krid")
	assert.NoError(err)
	assert.Equal(tx.FromShardId, "sh1")
	assert.Equal(tx.ToShardId, "sh2")
	assert.Equal(tx.ToTxName, "sh2-krid")
	assert.Equal(tx.FromTxName, "sh1-krid")
}

func TestQdbDeletesTransactions(t *testing.T) {
	assert := assert.New(t)

	mConn := &mockConn{}
	err := beginTransactions(context.TODO(), mConn, mConn)
	assert.NoError(err)

	db, err := qdb.NewQDB("mem")
	assert.NoError(err)
	err = commitTransactions(context.TODO(), "sh1", "sh2", "krid", &db)
	assert.NoError(err)

	_, err = db.GetTransferTx(context.TODO(), "krid")
	assert.NoError(err)

	err = db.RemoveTransferTx(context.TODO(), "krid")
	assert.NoError(err)

	_, err = db.GetTransferTx(context.TODO(), "krid")
	assert.Error(err)
}

type mockConn struct {
}

func (m *mockConn) Begin(context.Context) (pgx.Tx, error) {
	return &dbTx{}, nil
}
func (m *mockConn) BeginTx(context.Context, pgx.TxOptions) (pgx.Tx, error) {
	return &dbTx{}, nil
}
func (m *mockConn) Close(context.Context) error {
	return nil
}

type dbTx struct {
}

func (tx *dbTx) Begin(ctx context.Context) (pgx.Tx, error) {
	return nil, nil
}

func (tx *dbTx) Commit(ctx context.Context) error {
	return nil
}

func (tx *dbTx) Rollback(ctx context.Context) error {
	return nil
}

func (tx *dbTx) Exec(ctx context.Context, sql string, arguments ...any) (commandTag pgconn.CommandTag, err error) {
	return pgconn.CommandTag{}, nil
}

func (tx *dbTx) Prepare(ctx context.Context, name, sql string) (*pgconn.StatementDescription, error) {
	return nil, nil
}

func (tx *dbTx) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	return nil, nil
}

func (tx *dbTx) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	return nil
}

func (tx *dbTx) CopyFrom(ctx context.Context, tableName pgx.Identifier, columnNames []string, rowSrc pgx.CopyFromSource) (int64, error) {
	return 0, nil
}

func (tx *dbTx) SendBatch(ctx context.Context, b *pgx.Batch) pgx.BatchResults {
	return nil
}

func (tx *dbTx) LargeObjects() pgx.LargeObjects {
	return pgx.LargeObjects{}
}

func (tx *dbTx) Conn() *pgx.Conn {
	return nil
}
