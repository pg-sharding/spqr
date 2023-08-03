package datatransfers

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/pg-sharding/spqr/pkg/mock"
	"github.com/pg-sharding/spqr/qdb"
	"github.com/stretchr/testify/assert"
)

// commitTransactions tests
func TestCommitPositive(t *testing.T) {
	assert := assert.New(t)
	ctrl := gomock.NewController(t)

	m1 := mock.NewMockTx(ctrl)
	m1.EXPECT().Exec(context.TODO(), "PREPARE TRANSACTION 'sh2-krid'").Return(pgconn.CommandTag{}, nil)
	m1.EXPECT().Exec(context.TODO(), "COMMIT PREPARED 'sh2-krid'").Return(pgconn.CommandTag{}, nil)

	m2 := mock.NewMockTx(ctrl)
	m2.EXPECT().Exec(context.TODO(), "PREPARE TRANSACTION 'sh1-krid'").Return(pgconn.CommandTag{}, nil)
	m2.EXPECT().Exec(context.TODO(), "COMMIT PREPARED 'sh1-krid'").Return(pgconn.CommandTag{}, nil)

	db, err := qdb.NewQDB("mem")
	assert.NoError(err)
	err = commitTransactions(context.TODO(), "sh1", "sh2", "krid", m1, m2, &db)
	assert.NoError(err)

	tx, err := db.GetTransferTx(context.TODO(), "krid")
	assert.NoError(err)
	assert.Equal(tx.FromStatus, qdb.Commited)
	assert.Equal(tx.ToStatus, qdb.Commited)
	assert.Equal(tx.ToTxName, "sh2-krid")
	assert.Equal(tx.FromTxName, "sh1-krid")
}

func TestFailToCommitFirstTx(t *testing.T) {
	assert := assert.New(t)
	ctrl := gomock.NewController(t)

	m1 := mock.NewMockTx(ctrl)
	m1.EXPECT().Exec(context.TODO(), "PREPARE TRANSACTION 'sh2-krid'").Return(pgconn.CommandTag{}, nil)
	m1.EXPECT().Exec(context.TODO(), "COMMIT PREPARED 'sh2-krid'").Return(pgconn.CommandTag{}, fmt.Errorf(""))

	m2 := mock.NewMockTx(ctrl)
	m2.EXPECT().Exec(context.TODO(), "PREPARE TRANSACTION 'sh1-krid'").Return(pgconn.CommandTag{}, nil)

	db, err := qdb.NewQDB("mem")
	assert.NoError(err)
	err = commitTransactions(context.TODO(), "sh1", "sh2", "krid", m1, m2, &db)
	assert.Error(err)

	tx, err := db.GetTransferTx(context.TODO(), "krid")
	assert.NoError(err)
	assert.Equal(tx.FromStatus, qdb.Processing)
	assert.Equal(tx.ToStatus, qdb.Processing)
}

func TestFailToCommitSecondTx(t *testing.T) {
	assert := assert.New(t)
	ctrl := gomock.NewController(t)

	m1 := mock.NewMockTx(ctrl)
	m1.EXPECT().Exec(context.TODO(), "PREPARE TRANSACTION 'sh2-krid'").Return(pgconn.CommandTag{}, nil)
	m1.EXPECT().Exec(context.TODO(), "COMMIT PREPARED 'sh2-krid'").Return(pgconn.CommandTag{}, nil)

	m2 := mock.NewMockTx(ctrl)
	m2.EXPECT().Exec(context.TODO(), "PREPARE TRANSACTION 'sh1-krid'").Return(pgconn.CommandTag{}, nil)
	m2.EXPECT().Exec(context.TODO(), "COMMIT PREPARED 'sh1-krid'").Return(pgconn.CommandTag{}, fmt.Errorf(""))

	db, err := qdb.NewQDB("mem")
	assert.NoError(err)
	err = commitTransactions(context.TODO(), "sh1", "sh2", "krid", m1, m2, &db)
	assert.Error(err)

	tx, err := db.GetTransferTx(context.TODO(), "krid")
	assert.NoError(err)
	assert.Equal(tx.FromStatus, qdb.Processing)
	assert.Equal(tx.ToStatus, qdb.Commited)
}

func TestFailToPrepareFirstTx(t *testing.T) {
	assert := assert.New(t)
	ctrl := gomock.NewController(t)

	m1 := mock.NewMockTx(ctrl)
	m1.EXPECT().Exec(context.TODO(), "PREPARE TRANSACTION 'sh2-krid'").Return(pgconn.CommandTag{}, fmt.Errorf(""))

	m2 := mock.NewMockTx(ctrl)

	db, err := qdb.NewQDB("mem")
	assert.NoError(err)
	err = commitTransactions(context.TODO(), "sh1", "sh2", "krid", m1, m2, &db)
	assert.Error(err)

	_, err = db.GetTransferTx(context.TODO(), "krid")
	assert.Error(err)
}

func TestFailToPrepareSecondTx(t *testing.T) {
	assert := assert.New(t)
	ctrl := gomock.NewController(t)

	m1 := mock.NewMockTx(ctrl)
	m1.EXPECT().Exec(context.TODO(), "PREPARE TRANSACTION 'sh2-krid'").Return(pgconn.CommandTag{}, nil)
	m1.EXPECT().Exec(context.TODO(), "ROLLBACK PREPARED 'sh2-krid'").Return(pgconn.CommandTag{}, nil)

	m2 := mock.NewMockTx(ctrl)
	m2.EXPECT().Exec(context.TODO(), "PREPARE TRANSACTION 'sh1-krid'").Return(pgconn.CommandTag{}, fmt.Errorf(""))

	db, err := qdb.NewQDB("mem")
	assert.NoError(err)
	err = commitTransactions(context.TODO(), "sh1", "sh2", "krid", m1, m2, &db)
	assert.Error(err)

	_, err = db.GetTransferTx(context.TODO(), "krid")
	assert.Error(err)
}

// rollbackTransactions tests
func TestRollbackPositive(t *testing.T) {
	assert := assert.New(t)
	ctrl := gomock.NewController(t)

	m1 := mock.NewMockTx(ctrl)
	m1.EXPECT().Rollback(context.TODO()).Return(nil)

	m2 := mock.NewMockTx(ctrl)
	m2.EXPECT().Rollback(context.TODO()).Return(nil)

	err := rollbackTransactions(context.TODO(), m1, m2)
	assert.NoError(err)
}

func TestRollbackFirstFailAndSecondRollbacks(t *testing.T) {
	assert := assert.New(t)
	ctrl := gomock.NewController(t)

	m1 := mock.NewMockTx(ctrl)
	m1.EXPECT().Rollback(context.TODO()).Return(fmt.Errorf(""))

	m2 := mock.NewMockTx(ctrl)
	m2.EXPECT().Rollback(context.TODO()).Return(nil)

	err := rollbackTransactions(context.TODO(), m1, m2)
	assert.Error(err)
}

//beginTransactions tests

func TestBeginTxPositive(t *testing.T) {
	assert := assert.New(t)
	ctrl := gomock.NewController(t)

	m1 := mock.NewMockpgxConnIface(ctrl)
	m1.EXPECT().BeginTx(context.TODO(), pgx.TxOptions{}).Return(nil, nil)

	m2 := mock.NewMockpgxConnIface(ctrl)
	m2.EXPECT().BeginTx(context.TODO(), pgx.TxOptions{}).Return(nil, nil)

	_, _, err := beginTransactions(context.TODO(), m1, m2)
	assert.NoError(err)
}

// createConnString and LoadConfig tests
func TestConnectCreds(t *testing.T) {
	assert := assert.New(t)
	var wg sync.WaitGroup
	for k := 0; k < 100; k++ {
		wg.Add(1)
		go func() {
			for i := 0; i < 100; i++ {
				LoadConfig("")
				createConnString("sh1")
			}
			wg.Done()
		}()
	}
	wg.Wait()
	assert.True(true)
}
