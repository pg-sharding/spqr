package etcdqdb_integration_test

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/pg-sharding/spqr/pkg/models/spqrerror"
	"github.com/pg-sharding/spqr/qdb"
	"github.com/stretchr/testify/assert"
	clientv3 "go.etcd.io/etcd/client/v3"
)

const (
	EtcdPort        = 2379
	TestTimeout     = 10 * time.Second
	ComposerTimeout = 60
)

func runCompose(args []string) error {
	args2 := []string{}
	args2 = append(args2, "compose", "-f", "docker-compose.yaml", "-p", "etcdqdb_test")
	args2 = append(args2, args...)
	cmd := exec.Command("docker", args2...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("failed to run 'docker %s': %s\n%s", strings.Join(args2, " "), err, out)
	}
	return nil
}

func Down() error {
	return runCompose([]string{"down", "-v"})
}

func Up() error {
	return runCompose([]string{"up", "-d", "--force-recreate", "-t", strconv.Itoa(ComposerTimeout)})
}

func setupTestSet(t *testing.T) error {
	err := os.Setenv("DOCKER_API_VERSION", "1.48")
	if err != nil {
		return err
	}
	t.Log("load etcd")
	err = Up()
	if err != nil {
		return err
	}
	return nil
}

func cleanupDb(ctx context.Context, db *qdb.EtcdQDB) error {
	_, err := db.Client().Delete(ctx, "", clientv3.WithPrefix())
	return err
}

func setupSubTest(ctx context.Context) (*qdb.EtcdQDB, error) {
	db, err := qdb.NewEtcdQDB(fmt.Sprintf("http://localhost:%d", EtcdPort), 0)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to SPQR QDB: %s", err)
	}
	if err := cleanupDb(ctx, db); err != nil {
		return nil, err
	}
	return db, nil
}

func TestAddShard(t *testing.T) {
	is := assert.New(t)
	err := setupTestSet(t)
	is.NoError(err)
	defer func() {
		_ = Down()
	}()
	is.NoError(err)

	t.Run("test AddShard", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.TODO(), TestTimeout)
		defer cancel()
		db, err := setupSubTest(ctx)
		is.NoError(err)

		t.Run("happy path", func(t *testing.T) {
			err := cleanupDb(ctx, db)
			is.NoError(err)
			sh := qdb.NewShard("sh1", []string{"denchick.rs", "reshke.ru"})
			err = db.AddShard(ctx, sh)
			is.NoError(err)
			actual, err := db.GetShard(ctx, "sh1")
			is.NoError(err)
			expected := qdb.NewShard("sh1", []string{"denchick.rs", "reshke.ru"})
			is.Equal(actual, expected)
		})

		t.Run("fail", func(t *testing.T) {
			err := cleanupDb(ctx, db)
			is.NoError(err)
			err = db.AddShard(ctx, qdb.NewShard("sh1", []string{"denchick.rs", "reshke.ru"}))
			is.NoError(err)
			err = db.AddShard(ctx, qdb.NewShard("sh1", []string{"bug.rs", "bug.ru"}))
			is.Error(err)
			actual, err := db.GetShard(ctx, "sh1")
			is.NoError(err)
			expected := qdb.NewShard("sh1", []string{"denchick.rs", "reshke.ru"})
			is.Equal(actual, expected)
		})
	})

}

func TestLockUnlock(t *testing.T) {
	is := assert.New(t)
	err := setupTestSet(t)
	is.NoError(err)
	defer func() {
		_ = Down()
	}()
	is.NoError(err)

	t.Run("test UnLock", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.TODO(), TestTimeout)
		defer cancel()
		db, err := setupSubTest(ctx)
		is.NoError(err)
		t.Run("happy path", func(t *testing.T) {
			err := cleanupDb(ctx, db)
			is.NoError(err)
			keyRange1 := qdb.KeyRange{
				LowerBound:     [][]byte{[]byte("1111")},
				ShardID:        "sh1",
				KeyRangeID:     "krid1",
				DistributionId: "ds1",
			}
			statements, err := db.CreateKeyRange(ctx, &keyRange1)
			is.NoError(err)
			is.NoError(db.ExecNoTransaction(ctx, statements))
			_, err = db.NoWaitLockKeyRange(ctx, keyRange1.KeyRangeID)
			is.NoError(err)
			err = db.UnlockKeyRange(ctx, keyRange1.KeyRangeID)
			is.NoError(err)
			_, err = db.CheckLockedKeyRange(ctx, keyRange1.KeyRangeID)
			expectedErr := spqrerror.Newf(spqrerror.SPQR_KEYRANGE_ERROR, "key range %v not locked", keyRange1.KeyRangeID)
			is.Equal(expectedErr, err)
		})
	})

}

func TestTransactions(t *testing.T) {
	is := assert.New(t)
	err := setupTestSet(t)
	is.NoError(err)
	defer func() {
		_ = Down()
	}()
	is.NoError(err)
	t.Run("test Begin tran", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.TODO(), TestTimeout)
		defer cancel()
		db, err := setupSubTest(ctx)
		is.NoError(err)
		t.Run("simple begin tran success", func(t *testing.T) {
			tran, err := qdb.NewTransaction()
			is.NoError(err)
			err = db.BeginTransaction(ctx, tran)
			is.NoError(err)
			result, err := db.Client().Get(ctx, "transaction_request")
			is.NoError(err)
			is.Equal(tran.Id().String(), string(result.Kvs[0].Value))
		})
		t.Run("2 begin tran success", func(t *testing.T) {
			tran1, err := qdb.NewTransaction()
			is.NoError(err)
			err = db.BeginTransaction(ctx, tran1)
			is.NoError(err)
			tran2, err := qdb.NewTransaction()
			is.NoError(err)
			err = db.BeginTransaction(ctx, tran2)
			is.NoError(err)
			result, err := db.Client().Get(ctx, "transaction_request")
			is.NoError(err)
			is.Equal(tran2.Id().String(), string(result.Kvs[0].Value))
		})
	})
	t.Run("test exec no tran", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.TODO(), TestTimeout)
		defer cancel()
		db, err := setupSubTest(ctx)
		is.NoError(err)
		t.Run("happy path", func(t *testing.T) {
			statements := []qdb.QdbStatement{
				{CmdType: qdb.CMD_PUT, Key: "test1", Value: "val1"},
				{CmdType: qdb.CMD_PUT, Key: "test2", Value: "val2"},
				{CmdType: qdb.CMD_DELETE, Key: "test3"},
			}
			err := db.ExecNoTransaction(ctx, statements)
			is.NoError(err)
			//check execution
			result, err := db.Client().Get(ctx, "test1")
			is.NoError(err)
			is.Equal("val1", string(result.Kvs[0].Value))
			result, err = db.Client().Get(ctx, "test2")
			is.NoError(err)
			is.Equal("val2", string(result.Kvs[0].Value))
			result, err = db.Client().Get(ctx, "test3")
			is.NoError(err)
			is.Equal(0, len(result.Kvs))
		})
		t.Run("2 sequential runs", func(t *testing.T) {
			//run1
			statements := []qdb.QdbStatement{
				{CmdType: qdb.CMD_PUT, Key: "test3", Value: "val3"},
			}
			err := db.ExecNoTransaction(ctx, statements)
			is.NoError(err)
			//check execution 1
			result, err := db.Client().Get(ctx, "test3")
			is.NoError(err)
			is.Equal("val3", string(result.Kvs[0].Value))
			//run2
			statements = []qdb.QdbStatement{
				{CmdType: qdb.CMD_PUT, Key: "test1", Value: "val1"},
				{CmdType: qdb.CMD_DELETE, Key: "test3"},
			}
			err = db.ExecNoTransaction(ctx, statements)
			is.NoError(err)
			//check execution 2
			result, err = db.Client().Get(ctx, "test1")
			is.NoError(err)
			is.Equal("val1", string(result.Kvs[0].Value))
			result, err = db.Client().Get(ctx, "test3")
			is.NoError(err)
			is.Equal(0, len(result.Kvs))
		})
	})
	t.Run("test commit tran", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.TODO(), TestTimeout)
		defer cancel()
		db, err := setupSubTest(ctx)
		is.NoError(err)
		t.Run("happy path commit tran", func(t *testing.T) {
			tran, err := qdb.NewTransaction()
			is.NoError(err)
			err = db.BeginTransaction(ctx, tran)
			is.NoError(err)
			statements := []qdb.QdbStatement{
				{CmdType: qdb.CMD_PUT, Key: "test1", Value: "val1"},
				{CmdType: qdb.CMD_PUT, Key: "test2", Value: "val2"},
				{CmdType: qdb.CMD_DELETE, Key: "test3"},
			}
			err = tran.Append(statements)
			is.NoError(err)
			err = db.CommitTransaction(ctx, tran)
			is.NoError(err)
			//check execution
			result, err := db.Client().Get(ctx, "test1")
			is.NoError(err)
			is.Equal("val1", string(result.Kvs[0].Value))
			result, err = db.Client().Get(ctx, "test2")
			is.NoError(err)
			is.Equal("val2", string(result.Kvs[0].Value))
			result, err = db.Client().Get(ctx, "test3")
			is.NoError(err)
			is.Equal(0, len(result.Kvs))
		})
		t.Run("fail commit tran after begin another tran", func(t *testing.T) {
			tran1, err := qdb.NewTransaction()
			is.NoError(err)
			err = db.BeginTransaction(ctx, tran1)
			is.NoError(err)
			statements := []qdb.QdbStatement{
				{CmdType: qdb.CMD_PUT, Key: "test1", Value: "val1"},
			}
			err = tran1.Append(statements)
			is.NoError(err)
			tran2, err := qdb.NewTransaction()
			is.NoError(err)
			err = db.BeginTransaction(ctx, tran2)
			is.NoError(err)

			err = db.CommitTransaction(ctx, tran1)
			is.EqualError(err, fmt.Sprintf("transaction '%s' can't be committed", tran1.Id()))
		})
		t.Run("suddenly there was a boxwood", func(t *testing.T) {
			tran1, err := qdb.NewTransaction()
			is.NoError(err)
			err = db.BeginTransaction(ctx, tran1)
			is.NoError(err)
			statements := []qdb.QdbStatement{
				{CmdType: qdb.CMD_PUT, Key: "test1", Value: "val1"},
			}
			err = tran1.Append(statements)
			is.NoError(err)
			_, err = db.Client().Delete(ctx, "transaction_request")
			is.NoError(err)

			err = db.CommitTransaction(ctx, tran1)
			is.EqualError(err, fmt.Sprintf("transaction '%s' can't be committed", tran1.Id()))
		})

		t.Run("fails invalid tran", func(t *testing.T) {
			tran1, err := qdb.NewTransaction()
			is.NoError(err)
			err = db.BeginTransaction(ctx, tran1)
			is.NoError(err)
			statements := []qdb.QdbStatement{}
			_ = tran1.Append(statements) //handling this error was skipped intentionally
			err = db.CommitTransaction(ctx, tran1)
			is.EqualError(err, fmt.Sprintf("invalid transaction %s: transaction %s haven't statements", tran1.Id(), tran1.Id()))
		})

	})
}

func TestGetMoveTaskGroup(t *testing.T) {
	assert := assert.New(t)
	err := setupTestSet(t)
	assert.NoError(err)
	defer func() {
		_ = Down()
	}()
	assert.NoError(err)
	t.Run("test Begin tran", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.TODO(), TestTimeout)
		defer cancel()
		db, err := setupSubTest(ctx)
		assert.NoError(err)
		t.Run("empty request returns no task groups", func(t *testing.T) {
			assert.NoError(db.WriteMoveTaskGroup(
				ctx,
				"tg1",
				&qdb.MoveTaskGroup{},
				0,
				nil,
			))
			taskGroup, err := db.GetMoveTaskGroup(ctx, "")
			assert.NoError(err)
			assert.Nil(taskGroup)
		})
		t.Run("base case", func(t *testing.T) {
			tg := &qdb.MoveTaskGroup{ShardToId: "shard_to", KrIdFrom: "kr_from", KrIdTo: "kr_to"}
			assert.NoError(db.WriteMoveTaskGroup(
				ctx,
				"some_task_group",
				tg,
				0,
				nil,
			))
			taskGroup, err := db.GetMoveTaskGroup(ctx, "some_task_group")
			assert.NoError(err)
			assert.Equal(tg.KrIdFrom, taskGroup.KrIdFrom)
			assert.Equal(tg.KrIdTo, taskGroup.KrIdTo)
			assert.Equal(tg.ShardToId, taskGroup.ShardToId)
		})
	})
}
