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
	"github.com/pg-sharding/spqr/router/rfqn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
	err := os.Setenv("DOCKER_API_VERSION", "1.47")
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
	db, err := qdb.NewEtcdQDB([]string{fmt.Sprintf("http://localhost:%d", EtcdPort)}, 0)
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

	t.Run("test AddShard", func(_ *testing.T) {
		ctx, cancel := context.WithTimeout(context.TODO(), TestTimeout)
		defer cancel()
		db, err := setupSubTest(ctx)
		is.NoError(err)

		t.Run("happy path", func(_ *testing.T) {
			err := cleanupDb(ctx, db)
			is.NoError(err)
			sh := qdb.NewShard("sh1", []string{"denchick.rs", "reshke.ru"}, nil)
			err = db.AddShard(ctx, sh)
			is.NoError(err)
			actual, err := db.GetShard(ctx, "sh1")
			is.NoError(err)
			expected := qdb.NewShard("sh1", []string{"denchick.rs", "reshke.ru"}, nil)
			is.Equal(actual, expected)
		})

		t.Run("fail", func(_ *testing.T) {
			err := cleanupDb(ctx, db)
			is.NoError(err)
			err = db.AddShard(ctx, qdb.NewShard("sh1", []string{"denchick.rs", "reshke.ru"}, nil))
			is.NoError(err)
			err = db.AddShard(ctx, qdb.NewShard("sh1", []string{"bug.rs", "bug.ru"}, nil))
			is.Error(err)
			actual, err := db.GetShard(ctx, "sh1")
			is.NoError(err)
			expected := qdb.NewShard("sh1", []string{"denchick.rs", "reshke.ru"}, nil)
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

	t.Run("test UnLock", func(_ *testing.T) {
		ctx, cancel := context.WithTimeout(context.TODO(), TestTimeout)
		defer cancel()
		db, err := setupSubTest(ctx)
		is.NoError(err)
		t.Run("happy path", func(_ *testing.T) {
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
			_, err = db.LockKeyRange(ctx, keyRange1.KeyRangeID)
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
	t.Run("test Begin tran", func(_ *testing.T) {
		ctx, cancel := context.WithTimeout(context.TODO(), TestTimeout)
		defer cancel()
		db, err := setupSubTest(ctx)
		is.NoError(err)
		t.Run("simple begin tran success", func(_ *testing.T) {
			tran, err := qdb.NewTransaction()
			is.NoError(err)
			err = db.BeginTransaction(ctx, tran)
			is.NoError(err)
			result, err := db.Client().Get(ctx, "transaction_request")
			is.NoError(err)
			is.Equal(tran.Id().String(), string(result.Kvs[0].Value))
		})
		t.Run("2 begin tran success", func(_ *testing.T) {
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
	t.Run("test exec no tran", func(_ *testing.T) {
		ctx, cancel := context.WithTimeout(context.TODO(), TestTimeout)
		defer cancel()
		db, err := setupSubTest(ctx)
		is.NoError(err)
		t.Run("happy path", func(_ *testing.T) {
			statements := []qdb.QdbStatement{
				{CmdType: qdb.CmdPut, Key: "test1", Value: "val1"},
				{CmdType: qdb.CmdPut, Key: "test2", Value: "val2"},
				{CmdType: qdb.CmdDelete, Key: "test3"},
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
		t.Run("2 sequential runs", func(_ *testing.T) {
			//run1
			statements := []qdb.QdbStatement{
				{CmdType: qdb.CmdPut, Key: "test3", Value: "val3"},
			}
			err := db.ExecNoTransaction(ctx, statements)
			is.NoError(err)
			//check execution 1
			result, err := db.Client().Get(ctx, "test3")
			is.NoError(err)
			is.Equal("val3", string(result.Kvs[0].Value))
			//run2
			statements = []qdb.QdbStatement{
				{CmdType: qdb.CmdPut, Key: "test1", Value: "val1"},
				{CmdType: qdb.CmdDelete, Key: "test3"},
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
	t.Run("test commit tran", func(_ *testing.T) {
		ctx, cancel := context.WithTimeout(context.TODO(), TestTimeout)
		defer cancel()
		db, err := setupSubTest(ctx)
		is.NoError(err)
		t.Run("happy path commit tran", func(_ *testing.T) {
			tran, err := qdb.NewTransaction()
			is.NoError(err)
			err = db.BeginTransaction(ctx, tran)
			is.NoError(err)
			statements := []qdb.QdbStatement{
				{CmdType: qdb.CmdPut, Key: "test1", Value: "val1"},
				{CmdType: qdb.CmdPut, Key: "test2", Value: "val2"},
				{CmdType: qdb.CmdDelete, Key: "test3"},
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
		t.Run("fail commit tran after begin another tran", func(_ *testing.T) {
			tran1, err := qdb.NewTransaction()
			is.NoError(err)
			err = db.BeginTransaction(ctx, tran1)
			is.NoError(err)
			statements := []qdb.QdbStatement{
				{CmdType: qdb.CmdPut, Key: "test1", Value: "val1"},
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
		t.Run("suddenly there was a boxwood", func(_ *testing.T) {
			tran1, err := qdb.NewTransaction()
			is.NoError(err)
			err = db.BeginTransaction(ctx, tran1)
			is.NoError(err)
			statements := []qdb.QdbStatement{
				{CmdType: qdb.CmdPut, Key: "test1", Value: "val1"},
			}
			err = tran1.Append(statements)
			is.NoError(err)
			_, err = db.Client().Delete(ctx, "transaction_request")
			is.NoError(err)

			err = db.CommitTransaction(ctx, tran1)
			is.EqualError(err, fmt.Sprintf("transaction '%s' can't be committed", tran1.Id()))
		})

		t.Run("fails invalid tran", func(_ *testing.T) {
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
	t.Run("test Begin tran", func(_ *testing.T) {
		ctx, cancel := context.WithTimeout(context.TODO(), TestTimeout)
		defer cancel()
		db, err := setupSubTest(ctx)
		assert.NoError(err)
		t.Run("empty request returns no task groups", func(_ *testing.T) {
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
		t.Run("base case", func(_ *testing.T) {
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

func TestCreateSequence(t *testing.T) {
	is := assert.New(t)
	err := setupTestSet(t)
	is.NoError(err)
	defer func() {
		_ = Down()
	}()
	is.NoError(err)

	t.Run("test CreateSequence", func(_ *testing.T) {
		ctx, cancel := context.WithTimeout(context.TODO(), TestTimeout)
		defer cancel()
		db, err := setupSubTest(ctx)
		is.NoError(err)

		t.Run("happy path", func(_ *testing.T) {
			err := cleanupDb(ctx, db)
			is.NoError(err)
			seqExists, err := db.CheckSequence(ctx, "test1")
			is.NoError(err)
			is.Equal(false, seqExists)
			statements, err := db.CreateSequence(ctx, "test1", 0)
			is.NoError(err)
			is.Equal(1, len(statements))
			err = db.ExecNoTransaction(ctx, statements)
			is.NoError(err)
			seqExists, err = db.CheckSequence(ctx, "test1")
			is.NoError(err)
			is.Equal(true, seqExists)
			actual, err := db.ListSequences(ctx)
			is.NoError(err)
			is.Equal([]string{"test1"}, actual)
		})

	})

}

func TestKeyRangeVersion(t *testing.T) {
	is := assert.New(t)
	err := setupTestSet(t)
	is.NoError(err)
	defer func() {
		_ = Down()
	}()
	is.NoError(err)

	ctx, cancel := context.WithTimeout(context.TODO(), TestTimeout)
	defer cancel()
	db, err := setupSubTest(ctx)
	is.NoError(err)

	qdb.RunTestKeyRangeChangeVersion(t, db)
}

func TestMetadataErrors(t *testing.T) {
	require.NoError(t, setupTestSet(t))
	t.Cleanup(func() { require.NoError(t, Down()) })
	ctx, cancel := context.WithTimeout(context.Background(), TestTimeout)
	defer cancel()
	db, err := setupSubTest(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Client().Close()) })
	relation := &rfqn.RelationFQN{SchemaName: "sales", RelationName: "countries"}
	for _, tt := range []struct {
		name          string
		run           func() error
		code, message string
	}{
		{"distribution", func() error { _, err := db.GetDistribution(ctx, "missing"); return err }, spqrerror.SPQR_OBJECT_NOT_EXIST, `distribution "missing" not found`},
		{"reference relation", func() error { _, err := db.GetReferenceRelation(ctx, relation); return err }, spqrerror.SPQR_OBJECT_NOT_EXIST, `reference relation "sales.countries" not found`},
		{"index", func() error { return db.DropUniqueIndex(ctx, "missing") }, spqrerror.SPQR_OBJECT_NOT_EXIST, `unique index "missing" not found`},
		{"shard", func() error { return db.AlterShard(ctx, &qdb.Shard{ID: "missing"}) }, spqrerror.SPQR_NO_DATASHARD, `Shard "missing" not found.`},
		{"move task", func() error { return db.UpdateMoveTask(ctx, &qdb.MoveTask{ID: "missing"}) }, spqrerror.SPQR_OBJECT_NOT_EXIST, `move task "missing" not found`},
		{"task group", func() error { return db.WriteMoveTask(ctx, &qdb.MoveTask{ID: "task1", TaskGroupID: "missing"}) }, spqrerror.SPQR_OBJECT_NOT_EXIST, `task group "missing" not found`},
		{"redistribute task", func() error { return db.UpdateRedistributeTask(ctx, &qdb.RedistributeTask{ID: "missing"}) }, spqrerror.SPQR_OBJECT_NOT_EXIST, `redistribute task "missing" not found`},
		{"key range move", func() error { return db.DeleteKeyRangeMove(ctx, "missing", false) }, spqrerror.SPQR_OBJECT_NOT_EXIST, `key range move "missing" not found`},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var spErr *spqrerror.SpqrError
			require.ErrorAs(t, tt.run(), &spErr)
			assert.Equal(t, tt.code, spErr.ErrorCode)
			assert.Equal(t, tt.message, spErr.Error())
		})
	}
	t.Run("alter reference relation schema", func(t *testing.T) {
		statements, err := db.CreateDistribution(ctx, qdb.NewDistribution("REPLICATED", nil))
		require.NoError(t, err)
		require.NoError(t, db.ExecNoTransaction(ctx, statements))
		require.NoError(t, db.AlterDistributionAttach(ctx, "REPLICATED", []*qdb.DistributedRelation{{
			Name: relation.RelationName, SchemaName: relation.SchemaName, ReplicatedRelation: true,
		}}))
		require.NoError(t, db.CreateReferenceRelation(ctx, &qdb.ReferenceRelation{
			TableName: relation.RelationName, SchemaName: relation.SchemaName,
		}))
		// DROP deletes the reference record before its distribution mapping.
		_, err = db.Client().Delete(ctx, "/reference_relations/"+relation.RelationName)
		require.NoError(t, err)

		err = db.AlterReplicatedRelationSchema(ctx, "REPLICATED", relation, "archive")
		var spErr *spqrerror.SpqrError
		require.ErrorAs(t, err, &spErr)
		assert.Equal(t, spqrerror.SPQR_OBJECT_NOT_EXIST, spErr.ErrorCode)
		assert.Equal(t, "Run 'SHOW reference_relations' to see all configured reference relations.", spErr.ErrHint)
		assert.EqualError(t, err, `failed to get reference table: reference relation "sales.countries" not found`)
	})
	require.NoError(t, db.WriteMoveTaskGroup(ctx, "group1", &qdb.MoveTaskGroup{}, 0, nil))
	for _, tt := range []struct {
		name    string
		create  func() error
		message string
	}{
		{"shard", func() error { return db.AddShard(ctx, &qdb.Shard{ID: "sh1"}) }, `shard "sh1" already exists`},
		{"group", func() error { return db.WriteMoveTaskGroup(ctx, "group2", &qdb.MoveTaskGroup{}, 0, nil) }, `task group "group2" already exists`},
		{"move task", func() error { return db.WriteMoveTask(ctx, &qdb.MoveTask{ID: "task1", TaskGroupID: "group1"}) }, `move task "task1" already exists`},
	} {
		t.Run("duplicate "+tt.name, func(t *testing.T) {
			require.NoError(t, tt.create())
			var spErr *spqrerror.SpqrError
			require.ErrorAs(t, tt.create(), &spErr)
			assert.Equal(t, spqrerror.SPQR_INVALID_REQUEST, spErr.ErrorCode)
			assert.Equal(t, tt.message, spErr.Error())
			assert.NotEmpty(t, spErr.ErrHint)
		})
	}
	require.NoError(t, db.CreateRedistributeTask(ctx, &qdb.RedistributeTask{ID: "rt1", KeyRangeId: "kr1"}))
	for _, tt := range []struct {
		task    *qdb.RedistributeTask
		message string
	}{
		{&qdb.RedistributeTask{ID: "rt1", KeyRangeId: "kr2"}, `redistribute task "rt1" already exists`},
		{&qdb.RedistributeTask{ID: "rt2", KeyRangeId: "kr1"}, `redistribute task for key range "kr1" already exists`},
	} {
		var spErr *spqrerror.SpqrError
		require.ErrorAs(t, db.CreateRedistributeTask(ctx, tt.task), &spErr)
		assert.Equal(t, spqrerror.SPQR_INVALID_REQUEST, spErr.ErrorCode)
		assert.Equal(t, tt.message, spErr.Error())
	}
	group, err := db.GetMoveTaskGroup(ctx, "absent")
	require.NoError(t, err)
	assert.Nil(t, group)
}
