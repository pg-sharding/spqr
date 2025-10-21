package qdbintegration

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/pg-sharding/spqr/qdb"
	"github.com/pg-sharding/spqr/test/feature/testutil"
	"github.com/stretchr/testify/assert"
	clientv3 "go.etcd.io/etcd/client/v3"
)

const (
	EtcdPort    = 2379
	TestTimeout = 10 * time.Second
)

type testContext struct {
	composer testutil.Composer
}

func setupTestSet(t *testing.T) (*testContext, error) {
	t.Log("load etcd")
	var err error
	tctx := new(testContext)
	tctx.composer, err = testutil.NewDockerComposer("spqr", "docker-compose.yaml")
	if err != nil {
		return nil, err
	}
	err = tctx.composer.Up(make([]string, 0))
	if err != nil {
		return nil, err
	}
	return tctx, nil
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

func TestQdb(t *testing.T) {
	is := assert.New(t)
	tctx, err := setupTestSet(t)
	is.NoError(err)
	defer func() {
		tctx.composer.Down()
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
			sh := qdb.NewShard("sh1", []string{"denchik.rs", "reshke.ru"})
			err = db.AddShard(ctx, sh)
			is.NoError(err)
			actual, err := db.GetShard(ctx, "sh1")
			expected := qdb.NewShard("sh1", []string{"denchik.rs", "reshke.ru"})
			is.Equal(actual, expected)
		})

		t.Run("fail", func(t *testing.T) {
			err := cleanupDb(ctx, db)
			err = db.AddShard(ctx, qdb.NewShard("sh1", []string{"denchik.rs", "reshke.ru"}))
			is.NoError(err)
			err = db.AddShard(ctx, qdb.NewShard("sh1", []string{"bug.rs", "bug.ru"}))
			is.Error(err)
			actual, err := db.GetShard(ctx, "sh1")
			expected := qdb.NewShard("sh1", []string{"denchik.rs", "reshke.ru"})
			is.Equal(actual, expected)
		})
	})

}
