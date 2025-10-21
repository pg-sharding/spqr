package qdb_integration_test

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"

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
	args2 = append(args2, "compose", "-f", "docker-compose.yaml", "-p", "qdb_test")
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

func TestQdb(t *testing.T) {
	is := assert.New(t)
	err := setupTestSet(t)
	is.NoError(err)
	defer func() {
		Down()
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
			expected := qdb.NewShard("sh1", []string{"denchick.rs", "reshke.ru"})
			is.Equal(actual, expected)
		})

		t.Run("fail", func(t *testing.T) {
			err := cleanupDb(ctx, db)
			err = db.AddShard(ctx, qdb.NewShard("sh1", []string{"denchick.rs", "reshke.ru"}))
			is.NoError(err)
			err = db.AddShard(ctx, qdb.NewShard("sh1", []string{"bug.rs", "bug.ru"}))
			is.Error(err)
			actual, err := db.GetShard(ctx, "sh1")
			expected := qdb.NewShard("sh1", []string{"denchick.rs", "reshke.ru"})
			is.Equal(actual, expected)
		})
	})

}
