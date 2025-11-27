package qdb

import (
	"context"

	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/spqrlog"
	"go.etcd.io/etcd/client/v3/concurrency"
)

// TODO : unit tests
func unlockMutex(mu *concurrency.Mutex, ctx context.Context) {
	if err := mu.Unlock(ctx); err != nil {
		spqrlog.Zero.Error().Err(err).Msg("")
	}
}

// TODO : unit tests
func closeSession(sess *concurrency.Session) {
	if err := sess.Close(); err != nil {
		spqrlog.Zero.Error().Err(err).Msg("")
	}
}

var (
	local *MemQDB = nil
)

func GetQDB() (*MemQDB, error) {
	if local != nil {
		return local, nil
	}

	if config.RouterConfig().MemqdbBackupPath != "" {
		db, err := RestoreQDB(config.RouterConfig().MemqdbBackupPath)
		if err != nil {
			return nil, err
		}

		local = db
		return local, err
	}
	db, err := NewMemQDB(config.RouterConfig().MemqdbBackupPath)
	if err != nil {
		return nil, err
	}

	local = db
	return local, err
}
