package qdb

import (
	"context"
	"encoding/json"

	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/spqrlog"
	"github.com/spaolacci/murmur3"
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
	local   *MemQDB
	localPg *MemPgQDB
)

func GetMemQDB() (*MemQDB, error) {
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

func GetMemPgQDB() (*MemPgQDB, error) {
	if localPg != nil {
		return localPg, nil
	}

	if config.RouterConfig().MemqdbBackupPath != "" {
		db, err := RestoreMemPgQDB(config.RouterConfig().MemqdbBackupPath)
		if err != nil {
			return nil, err
		}

		localPg = db
		return localPg, err
	}
	db, err := NewMemPgQDB(config.RouterConfig().MemqdbBackupPath)
	if err != nil {
		return nil, err
	}

	localPg = db
	return localPg, err
}

func GetStateKeeperQDB() (StateKeeperQDB, error) {
	if config.RouterConfig().StoreTxDataPostgresql {
		return GetMemPgQDB()
	} else {
		return GetMemQDB()
	}
}

// GetQDBStateHash calculates hash of the QDB's state.
// Currently, only distributions, relations and key ranges are considered
func GetQDBStateHash(ctx context.Context, q QDB) (uint64, error) {
	hasher := murmur3.New64()
	distributions, err := q.ListDistributions(ctx)
	if err != nil {
		return 0, err
	}
	for _, distribution := range distributions {
		dsJSON, err := json.Marshal(distribution)
		if err != nil {
			return 0, err
		}
		if _, err := hasher.Write(dsJSON); err != nil {
			return 0, err
		}
	}
	keyRanges, err := q.ListAllKeyRanges(ctx)
	if err != nil {
		return 0, err
	}
	for _, keyRange := range keyRanges {
		krJSON, err := json.Marshal(keyRange)
		if err != nil {
			return 0, err
		}
		if _, err := hasher.Write(krJSON); err != nil {
			return 0, err
		}
	}
	return hasher.Sum64(), nil
}
