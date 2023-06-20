package qdb

import (
	"context"

	"github.com/pg-sharding/spqr/pkg/spqrlog"
	"go.etcd.io/etcd/client/v3/concurrency"
)

func unlockMutex(mu *concurrency.Mutex, ctx context.Context) {
	if err := mu.Unlock(ctx); err != nil {
		spqrlog.Logger.PrintError(err)
	}
}

func closeSession(sess *concurrency.Session) {
	if err := sess.Close(); err != nil {
		spqrlog.Logger.PrintError(err)
	}
}