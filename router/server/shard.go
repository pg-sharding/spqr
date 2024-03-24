package server

import (
	"crypto/tls"
	"fmt"
	"sync"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/pkg/pool"
	"github.com/pg-sharding/spqr/pkg/shard"
	"github.com/pg-sharding/spqr/pkg/spqrlog"
	"github.com/pg-sharding/spqr/pkg/txstatus"
)

var ErrShardUnavailable = fmt.Errorf("shard is unavailable, try again later")

type ShardServer struct {
	pool  pool.DBPool
	shard shard.Shard
	// protects shard
	mu sync.RWMutex
}

func NewShardServer(spool pool.DBPool) *ShardServer {
	return &ShardServer{
		pool: spool,
		mu:   sync.RWMutex{},
	}
}

// TODO : unit tests
func (srv *ShardServer) HasPrepareStatement(hash uint64) (bool, shard.PreparedStatementDescriptor) {
	return srv.shard.HasPrepareStatement(hash)
}

// TODO : unit tests
func (srv *ShardServer) Name() string {
	srv.mu.RLock()
	defer srv.mu.RUnlock()
	if srv.shard == nil {
		return ""
	}
	return srv.shard.Name()
}

// TODO : unit tests
func (srv *ShardServer) PrepareStatement(hash uint64, rd shard.PreparedStatementDescriptor) {
	srv.shard.PrepareStatement(hash, rd)
}

// TODO : unit tests
func (srv *ShardServer) Sync() int64 {
	srv.mu.RLock()
	defer srv.mu.RUnlock()
	if srv.shard == nil {
		return 0
	}
	return srv.shard.Sync()
}

// TODO : unit tests
func (srv *ShardServer) Reset() error {
	// todo there are no shard writes, so use rLock
	srv.mu.RLock()
	defer srv.mu.RUnlock()
	if srv.shard == nil {
		return ErrShardUnavailable
	}

	return srv.pool.Put(srv.shard)
}

// TODO : unit tests
func (srv *ShardServer) UnRouteShard(shkey kr.ShardKey, rule *config.FrontendRule) error {
	srv.mu.Lock()
	defer srv.mu.Unlock()
	if srv.shard == nil {
		return nil
	}

	defer func() {
		srv.shard = nil
	}()

	if srv.shard.SHKey().Name != shkey.Name {
		return fmt.Errorf("active datashard does not match unrouted: %v != %v", srv.shard.SHKey().Name, shkey.Name)
	}

	if srv.shard.Sync() != 0 {
		/* will automaticly discard connection,
		but we will not perform cleanup, which may stuck forever */
		return srv.pool.Put(srv.shard)
	}

	if err := srv.cleanupLockFree(rule); err != nil {
		return err
	}

	if err := srv.pool.Put(srv.shard); err != nil {
		return err
	}

	return nil
}

// TODO : unit tests
func (srv *ShardServer) AddDataShard(clid uint, shkey kr.ShardKey, tsa string) error {
	srv.mu.Lock()
	defer srv.mu.Unlock()
	if srv.shard != nil {
		return fmt.Errorf("single datashard " +
			"server does not support more than 1 datashard connection simultaneously")
	}

	var err error
	if srv.shard, err = srv.pool.Connection(clid, shkey, tsa); err != nil {
		return err
	}

	return nil
}

// TODO : unit tests
func (srv *ShardServer) AddTLSConf(cfg *tls.Config) error {
	srv.mu.RLock()
	defer srv.mu.RUnlock()
	if srv.shard == nil {
		return ErrShardUnavailable
	}
	return srv.shard.AddTLSConf(cfg)
}

// TODO : unit tests
func (srv *ShardServer) Send(query pgproto3.FrontendMessage) error {
	srv.mu.RLock()
	defer srv.mu.RUnlock()
	spqrlog.Zero.Debug().
		Uint("single-shard", srv.shard.ID()).
		Type("query-type", query).
		Msg("single-shard sending msg to server")
	if srv.shard == nil {
		return ErrShardUnavailable
	}
	return srv.shard.Send(query)
}

// TODO : unit tests
func (srv *ShardServer) Receive() (pgproto3.BackendMessage, error) {
	srv.mu.RLock()
	defer srv.mu.RUnlock()
	msg, err := srv.shard.Receive()
	spqrlog.Zero.Debug().
		Uint("single-shard", srv.shard.ID()).
		Type("message-type", msg).
		Str("txstatus", srv.TxStatus().String()).
		Uints("shards", shard.ShardIDs(srv.Datashards())).
		Err(err).
		Msg("single-shard receiving msg from server")
	return msg, err
}

// TODO : unit tests
func (srv *ShardServer) Cleanup(rule *config.FrontendRule) error {
	srv.mu.RLock()
	defer srv.mu.RUnlock()
	return srv.cleanupLockFree(rule)
}

// TODO : unit tests
func (srv *ShardServer) cleanupLockFree(rule *config.FrontendRule) error {
	if srv.shard == nil {
		return ErrShardUnavailable
	}
	return srv.shard.Cleanup(rule)
}

// TODO : unit tests
func (srv *ShardServer) Cancel() error {
	srv.mu.RLock()
	defer srv.mu.RUnlock()
	if srv.shard == nil {
		return ErrShardUnavailable
	}
	return srv.shard.Cancel()
}

// TODO : unit tests
func (srv *ShardServer) SetTxStatus(tx txstatus.TXStatus) {
	srv.mu.RLock()
	defer srv.mu.RUnlock()
	if srv.shard != nil {
		srv.shard.SetTxStatus(tx)
	}
}

// TODO : unit tests
func (srv *ShardServer) TxStatus() txstatus.TXStatus {
	srv.mu.RLock()
	defer srv.mu.RUnlock()
	if srv.shard == nil {
		return txstatus.TXERR
	}
	return srv.shard.TxStatus()
}

// TODO : unit tests
func (srv *ShardServer) Datashards() []shard.Shard {
	srv.mu.RLock()
	defer srv.mu.RUnlock()
	return []shard.Shard{srv.shard}
}

var _ Server = &ShardServer{}
