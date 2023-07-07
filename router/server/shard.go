package server

import (
	"crypto/tls"
	"fmt"
	"sync"

	"github.com/jackc/pgproto3/v2"
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
	mp    map[uint64]string
	// protects shard
	mu sync.RWMutex
}

func NewShardServer(spool pool.DBPool) *ShardServer {
	return &ShardServer{
		pool: spool,
		mp:   make(map[uint64]string),
		mu:   sync.RWMutex{},
	}
}

func (srv *ShardServer) HasPrepareStatement(hash uint64) bool {
	_, ok := srv.mp[hash]
	return ok
}

func (srv *ShardServer) Name() string {
	srv.mu.RLock()
	defer srv.mu.RUnlock()
	if srv.shard == nil {
		return ""
	}
	return srv.shard.Name()
}

func (srv *ShardServer) PrepareStatement(hash uint64) {
	srv.mp[hash] = "yes"
}

func (srv *ShardServer) Sync() int64 {
	srv.mu.RLock()
	defer srv.mu.RUnlock()
	if srv.shard == nil {
		return 0
	}
	return srv.shard.Sync()
}

func (srv *ShardServer) Reset() error {
	// todo there are no shard writes, so use rLock
	srv.mu.RLock()
	defer srv.mu.RUnlock()
	if srv.shard == nil {
		return ErrShardUnavailable
	}
	return nil
}

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

func (srv *ShardServer) AddDataShard(clid string, shkey kr.ShardKey, tsa string) error {
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

func (srv *ShardServer) AddTLSConf(cfg *tls.Config) error {
	srv.mu.RLock()
	defer srv.mu.RUnlock()
	if srv.shard == nil {
		return ErrShardUnavailable
	}
	return srv.shard.AddTLSConf(cfg)
}

func (srv *ShardServer) Send(query pgproto3.FrontendMessage) error {
	srv.mu.RLock()
	defer srv.mu.RUnlock()
	spqrlog.Logger.Printf(spqrlog.DEBUG5, "single-shard %p sending msg to server %T", srv, query)
	if srv.shard == nil {
		return ErrShardUnavailable
	}
	return srv.shard.Send(query)
}

func (srv *ShardServer) Receive() (pgproto3.BackendMessage, error) {
	srv.mu.RLock()
	defer srv.mu.RUnlock()
	msg, err := srv.shard.Receive()
	spqrlog.Logger.Printf(spqrlog.DEBUG5, "single-shard %p recv msg from server %T, tx status: %d", srv, msg, srv.TxStatus())
	return msg, err
}

func (srv *ShardServer) Cleanup(rule *config.FrontendRule) error {
	srv.mu.RLock()
	defer srv.mu.RUnlock()
	return srv.cleanupLockFree(rule)
}

func (srv *ShardServer) cleanupLockFree(rule *config.FrontendRule) error {
	if srv.shard == nil {
		return ErrShardUnavailable
	}
	return srv.shard.Cleanup(rule)
}

func (srv *ShardServer) Cancel() error {
	srv.mu.RLock()
	defer srv.mu.RUnlock()
	if srv.shard == nil {
		return ErrShardUnavailable
	}
	return srv.shard.Cancel()
}

func (srv *ShardServer) SetTxStatus(tx txstatus.TXStatus) {
	srv.mu.RLock()
	defer srv.mu.RUnlock()
	if srv.shard != nil {
		srv.shard.SetTxStatus(tx)
	}
}

func (srv *ShardServer) TxStatus() txstatus.TXStatus {
	srv.mu.RLock()
	defer srv.mu.RUnlock()
	if srv.shard == nil {
		return txstatus.TXERR
	}
	return srv.shard.TxStatus()
}

func (srv *ShardServer) Datashards() []shard.Shard {
	srv.mu.RLock()
	defer srv.mu.RUnlock()
	return []shard.Shard{srv.shard}
}

var _ Server = &ShardServer{}
