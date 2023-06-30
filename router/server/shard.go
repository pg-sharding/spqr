package server

import (
	"crypto/tls"
	"fmt"

	"github.com/jackc/pgproto3/v2"
	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/datashard"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/pkg/shard"
	"github.com/pg-sharding/spqr/pkg/spqrlog"
	"github.com/pg-sharding/spqr/pkg/txstatus"
)

var ErrShardUnavailable = fmt.Errorf("shard is unavailable, try again later")

type ShardServer struct {
	rule  *config.BackendRule
	pool  datashard.DBPool
	shard shard.Shard
	mp    map[uint64]string
}

func NewShardServer(rule *config.BackendRule, spool datashard.DBPool) *ShardServer {
	return &ShardServer{
		rule: rule,
		pool: spool,
		mp:   make(map[uint64]string),
	}
}

func (srv *ShardServer) HasPrepareStatement(hash uint64) bool {
	_, ok := srv.mp[hash]
	return ok
}

func (srv *ShardServer) Name() string {
	if srv.shard == nil {
		return ""
	}
	return srv.shard.Name()
}

func (srv *ShardServer) PrepareStatement(hash uint64) {
	srv.mp[hash] = "yes"
}

func (srv *ShardServer) Sync() int64 {
	if srv.shard == nil {
		return 0
	}
	return srv.shard.Sync()
}

func (srv *ShardServer) Reset() error {
	if srv.shard == nil {
		return ErrShardUnavailable
	}
	return nil
}

func (srv *ShardServer) UnRouteShard(shkey kr.ShardKey, rule *config.FrontendRule) error {
	if srv.shard == nil {
		return nil
	}

	if srv.shard.SHKey().Name != shkey.Name {
		return fmt.Errorf("active datashard does not match unrouted: %v != %v", srv.shard.SHKey().Name, shkey.Name)
	}

	if err := srv.Cleanup(rule); err != nil {
		return err
	}

	if err := srv.pool.Put(srv.shard); err != nil {
		return err
	}

	srv.shard = nil
	return nil
}

func (srv *ShardServer) AddDataShard(clid string, shkey kr.ShardKey, tsa string) error {
	if srv.shard != nil {
		return fmt.Errorf("single datashard " +
			"server does not support more than 1 datashard connection simultaneously")
	}

	var err error
	if srv.shard, err = srv.pool.Connection(clid, shkey, srv.rule, tsa); err != nil {
		return err
	}

	return nil
}

func (srv *ShardServer) AddTLSConf(cfg *tls.Config) error {
	if srv.shard == nil {
		return ErrShardUnavailable
	}
	return srv.shard.AddTLSConf(cfg)
}

func (srv *ShardServer) Send(query pgproto3.FrontendMessage) error {
	spqrlog.Logger.Printf(spqrlog.DEBUG5, "single-shard %p sending msg to server %T", srv, query)
	if srv.shard == nil {
		return ErrShardUnavailable
	}
	return srv.shard.Send(query)
}

func (srv *ShardServer) Receive() (pgproto3.BackendMessage, error) {
	msg, err := srv.shard.Receive()
	spqrlog.Logger.Printf(spqrlog.DEBUG5, "single-shard %p recv msg from server %T, tx status: %d", srv, msg, srv.TxStatus())
	return msg, err
}

func (srv *ShardServer) Cleanup(rule *config.FrontendRule) error {
	if srv.shard == nil {
		return ErrShardUnavailable
	}
	return srv.shard.Cleanup(rule)
}

func (srv *ShardServer) Cancel() error {
	if srv.shard == nil {
		return ErrShardUnavailable
	}
	return srv.shard.Cancel()
}

func (srv *ShardServer) SetTxStatus(tx txstatus.TXStatus) {
	if srv.shard != nil {
		srv.shard.SetTxStatus(tx)
	}
}

func (srv *ShardServer) TxStatus() txstatus.TXStatus {
	if srv.shard == nil {
		return txstatus.TXERR
	}
	return srv.shard.TxStatus()
}

func (srv *ShardServer) Datashards() []shard.Shard {
	return []shard.Shard{srv.shard}
}

var _ Server = &ShardServer{}
