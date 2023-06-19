package server

import (
	"crypto/tls"
	"fmt"

	"github.com/jackc/pgproto3/v2"
	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/pkg/spqrlog"
	"github.com/pg-sharding/spqr/router/pkg/datashard"
)

type ShardServer struct {
	rule *config.BackendRule

	pool  datashard.DBPool
	shard datashard.Shard
	mp    map[uint64]string
}

func (srv *ShardServer) HasPrepareStatement(hash uint64) bool {
	_, ok := srv.mp[hash]
	return ok
}

func (srv *ShardServer) Name() string {
	return srv.shard.Name()
}

func (srv *ShardServer) PrepareStatement(hash uint64) {
	srv.mp[hash] = "yes"
}

func (srv *ShardServer) Sync() int {
	//TODO implement me
	panic("implement me")
}

func (srv *ShardServer) Reset() error {
	return nil
}

func (srv *ShardServer) UnRouteShard(shkey kr.ShardKey, rule *config.FrontendRule) error {
	if srv.shard.SHKey().Name != shkey.Name {
		return fmt.Errorf("active datashard does not match unrouted: %v != %v", srv.shard.SHKey().Name, shkey.Name)
	}

	if err := srv.Cleanup(rule); err != nil {
		return err
	}

	spqrlog.Logger.Printf(spqrlog.DEBUG1, "put connection %p to %v back to pool\n", &srv.shard, srv.shard.Instance().Hostname())
	if err := srv.pool.Put(shkey, srv.shard); err != nil {
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
	return srv.shard.AddTLSConf(cfg)
}

func NewShardServer(rule *config.BackendRule, spool datashard.DBPool) *ShardServer {
	return &ShardServer{
		rule: rule,
		pool: spool,
		mp:   make(map[uint64]string),
	}
}

func (srv *ShardServer) Send(query pgproto3.FrontendMessage) error {
	spqrlog.Logger.Printf(spqrlog.DEBUG3, "single-shard %p sending msg to server %T", srv, query)
	return srv.shard.Send(query)
}

func (srv *ShardServer) Receive() (pgproto3.BackendMessage, error) {
	msg, err := srv.shard.Receive()
	spqrlog.Logger.Printf(spqrlog.DEBUG3, "single-shard %p recv msg from server %T", srv, msg)
	return msg, err
}

func (srv *ShardServer) Cleanup(rule *config.FrontendRule) error {
	return srv.shard.Cleanup(rule)
}

func (srv *ShardServer) Cancel() error {
	return srv.shard.Cancel()
}

var _ Server = &ShardServer{}
