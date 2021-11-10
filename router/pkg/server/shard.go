package server

import (
	"crypto/tls"
	"fmt"
	"github.com/jackc/pgproto3/v2"
	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/conn"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/router/pkg/datashard"
	"golang.org/x/xerrors"
)

type ShardServer struct {
	rule *config.BERule

	pool conn.ConnPool

	shard datashard.Shard
}

func (srv *ShardServer) Reset() error {
	return nil
}

func (srv *ShardServer) UnrouteShard(shkey kr.ShardKey) error {

	if srv.shard.SHKey().Name != shkey.Name {
		return xerrors.Errorf("active datashard does not match unrouted: %v != %v", srv.shard.SHKey().Name, shkey.Name)
	}

	pgi := srv.shard.Instance()
	fmt.Printf("put connection to %v back to pool\n", pgi.Hostname())

	if err := srv.pool.Put(shkey, pgi); err != nil {
		return err
	}

	srv.shard = nil

	return nil
}

func (srv *ShardServer) AddShard(shkey kr.ShardKey) error {
	if srv.shard != nil {
		return xerrors.New("single datashard server does not support more than 2 datashard connection simultaneously")

	}

	if pgi, err := srv.pool.Connection(shkey); err != nil {
		return err
	} else {

		srv.shard, err = datashard.NewShard(shkey, pgi, config.RouterConfig().RouterConfig.ShardMapping[shkey.Name])

		if err != nil {
			return err
		}
	}

	return nil
}

func (srv *ShardServer) AddTLSConf(cfg *tls.Config) error {
	return srv.shard.ReqBackendSsl(cfg)
}

func NewShardServer(rule *config.BERule, spool conn.ConnPool) *ShardServer {
	return &ShardServer{
		rule: rule,
		pool: spool,
	}
}

func (srv *ShardServer) Send(query pgproto3.FrontendMessage) error {
	return srv.shard.Send(query)
}

func (srv *ShardServer) Receive() (pgproto3.BackendMessage, error) {
	return srv.shard.Receive()
}

func (srv *ShardServer) Cleanup() error {

	if srv.rule.PoolRollback {
		if err := srv.Send(&pgproto3.Query{
			String: "ROLLBACK",
		}); err != nil {
			return err
		}
	}

	if srv.rule.PoolDiscard {
		if err := srv.Send(&pgproto3.Query{
			String: "DISCARD ALL",
		}); err != nil {
			return err
		}

	}

	return nil
}

var _ Server = &ShardServer{}
