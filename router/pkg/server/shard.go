package server

import (
	"crypto/tls"
	"fmt"

	"github.com/jackc/pgproto3/v2"
	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/pkg/spqrlog"
	"github.com/pg-sharding/spqr/router/pkg/datashard"
	"golang.org/x/xerrors"
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

func (srv *ShardServer) UnRouteShard(shkey kr.ShardKey) error {
	if srv.shard.SHKey().Name != shkey.Name {
		return fmt.Errorf("active datashard does not match unrouted: %v != %v", srv.shard.SHKey().Name, shkey.Name)
	}

	if err := srv.Cleanup(); err != nil {
		return err
	}

	spqrlog.Logger.Printf(spqrlog.DEBUG1, "put connection to %v back to pool\n", srv.shard.Instance().Hostname())
	if err := srv.pool.Put(shkey, srv.shard); err != nil {
		return err
	}

	srv.shard = nil
	return nil
}

func (srv *ShardServer) AddShard(shkey kr.ShardKey) error {
	if srv.shard != nil {
		return xerrors.New("single datashard server does not support more than 1 datashard connection simultaneously")
	}

	var err error
	if srv.shard, err = srv.pool.Connection(shkey, srv.rule); err != nil {
		return err
	}

	return nil
}

func (srv *ShardServer) AddTLSConf(cfg *tls.Config) error {
	return srv.shard.ReqBackendSsl(cfg)
}

func NewShardServer(rule *config.BackendRule, spool datashard.DBPool) *ShardServer {
	return &ShardServer{
		rule: rule,
		pool: spool,
		mp:   make(map[uint64]string),
	}
}

func (srv *ShardServer) Send(query pgproto3.FrontendMessage) error {
	spqrlog.Logger.Printf(spqrlog.DEBUG3, "send msg to server %+v", query)
	return srv.shard.Send(query)
}

func (srv *ShardServer) Receive() (pgproto3.BackendMessage, error) {
	msg, err := srv.shard.Receive()
	spqrlog.Logger.Printf(spqrlog.DEBUG3, "recv msg from server %T", msg)
	return msg, err
}

func (srv *ShardServer) fire(q string) error {
	if err := srv.Send(&pgproto3.Query{
		String: q,
	}); err != nil {
		spqrlog.Logger.Printf(spqrlog.DEBUG1, "error firing request to conn")
		return err
	}

	for {
		if msg, err := srv.Receive(); err != nil {
			return err
		} else {
			spqrlog.Logger.Printf(spqrlog.DEBUG1, "rollback resp %T", msg)

			switch msg.(type) {
			case *pgproto3.ReadyForQuery:
				return nil
			}
		}
	}
}

func (srv *ShardServer) Cleanup() error {
	if srv.rule.PoolRollback {
		if err := srv.fire("ROLLBACK"); err != nil {
			return err
		}
	}

	if srv.rule.PoolDiscard {
		if err := srv.fire("DISCARD ALL"); err != nil {
			return err
		}
	}

	return nil
}

var _ Server = &ShardServer{}
