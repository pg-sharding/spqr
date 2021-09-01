package internal

import (
	"crypto/tls"
	"net"

	"github.com/jackc/pgproto3"
	"github.com/pg-sharding/spqr/internal/config"
	"github.com/wal-g/tracelog"
)

type Server interface {
	Send(query pgproto3.FrontendMessage) error
	Receive() (pgproto3.BackendMessage, error)

	AddTLSConf(cfg *tls.Config)

	Cleanup() error
}

type ShardServer struct {
	rule   *config.BERule
	pgconn *PgConn
}

func (srv *ShardServer) AddTLSConf(cfg *tls.Config) {
	srv.pgconn.ReqBackendSsl(cfg)
}

func NewShardServer(rule *config.BERule, conn net.Conn, shard Shard) *ShardServer {
	return &ShardServer{
		rule: rule,
		pgconn: &PgConn{
			conn:  conn,
			shard: shard,
		},
	}
}

func (srv *ShardServer) Send(query pgproto3.FrontendMessage) error {
	return srv.pgconn.frontend.Send(query)
}

func (srv *ShardServer) Receive() (pgproto3.BackendMessage, error) {
	return srv.pgconn.frontend.Receive()
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

type MultiShardServer struct {
	rule *config.BERule

	servers map[string]*PgConn
}

func (m *MultiShardServer) AddTLSConf(cfg *tls.Config) {

	for _, pgconn := range m.servers {
		pgconn.ReqBackendSsl(cfg)
	}

}

func (m *MultiShardServer) Send(msg pgproto3.FrontendMessage) error {
	for _, pgconn := range m.servers {
		err := pgconn.frontend.Send(msg)
		if err != nil {
			tracelog.InfoLogger.PrintError(err)
			//
		}
	}

	return nil
}

func (m *MultiShardServer) Receive() (pgproto3.BackendMessage, error) {

	ret := &pgproto3.DataRow{
		Values: [][]byte{},
	}

	for _, pgconn := range m.servers {
		msg, err := pgconn.frontend.Receive()
		if err != nil {
			//
		}

		switch v := msg.(type) {
		case *pgproto3.DataRow:
			ret.Values = append(ret.Values, v.Values...)
		}
	}

	return ret, nil
}

func (m *MultiShardServer) Cleanup() error {

	if m.rule.PoolRollback {
		if err := m.Send(&pgproto3.Query{
			String: "ROLLBACK",
		}); err != nil {
			return err
		}
	}

	if m.rule.PoolDiscard {
		if err := m.Send(&pgproto3.Query{
			String: "DISCARD ALL",
		}); err != nil {
			return err
		}
	}

	return nil
}

var _ Server = &MultiShardServer{}

func NewMultiShardServer(rule *config.BERule, shards []Shard) (Server, error) {
	ret := &MultiShardServer{
		rule:    rule,
		servers: map[string]*PgConn{},
	}

	for _, shard := range shards {

		netconn, err := shard.Connect(shard.Cfg().Proto)

		if err != nil {
			return nil, err
		}

		pgconn := &PgConn{
			conn:  netconn,
			shard: shard,
		}

		ret.servers[shard.Name()] = pgconn
	}

	return ret, nil
}
