package internal

import (
	"net"

	"github.com/jackc/pgproto3"
	"github.com/pg-sharding/spqr/internal/config"
)


type Server interface {
	initConn(sm *pgproto3.StartupMessage) error
	Send(query pgproto3.FrontendMessage) error
	Receive() (pgproto3.BackendMessage, error)
	Cleanup() error
}

type ServerImpl struct {
	rule     *config.BERule
	conn     net.Conn
	frontend *pgproto3.Frontend

	shard Shard
}

func NewServer(rule *config.BERule, conn net.Conn, shard Shard) *ServerImpl {
	return &ServerImpl{
		rule: rule,
		conn: conn,
		shard: shard,
	}
}

func (srv *ServerImpl) initConn(sm *pgproto3.StartupMessage) error {

	var err error
	srv.frontend, err = pgproto3.NewFrontend(pgproto3.NewChunkReader(srv.conn), srv.conn)
	if err != nil {
		return err
	}

	err = srv.frontend.Send(sm)
	if err != nil {
		return err
	}

	for {
		msg, err := srv.frontend.Receive()
		if err != nil {
			return err
		}
		switch v := msg.(type) {
		case *pgproto3.ReadyForQuery:
			return nil

			//!! backend authBackend
		case *pgproto3.Authentication:
			err := authBackend(srv, v)
			if err != nil {
				return err
			}
		}
	}
}

func (srv *ServerImpl) Send(query pgproto3.FrontendMessage) error {
	return srv.frontend.Send(query)
}

func (srv *ServerImpl) Receive() (pgproto3.BackendMessage, error) {
	return srv.frontend.Receive()
}


func (srv *ServerImpl) Cleanup() error {

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
