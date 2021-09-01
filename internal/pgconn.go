package internal

import (
	"crypto/tls"
	"encoding/binary"
	"net"

	"github.com/jackc/pgproto3"
	"github.com/wal-g/tracelog"
	"golang.org/x/xerrors"
)

type PgConn struct {
	conn     net.Conn
	frontend *pgproto3.Frontend

	shard Shard
}

func (pgconn *PgConn) ReqBackendSsl(tlscfg *tls.Config) error {

	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, 8)
	// Gen salt
	b = append(b, 0, 0, 0, 0)
	binary.BigEndian.PutUint32(b[4:], sslproto)

	_, err := pgconn.conn.Write(b)

	if err != nil {
		return xerrors.Errorf("ReqBackendSsl: %w", err)
	}

	resp := make([]byte, 1)

	if _, err := pgconn.conn.Read(resp); err != nil {
		return err
	}

	sym := resp[0]

	if sym != 'S' {
		return xerrors.New("SSL should be enabled")
	}

	pgconn.conn = tls.Client(pgconn.conn, tlscfg)
	return nil
}

func (pgconn *PgConn) initConn(sm *pgproto3.StartupMessage) error {
	var err error
	pgconn.frontend, err = pgproto3.NewFrontend(pgproto3.NewChunkReader(pgconn.conn), pgconn.conn)
	if err != nil {
		return err
	}

	err = pgconn.frontend.Send(sm)
	if err != nil {
		return err
	}

	for {
		msg, err := pgconn.frontend.Receive()
		if err != nil {
			return err
		}
		switch v := msg.(type) {
		case *pgproto3.ReadyForQuery:
			return nil
		case *pgproto3.Authentication:
			err := authBackend(pgconn, v)
			if err != nil {
				tracelog.InfoLogger.Printf("failed to perform backend auth %w", err)
				return err
			}
		case *pgproto3.ErrorResponse:
			return xerrors.New(v.Message)
		default:
			tracelog.InfoLogger.Printf("unexpected msg type received %T", v)
		}
	}
}
