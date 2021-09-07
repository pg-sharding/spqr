package conn

import (
	"crypto/tls"
	"encoding/binary"
	"net"

	"github.com/jackc/pgproto3"
	"github.com/pg-sharding/spqr/internal/config"
	"github.com/wal-g/tracelog"
	"golang.org/x/xerrors"
)

const SSLPROTO = 80877103

type PgConn interface {
	Send(query pgproto3.FrontendMessage) error
	Receive() (pgproto3.BackendMessage, error)

	ReqBackendSsl(tlscfg *tls.Config) error
}

type PgConnImpl struct {
	conn     net.Conn
	frontend *pgproto3.Frontend
}

func (pgconn *PgConnImpl) Send(query pgproto3.FrontendMessage) error {
	return pgconn.frontend.Send(query)
}

func (pgconn *PgConnImpl) Receive() (pgproto3.BackendMessage, error) {
	return pgconn.frontend.Receive()
}

func NewPgConn(netconn net.Conn, tlscfg *tls.Config, sslmode string) (PgConn, error) {

	pgconn := &PgConnImpl{
		conn: netconn,
	}

	if sslmode == config.SSLMODEREQUIRE {
		err := pgconn.ReqBackendSsl(tlscfg)
		if err != nil {
			return nil, err
		}
	}

	var err error
	pgconn.frontend, err = pgproto3.NewFrontend(pgproto3.NewChunkReader(pgconn.conn), pgconn.conn)
	if err != nil {
		return nil, err
	}

	return pgconn, nil
}


func (pgconn *PgConnImpl) CheckRW() (bool, error) {

	msg := &pgproto3.Query{
		String: "SELECT pg_is_in_recovery()",
	}

	if err := pgconn.frontend.Send(msg); err != nil {
		return false, err
	}

	bmsg, err := pgconn.frontend.Receive()

	if err != nil {
		return false, err
	}

	switch v := bmsg.(type) {
	case *pgproto3.DataRow:
		if len(v.Values) == 1 && v.Values[0] != nil && v.Values[0][0] == byte('t') {
			return true, nil
		}
		return false, nil

	default:
		return false, xerrors.Errorf("unexcepted")
	}
}

var _ PgConn = &PgConnImpl{

}

func (pgconn *PgConnImpl) ReqBackendSsl(tlscfg *tls.Config) error {

	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, 8)
	// Gen salt
	b = append(b, 0, 0, 0, 0)
	binary.BigEndian.PutUint32(b[4:], SSLPROTO)

	_, err := pgconn.conn.Write(b)

	if err != nil {
		return xerrors.Errorf("ReqBackendSsl: %w", err)
	}

	resp := make([]byte, 1)

	if _, err := pgconn.conn.Read(resp); err != nil {
		return err
	}

	sym := resp[0]

	tracelog.InfoLogger.Printf("recv sym %v", sym)

	if sym != 'S' {
		return xerrors.New("SSL should be enabled")
	}

	pgconn.conn = tls.Client(pgconn.conn, tlscfg)
	return nil
}
