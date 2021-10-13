package conn

import (
	"crypto/tls"
	"encoding/binary"
	"net"

	"github.com/jackc/pgproto3"
	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/wal-g/tracelog"
	"golang.org/x/xerrors"
)

const SSLPROTO = 80877103

type DBInstance interface {
	Send(query pgproto3.FrontendMessage) error
	Receive() (pgproto3.BackendMessage, error)

	CheckRW() (bool, error)
	ReqBackendSsl(tlscfg *tls.Config) error

	Hostname() string

	Close() error
}

type PostgreSQLInstance struct {
	conn     net.Conn
	frontend *pgproto3.Frontend

	hostname string
}

func (pgi *PostgreSQLInstance) Close() error {
	return pgi.conn.Close()
}

func (pgi *PostgreSQLInstance) Hostname() string {
	return pgi.hostname
}

func (pgi *PostgreSQLInstance) Send(query pgproto3.FrontendMessage) error {
	return pgi.frontend.Send(query)
}

func (pgi *PostgreSQLInstance) Receive() (pgproto3.BackendMessage, error) {
	return pgi.frontend.Receive()
}

const defaultProto = "tcp"

func (pgi *PostgreSQLInstance) connect(addr, proto string) (net.Conn, error) {
	if proto == "" {
		return net.Dial(defaultProto, addr)
	}

	return net.Dial(proto, addr)
}

func NewInstanceConn(cfg *config.InstanceCFG, tlscfg *tls.Config, sslmode string) (DBInstance, error) {

	instance := &PostgreSQLInstance{hostname: cfg.ConnAddr}

	netconn, err := instance.connect(cfg.ConnAddr, cfg.Proto)
	if err != nil {
		return nil, err
	}

	instance.conn = netconn

	if sslmode == config.SSLMODEREQUIRE {
		err := instance.ReqBackendSsl(tlscfg)
		if err != nil {
			return nil, err
		}
	}

	instance.frontend, err = pgproto3.NewFrontend(pgproto3.NewChunkReader(instance.conn), instance.conn)
	if err != nil {
		return nil, err
	}

	return instance, nil
}

func (pgi *PostgreSQLInstance) CheckRW() (bool, error) {

	msg := &pgproto3.Query{
		String: "SELECT pg_is_in_recovery()",
	}

	if err := pgi.frontend.Send(msg); err != nil {
		tracelog.InfoLogger.Printf("got error while checking rw %v", err)
		return false, err
	}

	bmsg, err := pgi.frontend.Receive()

	if err != nil {
		tracelog.InfoLogger.Printf("got error while checking rw %v", err)
		return false, err
	}
	tracelog.InfoLogger.Printf("got reply from %v: %T", pgi.hostname, bmsg)

	switch v := bmsg.(type) {
	case *pgproto3.DataRow:

		tracelog.InfoLogger.Printf("got datarow %v", v.Values)

		if len(v.Values) == 1 && v.Values[0] != nil && v.Values[0][0] == byte('t') {
			return true, nil
		}
		return false, nil

	default:
		return false, xerrors.Errorf("unexcepted")
	}
}

var _ DBInstance = &PostgreSQLInstance{}

func (pgi *PostgreSQLInstance) ReqBackendSsl(tlscfg *tls.Config) error {

	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, 8)
	// Gen salt
	b = append(b, 0, 0, 0, 0)
	binary.BigEndian.PutUint32(b[4:], SSLPROTO)

	_, err := pgi.conn.Write(b)

	if err != nil {
		return xerrors.Errorf("ReqBackendSsl: %w", err)
	}

	resp := make([]byte, 1)

	if _, err := pgi.conn.Read(resp); err != nil {
		return err
	}

	sym := resp[0]

	tracelog.InfoLogger.Printf("recv sym %v", sym)

	if sym != 'S' {
		return xerrors.New("SSL should be enabled")
	}

	pgi.conn = tls.Client(pgi.conn, tlscfg)
	return nil
}
