package conn

import (
	"crypto/tls"
	"encoding/binary"
	"fmt"
	"net"

	"github.com/jackc/pgproto3/v2"
	"github.com/wal-g/tracelog"
	"golang.org/x/xerrors"

	"github.com/pg-sharding/spqr/pkg/config"
)

const SSLREQ = 80877103
const CANCELREQ = 80877102

type TXStatus byte

const (
	TXIDLE = TXStatus(73)
	TXERR  = TXStatus(69)
	TXACT  = TXStatus(84)
	TXCONT = TXStatus(1)
)

type InstanceStatus string

const NotInitialized = InstanceStatus("NOT_INITIALIZED")
const ACQUIRED = InstanceStatus("ACQUIRED")

type DBInstance interface {
	Send(query pgproto3.FrontendMessage) error
	Receive() (pgproto3.BackendMessage, error)

	CheckRW() (bool, error)
	ReqBackendSsl(*tls.Config) error

	Hostname() string

	Close() error
	Status() InstanceStatus
	SetStatus(status InstanceStatus)
}

type PostgreSQLInstance struct {
	conn     net.Conn
	frontend *pgproto3.Frontend

	hostname string
	status   InstanceStatus
}

func (pgi *PostgreSQLInstance) SetStatus(status InstanceStatus) {
	pgi.status = status
}

func (pgi *PostgreSQLInstance) Status() InstanceStatus {
	return pgi.status
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

func NewInstanceConn(cfg *config.InstanceCFG, tlsconfig *tls.Config) (DBInstance, error) {
	tracelog.InfoLogger.Printf("initializing new postgresql instance connection to %v", cfg.ConnAddr)

	instance := &PostgreSQLInstance{
		hostname: cfg.ConnAddr,
		status:   NotInitialized,
	}

	netconn, err := instance.connect(cfg.ConnAddr, cfg.Proto)
	if err != nil {
		return nil, err
	}

	instance.conn = netconn

	if tlsconfig != nil {
		err := instance.ReqBackendSsl(tlsconfig)
		if err != nil {
			return nil, err
		}
	}

	instance.frontend = pgproto3.NewFrontend(pgproto3.NewChunkReader(instance.conn), instance.conn)
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
		return false, fmt.Errorf("unexcepted")
	}
}

var _ DBInstance = &PostgreSQLInstance{}

func (pgi *PostgreSQLInstance) ReqBackendSsl(tlsconfig *tls.Config) error {
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, 8)
	// Gen salt
	b = append(b, 0, 0, 0, 0)
	binary.BigEndian.PutUint32(b[4:], SSLREQ)

	_, err := pgi.conn.Write(b)

	if err != nil {
		return fmt.Errorf("ReqBackendSsl: %w", err)
	}

	resp := make([]byte, 1)

	if _, err := pgi.conn.Read(resp); err != nil {
		return err
	}

	sym := resp[0]

	if sym != 'S' {
		return xerrors.New("SSL should be enabled")
	}

	pgi.conn = tls.Client(pgi.conn, tlsconfig)
	return nil
}

func (pgi *PostgreSQLInstance) Cancel() error {
	msg := &pgproto3.CancelRequest{}

	return pgi.frontend.Send(msg)
}
