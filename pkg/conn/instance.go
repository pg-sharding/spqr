package conn

import (
	"crypto/tls"
	"encoding/binary"
	"fmt"
	"net"

	"github.com/jackc/pgproto3/v2"
	"github.com/pg-sharding/spqr/pkg/spqrlog"
)

const CANCELREQ = 80877102
const SSLREQ = 80877103
const GSSREQ = 80877104

type InstanceStatus string

const NotInitialized = InstanceStatus("NOT_INITIALIZED")
const ACQUIRED = InstanceStatus("ACQUIRED")

type DBInstance interface {
	Send(query pgproto3.FrontendMessage) error
	Receive() (pgproto3.BackendMessage, error)

	ReqBackendSsl(*tls.Config) error

	Hostname() string
	ShardName() string

	Close() error
	Status() InstanceStatus
	SetStatus(status InstanceStatus)

	Cancel(csm *pgproto3.CancelRequest) error

	Tls() *tls.Config
}

type PostgreSQLInstance struct {
	conn     net.Conn
	frontend *pgproto3.Frontend

	hostname  string
	shardname string
	status    InstanceStatus

	tlsconfig *tls.Config
}

func (pgi *PostgreSQLInstance) SetStatus(status InstanceStatus) {
	pgi.status = status
}

func (pgi *PostgreSQLInstance) SetShardName(name string) {
	pgi.shardname = name
}

func (pgi *PostgreSQLInstance) SetFrontend(f *pgproto3.Frontend) {
	pgi.frontend = f
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

func (pgi *PostgreSQLInstance) ShardName() string {
	return pgi.shardname
}

func (pgi *PostgreSQLInstance) Send(query pgproto3.FrontendMessage) error {
	return pgi.frontend.Send(query)
}

func (pgi *PostgreSQLInstance) Receive() (pgproto3.BackendMessage, error) {
	return pgi.frontend.Receive()
}

func NewInstanceConn(host string, shard string, tlsconfig *tls.Config) (DBInstance, error) {
	netconn, err := net.Dial("tcp", host)
	if err != nil {
		return nil, err
	}

	instance := &PostgreSQLInstance{
		hostname:  host,
		shardname: shard,
		conn:      netconn,
		status:    NotInitialized,
		tlsconfig: tlsconfig,
	}

	if tlsconfig != nil {
		err := instance.ReqBackendSsl(tlsconfig)
		if err != nil {
			return nil, err
		}
	}

	spqrlog.Zero.Info().
		Str("host", host).
		Bool("ssl", tlsconfig != nil).
		Msg("instance acquire new connection")

	instance.frontend = pgproto3.NewFrontend(pgproto3.NewChunkReader(instance.conn), instance.conn)
	return instance, nil
}

func (pgi *PostgreSQLInstance) Cancel(csm *pgproto3.CancelRequest) error {
	return pgi.frontend.Send(csm)
}

func (pgi *PostgreSQLInstance) Tls() *tls.Config {
	return pgi.tlsconfig
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
		return fmt.Errorf("SSL should be enabled")
	}

	pgi.conn = tls.Client(pgi.conn, tlsconfig)
	spqrlog.Zero.Debug().
		Uint("instance", spqrlog.GetPointer(pgi)).
		Msg("initaited backend connection with TLS")
	return nil
}
