package conn

import (
	"crypto/tls"
	"encoding/binary"
	"fmt"
	"net"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
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

// SetStatus sets the status of the PostgreSQLInstance.
//
// Parameters:
// - status (InstanceStatus): a new status of the instance.
//
// Returns:
// - None.
func (pgi *PostgreSQLInstance) SetStatus(status InstanceStatus) {
	pgi.status = status
}

// SetShardName sets the shard name of the PostgreSQLInstance.
//
// Parameters:
// - name (string): the name of the shard to be set.
//
// Returns:
// - None.
func (pgi *PostgreSQLInstance) SetShardName(name string) {
	pgi.shardname = name
}

// SetFrontend sets the frontend of the PostgreSQLInstance.
//
// Parameters:
// - f (*pgproto3.Frontend): the frontend to be set.
//
// Returns:
// - None.
func (pgi *PostgreSQLInstance) SetFrontend(f *pgproto3.Frontend) {
	pgi.frontend = f
}

// Status returns the status of the PostgreSQLInstance.
//
// Parameters:
// - None.
//
// Returns:
// - InstanceStatus.
func (pgi *PostgreSQLInstance) Status() InstanceStatus {
	return pgi.status
}

// Close closes the connection of the PostgreSQLInstance.
// It returns an error if there was a problem closing the connection.
//
// Parameters:
// - None.
//
// Returns:
// - error: An error if there was a problem closing the connection.
func (pgi *PostgreSQLInstance) Close() error {
	return pgi.conn.Close()
}

// Hostname returns the hostname of the PostgreSQLInstance.
//
// Parameters:
// - None.
//
// Returns:
// - string: The hostname of the PostgreSQLInstance.
func (pgi *PostgreSQLInstance) Hostname() string {
	return pgi.hostname
}

// ShardName returns the shard name of the PostgreSQLInstance.
//
// Parameters:
// - None.
//
// Returns:
// - string: The shard name of the PostgreSQLInstance.
func (pgi *PostgreSQLInstance) ShardName() string {
	return pgi.shardname
}

// Send sends a query using the PostgreSQLInstance's frontend and flushes the connection.
//
// Parameters:
// - query (pgproto3.FrontendMessage): The query to send.
//
// Return:
// - error: An error if the sending or flushing fails.
func (pgi *PostgreSQLInstance) Send(query pgproto3.FrontendMessage) error {
	pgi.frontend.Send(query)

	switch query.(type) {
	case *pgproto3.Sync, *pgproto3.Query, *pgproto3.StartupMessage:
		return pgi.frontend.Flush()
	default:
		return nil
	}
}

// Receive returns a backend message received using the PostgreSQLInstance's frontend.
//
// Parameters:
// - None.
//
// Return:
// - pgproto3.BackendMessage: The received backend message.
// - error: An error if the receiving fails.
func (pgi *PostgreSQLInstance) Receive() (pgproto3.BackendMessage, error) {
	return pgi.frontend.Receive()
}

// NewInstanceConn creates a new instance connection to a PostgreSQL database.
//
// Parameters:
// - host (string): The host of the PostgreSQL database.
// - shard (string): The shard name of the PostgreSQL database.
// - tlsconfig (*tls.Config): The TLS configuration for the connection.
//
// Return:
// - (DBInstance, error): The newly created instance connection and any error that occurred.
func NewInstanceConn(host string, shard string, tlsconfig *tls.Config, timout time.Duration, keepAlive time.Duration) (DBInstance, error) {
	dd := net.Dialer{
		Timeout:   timout,
		KeepAlive: keepAlive,
	}

	netconn, err := dd.Dial("tcp", host)
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

	instance.frontend = pgproto3.NewFrontend(instance.conn, instance.conn)
	return instance, nil
}

// Cancel cancels a PostgreSQL query execution.
//
// Parameters:
// - csm: A pointer to a pgproto3.CancelRequest struct that represents the cancel request.
//
// Returns:
// - error: An error if the cancel request fails, otherwise nil.
func (pgi *PostgreSQLInstance) Cancel(csm *pgproto3.CancelRequest) error {
	pgi.frontend.Send(csm)
	return pgi.frontend.Flush()
}

// Tls returns the TLS configuration of the PostgreSQLInstance.
//
// Parameters:
// - None.
//
// Returns:
// - *tls.Config: The TLS configuration of the PostgreSQLInstance.
func (pgi *PostgreSQLInstance) Tls() *tls.Config {
	return pgi.tlsconfig
}

var _ DBInstance = &PostgreSQLInstance{}

// TODO : unit tests

// ReqBackendSsl requests the backend to enable SSL/TLS communication.
//
// Parameters:
// - tlsconfig (*tls.Config): The TLS configuration to use for the SSL/TLS handshake.
//
// Returns:
// - error: An error if there was a problem requesting the backend to enable SSL/TLS communication.
//
// This function sends a request to the backend to enable SSL/TLS communication. It first writes a 4-byte
// message to the connection, indicating the length of the message and the SSLREQ constant. It then reads
// a single byte response from the connection. If the response is not 'S', indicating that SSL should be
// enabled, an error is returned. Otherwise, the connection is upgraded to use TLS using the provided
// TLS configuration. Finally, a debug log message is printed indicating that the backend connection has
// been initialized with TLS.
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
