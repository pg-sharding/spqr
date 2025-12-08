package conn

import (
	"crypto/tls"
	"encoding/binary"
	"fmt"
	"net"
	"os"
	"syscall"
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

	Conn() net.Conn

	Hostname() string
	AvailabilityZone() string
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
	az        string // availability zone
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

// AvailabilityZone returns the availability zone of the PostgreSQLInstance.
//
// Parameters:
// - None.
//
// Returns:
// - string: The availability zone of the PostgreSQLInstance.
func (pgi *PostgreSQLInstance) AvailabilityZone() string {
	return pgi.az
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
	case
		*pgproto3.Sync,
		*pgproto3.Query,
		*pgproto3.StartupMessage,
		*pgproto3.PasswordMessage,
		*pgproto3.SASLResponse, *pgproto3.SASLInitialResponse,
		*pgproto3.GSSResponse, *pgproto3.GSSEncRequest,
		*pgproto3.CopyDone, *pgproto3.CopyData, *pgproto3.CopyFail:
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

func setTCPUserTimeout(d time.Duration) func(string, string, syscall.RawConn) error {
	return func(network, address string, c syscall.RawConn) error {
		var sysErr error
		var err = c.Control(func(fd uintptr) {
			/*
				#define TCP_USER_TIMEOUT	 18 // How long for loss retry before timeout
			*/

			sysErr = syscall.SetsockoptInt(int(fd), syscall.IPPROTO_TCP, 0x12, int(d.Milliseconds()))
		})
		if sysErr != nil {
			return os.NewSyscallError("setsockopt", sysErr)
		}
		return err
	}
}

// NewInstanceConn creates a new instance connection to a PostgreSQL database.
//
// Parameters:
// - host (string): The hostname of the PostgreSQL instance.
// - availabilityZone (string): The availability zone of the PostgreSQL instance.
// - shardname (string): The name of the shard.
// - tlsconfig (*tls.Config): The TLS configuration to use for the SSL/TLS handshake.
// - timeout (time.Duration): The timeout for the connection.
// - keepAlive (time.Duration): The keep alive duration for the connection.
// - tcpUserTimeout (time.Duration): The TCP user timeout duration for the connection.
//
// Returns:
// - DBInstance: The new PostgreSQLInstance.
// - error: An error if there was a problem creating the new PostgreSQLInstance.
func NewInstanceConn(host string, availabilityZone string, shardname string, tlsconfig *tls.Config, timeout time.Duration, keepAlive time.Duration, tcpUserTimeout time.Duration) (DBInstance, error) {
	dd := net.Dialer{
		Timeout:   timeout,
		KeepAlive: keepAlive,

		Control: setTCPUserTimeout(tcpUserTimeout),
	}

	// assuming here host is in the form of hostname:port
	netconn, err := dd.Dial("tcp", host)
	if err != nil {
		return nil, err
	}

	instance := &PostgreSQLInstance{
		hostname:  host,
		az:        availabilityZone,
		shardname: shardname,
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

func (pgi *PostgreSQLInstance) Conn() net.Conn {
	return pgi.conn
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
