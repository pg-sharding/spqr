package conn

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
)

// ConnectionError identifies the phase that failed while establishing a backend
// connection. The underlying error remains available through errors.Is/As.
type ConnectionError struct {
	Stage string
	Err   error
}

func (e *ConnectionError) Error() string { return fmt.Sprintf("%s: %v", e.Stage, e.Err) }
func (e *ConnectionError) Unwrap() error { return e.Err }

// NewInstanceConnContext opens a dedicated backend connection. The caller's
// absolute deadline applies to every read and write until Close, including the
// TLS handshake and subsequent authentication. Cancellation closes the socket.
func NewInstanceConnContext(ctx context.Context, host, availabilityZone, shardname string, tlsconfig *tls.Config, keepAlive, tcpUserTimeout time.Duration) (DBInstance, error) {
	dialer := net.Dialer{KeepAlive: keepAlive, Control: setTCPUserTimeout(tcpUserTimeout)}
	netconn, err := dialer.DialContext(ctx, "tcp", host)
	if err != nil {
		return nil, &ConnectionError{Stage: "connect", Err: err}
	}
	return newInstanceWithContext(ctx, netconn, host, availabilityZone, shardname, tlsconfig)
}

func newInstanceWithContext(ctx context.Context, netconn net.Conn, host, availabilityZone, shardname string, tlsconfig *tls.Config) (DBInstance, error) {
	instance := &PostgreSQLInstance{
		hostname: host, az: availabilityZone, shardname: shardname,
		conn: netconn, status: NotInitialized, tlsconfig: tlsconfig,
	}
	instance.stopContext = context.AfterFunc(ctx, func() { _ = netconn.Close() })
	fail := func(stage string, err error) (DBInstance, error) {
		_ = instance.Close()
		if ctx.Err() != nil {
			err = ctx.Err()
		}
		return nil, &ConnectionError{Stage: stage, Err: err}
	}
	if err := ctx.Err(); err != nil {
		return fail("connect", err)
	}
	if deadline, ok := ctx.Deadline(); ok {
		if err := netconn.SetDeadline(deadline); err != nil {
			return fail("connect", err)
		}
	}
	if tlsconfig != nil {
		if err := instance.ReqBackendSsl(tlsconfig); err != nil {
			return fail("tls", err)
		}
		if err := instance.conn.(*tls.Conn).HandshakeContext(ctx); err != nil {
			return fail("tls", err)
		}
	}
	instance.frontend = pgproto3.NewFrontend(instance.conn, instance.conn)
	return instance, nil
}
