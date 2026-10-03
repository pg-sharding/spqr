//go:build simple || all

package prep_stmt_test

import (
	"encoding/binary"
	"net"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/stretchr/testify/assert"
)

// malformed 'Q' payloads: missing trailing NUL byte and empty body;
// the router must reject them and keep serving connections
const malformedQueryReadDeadline = 15 * time.Second

func sendRawQuery(conn net.Conn, payload []byte) error {
	msg := make([]byte, 0, 5+len(payload))
	msg = append(msg, 'Q')
	length := make([]byte, 4)
	binary.BigEndian.PutUint32(length, uint32(4+len(payload)))
	msg = append(msg, length...)
	msg = append(msg, payload...)
	_, err := conn.Write(msg)
	return err
}

func drainTillConnClosed(t *testing.T, frontend *pgproto3.Frontend, conn net.Conn) {
	_ = conn.SetReadDeadline(time.Now().Add(malformedQueryReadDeadline))
	defer func() {
		_ = conn.SetReadDeadline(time.Time{})
	}()

	for {
		msg, err := frontend.Receive()
		if err != nil {
			if nerr, ok := err.(net.Error); ok && nerr.Timeout() {
				t.Fatalf("router hung on malformed 'Q' message: %v", err)
			}
			return
		}
		switch msg.(type) {
		case *pgproto3.ErrorResponse:
		default:
		}
	}
}

func routerStillServes(t *testing.T) {
	frontend, conn, err := bootstrapConnection(t)
	assert.NoError(t, err, "router does not accept new connections after malformed 'Q'")
	if err != nil {
		return
	}
	defer func() {
		_ = conn.Close()
	}()

	frontend.Send(&pgproto3.Query{String: "select 1"})
	assert.NoError(t, frontend.Flush())

	for {
		msg, err := frontend.Receive()
		assert.NoError(t, err, "router failed to serve query on a fresh connection")
		if err != nil {
			return
		}
		switch msg.(type) {
		case *pgproto3.ReadyForQuery:
			return
		}
	}
}

func TestSimpleQueryMalformedNulTermination(t *testing.T) {
	for _, tt := range []struct {
		name    string
		payload []byte
	}{
		{
			name:    "query without trailing NUL byte",
			payload: []byte("select 1"),
		},
		{
			name:    "empty query payload",
			payload: []byte{},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			frontend, conn, err := bootstrapConnection(t)
			assert.NoError(t, err)
			if err != nil {
				return
			}
			defer func() {
				_ = conn.Close()
			}()

			assert.NoError(t, sendRawQuery(conn, tt.payload))
			drainTillConnClosed(t, frontend, conn)
		})
	}

	routerStillServes(t)
}
