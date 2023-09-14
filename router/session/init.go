package session

import (
	"bufio"
	"crypto/tls"
	"encoding/binary"
	"fmt"
	"math/rand"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/pg-sharding/spqr/pkg/conn"
	"github.com/pg-sharding/spqr/pkg/spqrlog"
)

type SessionMgr interface {
	SessionInit() error
}

type ProxySessionMgr struct {
}

func (p *ProxySessionMgr) SessionInit(cl RouterSessionClient, tlsconfig *tls.Config) error {
	for {
		var backend *pgproto3.Backend

		var sm *pgproto3.StartupMessage

		headerRaw := make([]byte, 4)

		_, err := cl.Read(headerRaw)
		if err != nil {
			return err
		}

		msgSize := int(binary.BigEndian.Uint32(headerRaw) - 4)
		msg := make([]byte, msgSize)

		_, err = cl.Read(msg)
		if err != nil {
			return err
		}

		protoVer := binary.BigEndian.Uint32(msg)

		spqrlog.Zero.Debug().
			Uint("client", spqrlog.GetPointer(cl)).
			Uint32("proto-version", protoVer).
			Msg("received protocol version")

		switch protoVer {
		case conn.GSSREQ:
			spqrlog.Zero.Debug().Msg("negotiate gss enc request")
			_, err := cl.Write([]byte{'N'})
			if err != nil {
				return err
			}
			// proceed next iter, for protocol version number or GSSAPI interaction
			continue

		case conn.SSLREQ:
			if tlsconfig == nil {
				_, err := cl.Write([]byte{'N'})
				if err != nil {
					return err
				}
				// proceed next iter, for protocol version number or GSSAPI interaction
				continue
			}

			_, err := cl.Write([]byte{'S'})
			if err != nil {
				return err
			}

			// cl.conn = tls.Server(cl, tlsconfig)

			backend = pgproto3.NewBackend(bufio.NewReader(cl), cl)

			frsm, err := backend.ReceiveStartupMessage()

			switch msg := frsm.(type) {
			case *pgproto3.StartupMessage:
				sm = msg
			default:
				return fmt.Errorf("received unexpected message type %T", frsm)
			}

			if err != nil {
				return err
			}
		//
		case pgproto3.ProtocolVersionNumber:
			// reuse
			sm = &pgproto3.StartupMessage{}
			err = sm.Decode(msg)
			if err != nil {
				spqrlog.Zero.Error().Err(err).Msg("")
				return err
			}
			backend = pgproto3.NewBackend(bufio.NewReader(cl), cl)
		case conn.CANCELREQ:
			csm := &pgproto3.CancelRequest{}
			if err = csm.Decode(msg); err != nil {
				return err
			}

			cl.AssingCancelMsg(csm)

			return nil
		default:
			return fmt.Errorf("protocol number %d not supported", protoVer)
		}

		/* setup client params */

		for k, v := range sm.Parameters {
			cl.SetParam(k, v)
		}

		cl.SetStartup(sm)

		cl.AssingBackend(backend)

		cl.AssingCancelKeys(rand.Uint32(), rand.Uint32())

		spqrlog.Zero.Debug().
			Uint("client", spqrlog.GetPointer(cl)).
			Uint32("cancel_key", cl.CancelKey()).
			Uint32("cancel_pid", cl.CancelPid())

		if tlsconfig != nil && protoVer != conn.SSLREQ {
			if err := cl.Send(
				&pgproto3.ErrorResponse{
					Severity: "ERROR",
					Message:  "SSL IS REQUIRED",
				}); err != nil {
				return err
			}
		}

		return nil
	}
}
