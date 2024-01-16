package prep_stmt_test

import (
	"net"
	"testing"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/stretchr/testify/assert"
)

func getC() (net.Conn, error) {
	const proto = "tcp"
	const addr = "[::1]:6432"
	return net.Dial(proto, addr)
}

// nolint
func readCnt(fr *pgproto3.Frontend, count int) error {
	for i := 0; i < count; i++ {
		if _, err := fr.Receive(); err != nil {
			return err
		}
	}

	return nil
}

func waitRFQ(fr *pgproto3.Frontend) error {
	for {
		if msg, err := fr.Receive(); err != nil {
			return err
		} else {
			switch msg.(type) {
			case *pgproto3.ReadyForQuery:
				return nil
			}
		}
	}
}

func TestPrepStmt(t *testing.T) {
	conn, err := getC()
	if err != nil {
		assert.NoError(t, err, "startup failed")
		return
	}
	defer conn.Close()

	frontend := pgproto3.NewFrontend(conn, conn)
	frontend.Send(&pgproto3.StartupMessage{
		ProtocolVersion: 196608,
		Parameters: map[string]string{
			"user":     "user1",
			"database": "db1",
			"password": "12345678",
		},
	})
	if err := frontend.Flush(); err != nil {
		assert.NoError(t, err, "startup failed")
	}

	if err := waitRFQ(frontend); err != nil {
		assert.NoError(t, err, "startup failed")
		return
	}

	type MessageGroup struct {
		Request  []pgproto3.FrontendMessage
		Responce []pgproto3.BackendMessage
	}

	for _, msgroup := range []MessageGroup{
		{
			Request: []pgproto3.FrontendMessage{
				&pgproto3.Parse{
					Name:  "stmt1",
					Query: "select 11 as test",
				},
				&pgproto3.Describe{
					ObjectType: 'S',
					Name:       "stmt1",
				},
				&pgproto3.Sync{},
			},
			Responce: []pgproto3.BackendMessage{
				&pgproto3.ParseComplete{},
				&pgproto3.ParameterDescription{
					ParameterOIDs: []uint32{},
				},
				&pgproto3.RowDescription{
					Fields: []pgproto3.FieldDescription{
						{
							Name:         []byte("test"),
							DataTypeOID:  23,
							DataTypeSize: 4,
							TypeModifier: -1,
						},
					},
				},
				&pgproto3.ReadyForQuery{
					TxStatus: 73, /*txidle*/
				},
			},
		},
	} {
		for _, msg := range msgroup.Request {
			frontend.Send(msg)
		}
		frontend.Flush()
		for _, msg := range msgroup.Responce {
			retMsg, err := frontend.Receive()
			assert.NoError(t, err)
			assert.Equal(t, msg, retMsg)
		}
	}
}
