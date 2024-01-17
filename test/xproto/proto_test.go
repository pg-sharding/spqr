package prep_stmt_test

import (
	"fmt"
	"net"
	"os"
	"testing"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/stretchr/testify/assert"
)

func getC() (net.Conn, error) {
	const proto = "tcp"
	host := os.Getenv("POSTGRES_HOST")
	if host == "" {
		host = "[::1]"
	}
	addr := fmt.Sprintf("%s:6432", host)
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

func getConnectionParams() map[string]string {
	database := os.Getenv("POSTGRES_DB")
	if database == "" {
		database = "db1"
	}
	user := os.Getenv("POSTGRES_USER")
	if user == "" {
		user = "user1"
	}
	password := os.Getenv("POSTGRES_PASSWORD")
	return map[string]string{
		"user":     user,
		"database": database,
		"password": password,
	}
}

func TestPrepStmt(t *testing.T) {
	conn, err := getC()
	if err != nil {
		assert.NoError(t, err, "startup failed")
		return
	}
	defer func() {
		_ = conn.Close()
	}()

	frontend := pgproto3.NewFrontend(conn, conn)
	frontend.Send(&pgproto3.StartupMessage{
		ProtocolVersion: 196608,
		Parameters:      getConnectionParams(),
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
		Response []pgproto3.BackendMessage
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
			Response: []pgproto3.BackendMessage{
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

		{
			Request: []pgproto3.FrontendMessage{
				&pgproto3.Bind{
					PreparedStatement: "stmt1",
				},
				&pgproto3.Execute{},
				&pgproto3.Sync{},
			},
			Response: []pgproto3.BackendMessage{
				&pgproto3.BindComplete{},
				&pgproto3.DataRow{
					Values: [][]byte{
						[]byte("11"),
					},
				},
				&pgproto3.CommandComplete{
					CommandTag: []byte("SELECT 1"),
				},
				&pgproto3.ReadyForQuery{
					TxStatus: 73, /*txidle*/
				},
			},
		},

		{
			Request: []pgproto3.FrontendMessage{
				&pgproto3.Parse{
					Name:  "stmt2",
					Query: "select 22 as test",
				},
				&pgproto3.Describe{
					ObjectType: 'S',
					Name:       "stmt2",
				},
				&pgproto3.Bind{
					PreparedStatement: "stmt2",
				},
				&pgproto3.Execute{},
				&pgproto3.Sync{},
			},
			Response: []pgproto3.BackendMessage{
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
				&pgproto3.BindComplete{},
				&pgproto3.DataRow{
					Values: [][]byte{
						[]byte("22"),
					},
				},
				&pgproto3.CommandComplete{
					CommandTag: []byte("SELECT 1"),
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
		_ = frontend.Flush()
		for _, msg := range msgroup.Response {
			retMsg, err := frontend.Receive()
			assert.NoError(t, err)
			assert.Equal(t, msg, retMsg)
		}
	}
}
