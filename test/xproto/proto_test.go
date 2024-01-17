package prep_stmt_test

import (
	"context"
	"encoding/binary"
	"fmt"
	"github.com/jackc/pgx/v5"
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
	port := os.Getenv("POSTGRES_PORT")
	if port == "" {
		port = "6432"
	}
	addr := fmt.Sprintf("%s:%s", host, port)
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
	res := map[string]string{
		"user":     user,
		"database": database,
	}
	if password != "" {
		res["password"] = password
	}
	return res
}

func SetupSharding() {
	host := os.Getenv("POSTGRES_HOST")
	if host == "" {
		host = "[::1]"
	}
	port := os.Getenv("POSTGRES_PORT")
	if port == "" {
		port = "6432"
	}

	dsn := fmt.Sprintf(
		"host=%s user=%s dbname=%s port=%s",
		host,
		"spqr-console",
		"spqr-console",
		port,
	)

	conn, err := pgx.Connect(context.Background(), dsn)
	if err != nil {
		return
	}
	defer func() {
		_ = conn.Close(context.Background())
	}()

	_, err = conn.Exec(context.Background(), "CREATE SHARDING RULE r1 COLUMNS id;")
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "could not setup sharding: %s", err)
	}
	_, err = conn.Exec(context.Background(), "Add KEY RANGE krid1 FROM 1 TO 11 ROUTE TO sh1;")
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "could not setup sharding: %s", err)
	}
	_, err = conn.Exec(context.Background(), "Add KEY RANGE krid2 FROM 11 TO 21 ROUTE TO sh2;")
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "could not setup sharding: %s", err)
	}
}

func CreateTable() {
	host := os.Getenv("POSTGRES_HOST")
	if host == "" {
		host = "[::1]"
	}
	port := os.Getenv("POSTGRES_PORT")
	if port == "" {
		port = "6432"
	}

	params := getConnectionParams()

	dsn := fmt.Sprintf(
		"host=%s user=%s dbname=%s port=%s",
		host,
		params["user"],
		params["database"],
		port,
	)

	if val, ok := params["password"]; ok {
		dsn = dsn + " password=" + val
	}

	conn, err := pgx.Connect(context.Background(), dsn)
	if err != nil {
		panic(fmt.Errorf("failed to connect to database: %s", err))
	}
	defer func() {
		_ = conn.Close(context.Background())
	}()

	_, err = conn.Exec(context.Background(), "CREATE TABLE t (id int)")
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "could not create table: %s", err)
	}
}

func TestMain(m *testing.M) {
	SetupSharding()
	CreateTable()
	code := m.Run()
	os.Exit(code)
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
		{
			Request: []pgproto3.FrontendMessage{
				&pgproto3.Parse{
					Name:  "stmt3",
					Query: "INSERT INTO t (\"id\") values ($1) RETURNING \"id\"",
				},
				&pgproto3.Describe{
					ObjectType: 'S',
					Name:       "stmt3",
				},
				&pgproto3.Bind{
					PreparedStatement:    "stmt3",
					DestinationPortal:    "",
					ParameterFormatCodes: []int16{pgproto3.BinaryFormat},
					Parameters: [][]byte{
						func() []byte {
							res := make([]byte, 4)
							binary.BigEndian.PutUint32(res, 1)
							return res
						}(),
					},
					ResultFormatCodes: []int16{pgproto3.BinaryFormat},
				},
				&pgproto3.Describe{
					ObjectType: 'P',
					Name:       "",
				},
				&pgproto3.Execute{},
				&pgproto3.Sync{},
			},
			Response: []pgproto3.BackendMessage{
				&pgproto3.ParseComplete{},
				&pgproto3.ParameterDescription{
					ParameterOIDs: []uint32{23},
				},
				&pgproto3.RowDescription{
					Fields: []pgproto3.FieldDescription{
						{
							Name:                 []byte("id"),
							DataTypeOID:          23,
							DataTypeSize:         4,
							TypeModifier:         -1,
							TableAttributeNumber: 1,
						},
					},
				},
				&pgproto3.BindComplete{},
				&pgproto3.RowDescription{
					Fields: []pgproto3.FieldDescription{
						{
							TableAttributeNumber: 1,
							Name:                 []byte("id"),
							DataTypeOID:          23,
							DataTypeSize:         4,
							TypeModifier:         -1,
							Format:               1,
						},
					},
				},
				&pgproto3.DataRow{
					Values: [][]byte{
						func() []byte {
							res := make([]byte, 4)
							binary.BigEndian.PutUint32(res, 1)
							return res
						}(),
					},
				},
				&pgproto3.CommandComplete{
					CommandTag: []byte("INSERT 0 1"),
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
			switch retMsgType := retMsg.(type) {
			case *pgproto3.RowDescription:
				for i := range retMsgType.Fields {
					// We don't want to check table OID
					retMsgType.Fields[i].TableOID = 0
				}
			default:
				break
			}
			assert.Equal(t, msg, retMsg)
		}
	}
}
