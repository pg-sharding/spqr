package prep_stmt_test

import (
	"context"
	"encoding/binary"
	"fmt"
	"net"
	"os"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/pg-sharding/spqr/pkg/txstatus"

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
	_, err = conn.Exec(context.Background(), "CREATE KEY RANGE krid1 FROM 1 ROUTE TO sh1;")
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "could not setup sharding: %s", err)
	}
	_, err = conn.Exec(context.Background(), "CREATE KEY RANGE krid2 FROM 11 ROUTE TO sh2;")
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "could not setup sharding: %s", err)
	}
}

func CreateTables() {
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
	_, err = conn.Exec(context.Background(), "CREATE TABLE text_table (id text)")
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "could not create table: %s", err)
	}
}

func TestMain(m *testing.M) {
	SetupSharding()
	CreateTables()
	code := m.Run()
	os.Exit(code)
}

func TestSimpleQuery(t *testing.T) {
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
				&pgproto3.Query{
					String: "BEGIN",
				},

				&pgproto3.Query{
					String: "INSERT INTO t (id) VALUES(1);",
				},

				&pgproto3.Query{
					String: "SELECT * FROM t WHERE id = 1;",
				},
				&pgproto3.Query{
					String: "ROLLBACK",
				},
			},
			Response: []pgproto3.BackendMessage{
				&pgproto3.CommandComplete{
					CommandTag: []byte("BEGIN"),
				},

				&pgproto3.ReadyForQuery{
					TxStatus: byte(txstatus.TXACT),
				},

				&pgproto3.CommandComplete{
					CommandTag: []byte("INSERT 0 1"),
				},

				&pgproto3.ReadyForQuery{
					TxStatus: byte(txstatus.TXACT),
				},

				&pgproto3.RowDescription{
					Fields: []pgproto3.FieldDescription{
						{
							Name:                 []byte("id"),
							DataTypeOID:          23,
							TableAttributeNumber: 1,
							DataTypeSize:         4,
							TypeModifier:         -1,
						},
					},
				},

				&pgproto3.DataRow{
					Values: [][]byte{
						{'1'},
					},
				},

				&pgproto3.CommandComplete{
					CommandTag: []byte("SELECT 1"),
				},

				&pgproto3.ReadyForQuery{
					TxStatus: 84,
				},

				&pgproto3.CommandComplete{
					CommandTag: []byte("ROLLBACK"),
				},

				&pgproto3.ReadyForQuery{
					TxStatus: 73,
				},
			},
		},
	} {
		for _, msg := range msgroup.Request {
			frontend.Send(msg)
		}
		_ = frontend.Flush()
		backendFinished := false
		for _, msg := range msgroup.Response {
			if backendFinished {
				break
			}
			retMsg, err := frontend.Receive()
			assert.NoError(t, err)
			switch retMsgType := retMsg.(type) {
			case *pgproto3.RowDescription:
				for i := range retMsgType.Fields {
					// We don't want to check table OID
					retMsgType.Fields[i].TableOID = 0
				}
			case *pgproto3.ReadyForQuery:
				switch msg.(type) {
				case *pgproto3.ReadyForQuery:
					break
				default:
					backendFinished = true
				}
			default:
				break
			}
			assert.Equal(t, msg, retMsg)
		}
	}
}

func TestSimpleAdvancedParsing(t *testing.T) {
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
				&pgproto3.Query{
					String: "BEGIN",
				},
				&pgproto3.Query{
					String: "SELECT 1 as s",
				},
				&pgproto3.Query{
					String: "COMMIT",
				},
			},
			Response: []pgproto3.BackendMessage{
				&pgproto3.CommandComplete{
					CommandTag: []byte("BEGIN"),
				},

				&pgproto3.ReadyForQuery{
					TxStatus: 84,
				},

				&pgproto3.RowDescription{
					Fields: []pgproto3.FieldDescription{
						{
							Name:         []byte("s"),
							DataTypeOID:  23,
							DataTypeSize: 4,
							TypeModifier: -1,
						},
					},
				},

				&pgproto3.DataRow{
					Values: [][]byte{
						{'1'},
					},
				},

				&pgproto3.CommandComplete{
					CommandTag: []byte("SELECT 1"),
				},

				&pgproto3.ReadyForQuery{
					TxStatus: 84,
				},

				&pgproto3.CommandComplete{
					CommandTag: []byte("COMMIT"),
				},

				&pgproto3.ReadyForQuery{
					TxStatus: 73,
				},
			},
		},
	} {
		for _, msg := range msgroup.Request {
			frontend.Send(msg)
		}
		_ = frontend.Flush()
		backendFinished := false
		for _, msg := range msgroup.Response {
			if backendFinished {
				break
			}
			retMsg, err := frontend.Receive()
			assert.NoError(t, err)
			switch retMsgType := retMsg.(type) {
			case *pgproto3.RowDescription:
				for i := range retMsgType.Fields {
					// We don't want to check table OID
					retMsgType.Fields[i].TableOID = 0
				}
			case *pgproto3.ReadyForQuery:
				switch msg.(type) {
				case *pgproto3.ReadyForQuery:
					break
				default:
					backendFinished = true
				}
			default:
				break
			}
			assert.Equal(t, msg, retMsg)
		}
	}
}

func TestSimpleAdvancedSETParsing(t *testing.T) {
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
				&pgproto3.Query{
					String: "BEGIN",
				},

				&pgproto3.Query{
					String: "SELECT 3 as kek",
				},

				&pgproto3.Query{
					String: "SET search_path to 'lol'",
				},

				&pgproto3.Query{
					String: "SHOW search_path",
				},

				&pgproto3.Query{
					String: "COMMIT",
				},

				&pgproto3.Query{
					String: "SHOW search_path",
				},
			},
			Response: []pgproto3.BackendMessage{
				&pgproto3.CommandComplete{
					CommandTag: []byte("BEGIN"),
				},

				&pgproto3.ReadyForQuery{
					TxStatus: byte(txstatus.TXACT),
				},

				// select response
				&pgproto3.RowDescription{
					Fields: []pgproto3.FieldDescription{
						{
							Name:         []byte("kek"),
							DataTypeOID:  23,
							DataTypeSize: 4,
							TypeModifier: -1,
						},
					},
				},

				&pgproto3.DataRow{
					Values: [][]byte{
						{'3'},
					},
				},

				&pgproto3.CommandComplete{
					CommandTag: []byte("SELECT 1"),
				},

				&pgproto3.ReadyForQuery{
					TxStatus: byte(txstatus.TXACT),
				},

				// set
				&pgproto3.CommandComplete{
					CommandTag: []byte("SET"),
				},

				&pgproto3.ReadyForQuery{
					TxStatus: byte(txstatus.TXACT),
				},

				// show response
				&pgproto3.RowDescription{
					Fields: []pgproto3.FieldDescription{
						{
							Name:         []byte("search_path"),
							DataTypeOID:  25,
							DataTypeSize: -1,
							TypeModifier: -1,
						},
					},
				},

				&pgproto3.DataRow{
					Values: [][]byte{
						{'l', 'o', 'l'},
					},
				},

				&pgproto3.CommandComplete{
					CommandTag: []byte("SHOW"),
				},

				&pgproto3.ReadyForQuery{
					TxStatus: 84,
				},

				&pgproto3.CommandComplete{
					CommandTag: []byte("COMMIT"),
				},

				&pgproto3.ReadyForQuery{
					TxStatus: 73,
				},

				// show response after commit (unrouted)
				&pgproto3.RowDescription{
					Fields: []pgproto3.FieldDescription{
						{
							Name:         []byte("search_path"),
							DataTypeOID:  25,
							DataTypeSize: -1,
							TypeModifier: -1,
						},
					},
				},

				&pgproto3.DataRow{
					Values: [][]byte{
						{'l', 'o', 'l'},
					},
				},

				&pgproto3.CommandComplete{
					CommandTag: []byte("SHOW"),
				},

				&pgproto3.ReadyForQuery{
					TxStatus: byte(txstatus.TXIDLE),
				},
			},
		},
	} {
		for _, msg := range msgroup.Request {
			frontend.Send(msg)
		}
		_ = frontend.Flush()
		backendFinished := false
		for ind, msg := range msgroup.Response {
			if backendFinished {
				break
			}
			retMsg, err := frontend.Receive()
			assert.NoError(t, err)
			switch retMsgType := retMsg.(type) {
			case *pgproto3.RowDescription:
				for i := range retMsgType.Fields {
					// We don't want to check table OID
					retMsgType.Fields[i].TableOID = 0
				}
			case *pgproto3.ReadyForQuery:
				switch msg.(type) {
				case *pgproto3.ReadyForQuery:
					break
				default:
					backendFinished = true
				}
			default:
				break
			}
			assert.Equal(t, msg, retMsg, fmt.Sprintf("fail on index %d", ind))
		}
	}
}

func TestPrepStmtSimple(t *testing.T) {
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
					Name:  "stmtcache_1",
					Query: "select 'Hello, world!';",
				},
				&pgproto3.Describe{
					Name:       "stmtcache_1",
					ObjectType: 'S',
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
							Name:                 []byte("?column?"),
							TableOID:             0,
							TableAttributeNumber: 0,
							DataTypeOID:          25, /* textoid */
							DataTypeSize:         -1,
							TypeModifier:         -1,
							Format:               0,
						},
					},
				},
				&pgproto3.ReadyForQuery{
					TxStatus: byte(txstatus.TXIDLE),
				},
			},
		},
	} {
		for _, msg := range msgroup.Request {
			frontend.Send(msg)
		}
		_ = frontend.Flush()
		backendFinished := false
		for _, msg := range msgroup.Response {
			if backendFinished {
				break
			}
			retMsg, err := frontend.Receive()
			assert.NoError(t, err)
			switch retMsgType := retMsg.(type) {
			case *pgproto3.RowDescription:
				for i := range retMsgType.Fields {
					// We don't want to check table OID
					retMsgType.Fields[i].TableOID = 0
				}
			case *pgproto3.ReadyForQuery:
				switch msg.(type) {
				case *pgproto3.ReadyForQuery:
					break
				default:
					backendFinished = true
				}
			default:
				break
			}
			assert.Equal(t, msg, retMsg)
		}
	}
}

func TestPrepStmtAdvadsedParsing(t *testing.T) {
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
					Query: "BEGIN;",
				},
				&pgproto3.Sync{},
				&pgproto3.Parse{
					Name:  "stmt2",
					Query: "SELECT 1;",
				},
				&pgproto3.Sync{},

				&pgproto3.Parse{
					Name:  "stmt3",
					Query: "ROLLBACK;",
				},
				&pgproto3.Sync{},

				&pgproto3.Bind{
					PreparedStatement: "stmt1",
				},
				&pgproto3.Execute{},

				&pgproto3.Bind{
					PreparedStatement: "stmt2",
				},
				&pgproto3.Execute{},

				&pgproto3.Bind{
					PreparedStatement: "stmt3",
				},
				&pgproto3.Execute{},

				&pgproto3.Sync{},
			},
			Response: []pgproto3.BackendMessage{
				&pgproto3.ParseComplete{},

				&pgproto3.ReadyForQuery{
					TxStatus: byte(txstatus.TXIDLE),
				},

				&pgproto3.ParseComplete{},
				&pgproto3.ReadyForQuery{
					TxStatus: byte(txstatus.TXIDLE),
				},

				&pgproto3.ParseComplete{},
				&pgproto3.ReadyForQuery{
					TxStatus: byte(txstatus.TXIDLE),
				},

				&pgproto3.BindComplete{},
				&pgproto3.CommandComplete{
					CommandTag: []byte("BEGIN"),
				},

				&pgproto3.BindComplete{},
				&pgproto3.DataRow{
					Values: [][]byte{
						{'1'},
					},
				},

				&pgproto3.CommandComplete{
					CommandTag: []byte("SELECT 1"),
				},

				&pgproto3.BindComplete{},
				&pgproto3.CommandComplete{
					CommandTag: []byte("ROLLBACK"),
				},

				&pgproto3.ReadyForQuery{
					TxStatus: byte(txstatus.TXIDLE),
				},
			},
		},

		{
			Request: []pgproto3.FrontendMessage{

				&pgproto3.Parse{
					Name:  "stmt1-0",
					Query: "set session characteristics as transaction read only;",
				},
				&pgproto3.Parse{
					Name:  "stmt1-1",
					Query: "BEGIN;",
				},
				&pgproto3.Sync{},
				&pgproto3.Parse{
					Name:  "stmt1-2",
					Query: "SELECT 1;",
				},
				&pgproto3.Sync{},

				&pgproto3.Parse{
					Name:  "stmt1-3",
					Query: "ROLLBACK;",
				},
				&pgproto3.Sync{},

				&pgproto3.Bind{
					PreparedStatement: "stmt1-0",
				},
				&pgproto3.Execute{},

				&pgproto3.Bind{
					PreparedStatement: "stmt1-1",
				},
				&pgproto3.Execute{},

				&pgproto3.Bind{
					PreparedStatement: "stmt1-2",
				},
				&pgproto3.Execute{},

				&pgproto3.Bind{
					PreparedStatement: "stmt1-3",
				},
				&pgproto3.Execute{},

				&pgproto3.Sync{},
			},
			Response: []pgproto3.BackendMessage{
				&pgproto3.ParseComplete{},

				&pgproto3.ParseComplete{},

				&pgproto3.ReadyForQuery{
					TxStatus: byte(txstatus.TXIDLE),
				},

				&pgproto3.ParseComplete{},
				&pgproto3.ReadyForQuery{
					TxStatus: byte(txstatus.TXIDLE),
				},

				&pgproto3.ParseComplete{},
				&pgproto3.ReadyForQuery{
					TxStatus: byte(txstatus.TXIDLE),
				},

				&pgproto3.BindComplete{},
				&pgproto3.CommandComplete{
					CommandTag: []byte("SET"),
				},

				&pgproto3.BindComplete{},
				&pgproto3.CommandComplete{
					CommandTag: []byte("BEGIN"),
				},

				&pgproto3.BindComplete{},
				&pgproto3.DataRow{
					Values: [][]byte{
						{'1'},
					},
				},

				&pgproto3.CommandComplete{
					CommandTag: []byte("SELECT 1"),
				},

				&pgproto3.BindComplete{},
				&pgproto3.CommandComplete{
					CommandTag: []byte("ROLLBACK"),
				},

				&pgproto3.ReadyForQuery{
					TxStatus: byte(txstatus.TXIDLE),
				},
			},
		},
	} {
		for _, msg := range msgroup.Request {
			frontend.Send(msg)
		}
		_ = frontend.Flush()
		backendFinished := false
		for ind, msg := range msgroup.Response {
			if backendFinished {
				break
			}
			retMsg, err := frontend.Receive()
			assert.NoError(t, err)
			switch retMsgType := retMsg.(type) {
			case *pgproto3.RowDescription:
				for i := range retMsgType.Fields {
					// We don't want to check table OID
					retMsgType.Fields[i].TableOID = 0
				}
			case *pgproto3.ReadyForQuery:
				switch msg.(type) {
				case *pgproto3.ReadyForQuery:
					break
				default:
					backendFinished = true
				}
			default:
				break
			}
			assert.Equal(t, msg, retMsg, fmt.Sprintf("index %d: %s", ind, retMsg))
		}
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
		{
			Request: []pgproto3.FrontendMessage{
				&pgproto3.Parse{
					Name:  "stmt4",
					Query: "INSERT INTO text_table (\"id\") values ($1) RETURNING \"id\"",
				},
				&pgproto3.Describe{
					ObjectType: 'S',
					Name:       "stmt4",
				},
				&pgproto3.Bind{
					PreparedStatement:    "stmt4",
					DestinationPortal:    "",
					ParameterFormatCodes: []int16{pgproto3.TextFormat},
					Parameters: [][]byte{
						[]byte("1"),
					},
					ResultFormatCodes: []int16{pgproto3.TextFormat},
				},
				&pgproto3.Execute{},
				&pgproto3.Sync{},
			},
			Response: []pgproto3.BackendMessage{
				&pgproto3.ParseComplete{},
				&pgproto3.ParameterDescription{
					ParameterOIDs: []uint32{25},
				},
				&pgproto3.RowDescription{
					Fields: []pgproto3.FieldDescription{
						{
							Name:                 []byte("id"),
							DataTypeOID:          25,
							DataTypeSize:         -1,
							TypeModifier:         -1,
							TableAttributeNumber: 1,
						},
					},
				},
				&pgproto3.BindComplete{},
				&pgproto3.DataRow{
					Values: [][]byte{
						[]byte("1"),
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

		{
			Request: []pgproto3.FrontendMessage{
				&pgproto3.Parse{
					Name:  "pstmt1",
					Query: "SELECT set_config($1, $2, $3)",
				},
				&pgproto3.Sync{},
				&pgproto3.Describe{
					ObjectType: 'S',
					Name:       "pstmt1",
				},
				&pgproto3.Sync{},
				&pgproto3.Bind{
					PreparedStatement: "pstmt1",
					Parameters: [][]byte{
						[]byte("log_statement_stats"),
						[]byte("off"),
						[]byte("false"),
					},
				},
				&pgproto3.Describe{ObjectType: 'P'},
				&pgproto3.Execute{},
				&pgproto3.Sync{},

				&pgproto3.Parse{
					Name:  "pstmt2",
					Query: "SELECT pg_is_in_recovery(), current_setting('transaction_read_only')::bool",
				},
				&pgproto3.Sync{},
				&pgproto3.Describe{
					ObjectType: 'S',
					Name:       "pstmt2",
				},
				&pgproto3.Sync{},
				&pgproto3.Bind{
					PreparedStatement: "pstmt2",
				},
				&pgproto3.Describe{ObjectType: 'P'},
				&pgproto3.Execute{},
				&pgproto3.Sync{},
				&pgproto3.Bind{
					PreparedStatement: "pstmt1",
					Parameters: [][]byte{
						[]byte("statement_timeout"),
						[]byte("19"),
						[]byte("false"),
					},
				},
				&pgproto3.Describe{ObjectType: 'P'},
				&pgproto3.Execute{},
				&pgproto3.Sync{},
			},
			Response: []pgproto3.BackendMessage{
				&pgproto3.ParseComplete{},
				&pgproto3.ReadyForQuery{
					TxStatus: 73,
				},
				&pgproto3.ParameterDescription{
					ParameterOIDs: []uint32{25, 25, 16},
				},
				&pgproto3.RowDescription{
					Fields: []pgproto3.FieldDescription{
						{
							Name:         []byte("set_config"),
							DataTypeOID:  25,
							TypeModifier: -1,
							DataTypeSize: -1,
						},
					},
				},
				&pgproto3.ReadyForQuery{
					TxStatus: 73,
				},

				&pgproto3.BindComplete{},
				&pgproto3.RowDescription{
					Fields: []pgproto3.FieldDescription{
						{
							Name:         []byte("set_config"),
							DataTypeOID:  25,
							TypeModifier: -1,
							DataTypeSize: -1,
						},
					},
				},

				&pgproto3.DataRow{Values: [][]byte{
					[]byte("off"),
				}},

				&pgproto3.CommandComplete{CommandTag: []byte("SELECT 1")},

				&pgproto3.ReadyForQuery{
					TxStatus: 73,
				},

				/* select pg in recovery */
				&pgproto3.ParseComplete{},
				&pgproto3.ReadyForQuery{
					TxStatus: 73,
				},
				&pgproto3.ParameterDescription{
					ParameterOIDs: []uint32{},
				},
				&pgproto3.RowDescription{
					Fields: []pgproto3.FieldDescription{
						{
							Name:         []byte("pg_is_in_recovery"),
							DataTypeOID:  16,
							TypeModifier: -1,
							DataTypeSize: 1,
						},
						{
							Name:         []byte("current_setting"),
							DataTypeOID:  16,
							TypeModifier: -1,
							DataTypeSize: 1,
						},
					},
				},
				&pgproto3.ReadyForQuery{
					TxStatus: 73,
				},
				&pgproto3.BindComplete{},
				&pgproto3.RowDescription{
					Fields: []pgproto3.FieldDescription{
						{
							Name:         []byte("pg_is_in_recovery"),
							DataTypeOID:  16,
							TypeModifier: -1,
							DataTypeSize: 1,
						},
						{
							Name:         []byte("current_setting"),
							DataTypeOID:  16,
							TypeModifier: -1,
							DataTypeSize: 1,
						},
					},
				},

				&pgproto3.DataRow{Values: [][]byte{
					[]byte("f"),
					[]byte("f"),
				}},
				&pgproto3.CommandComplete{CommandTag: []byte("SELECT 1")},
				&pgproto3.ReadyForQuery{
					TxStatus: 73,
				},

				/* execute again */
				&pgproto3.BindComplete{},
				&pgproto3.RowDescription{
					Fields: []pgproto3.FieldDescription{
						{
							Name:         []byte("set_config"),
							DataTypeOID:  25,
							TypeModifier: -1,
							DataTypeSize: -1,
						},
					},
				},

				&pgproto3.DataRow{Values: [][]byte{
					[]byte("19ms"),
				}},
				&pgproto3.CommandComplete{CommandTag: []byte("SELECT 1")},
				&pgproto3.ReadyForQuery{
					TxStatus: 73,
				},
			},
		},
	} {
		for _, msg := range msgroup.Request {
			frontend.Send(msg)
		}
		_ = frontend.Flush()
		backendFinished := false
		for ind, msg := range msgroup.Response {
			if backendFinished {
				break
			}
			retMsg, err := frontend.Receive()
			assert.NoError(t, err)
			switch retMsgType := retMsg.(type) {
			case *pgproto3.RowDescription:
				for i := range retMsgType.Fields {
					// We don't want to check table OID
					retMsgType.Fields[i].TableOID = 0
				}
			case *pgproto3.ReadyForQuery:
				switch msg.(type) {
				case *pgproto3.ReadyForQuery:
					break
				default:
					backendFinished = true
				}
			default:
				break
			}
			assert.Equal(t, msg, retMsg, fmt.Sprintf("failed msg no %d", ind))
		}
	}
}
