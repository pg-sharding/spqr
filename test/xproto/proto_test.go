package prep_stmt_test

import (
	"context"
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

	_, err = conn.Exec(context.Background(), "CREATE DISTRIBUTION ds1 COLUMN TYPES integer;")
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "could not setup sharding: %s\n", err)
	}
	_, err = conn.Exec(context.Background(), "CREATE SHARDING RULE r1 COLUMNS id FOR DISTRIBUTION ds1;")
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "could not setup sharding: %s\n", err)
	}
	_, err = conn.Exec(context.Background(), "CREATE KEY RANGE krid1 FROM 1 ROUTE TO sh1 FOR DISTRIBUTION ds1;")
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "could not setup sharding: %s\n", err)
	}
	_, err = conn.Exec(context.Background(), "CREATE KEY RANGE krid2 FROM 11 ROUTE TO sh2 FOR DISTRIBUTION ds1;")
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "could not setup sharding: %s\n", err)
	}
	_, err = conn.Exec(context.Background(), "ALTER DISTRIBUTION ds1 ATTACH RELATION t DISTRIBUTION KEY id;")
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "could not setup sharding: %s\n", err)
	}
	_, err = conn.Exec(context.Background(), "CREATE DISTRIBUTION ds2 COLUMN TYPES varchar;")
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "could not setup sharding: %s\n", err)
	}
	_, err = conn.Exec(context.Background(), "CREATE SHARDING RULE r2 COLUMNS id FOR DISTRIBUTION ds2;")
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "could not setup sharding: %s\n", err)
	}
	_, err = conn.Exec(context.Background(), "CREATE KEY RANGE krid3 FROM 1 ROUTE TO sh1 FOR DISTRIBUTION ds2;")
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "could not setup sharding: %s\n", err)
	}
	_, err = conn.Exec(context.Background(), "CREATE KEY RANGE krid4 FROM 11 ROUTE TO sh2 FOR DISTRIBUTION ds2;")
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "could not setup sharding: %s\n", err)
	}
	_, err = conn.Exec(context.Background(), "ALTER DISTRIBUTION ds2 ATTACH RELATION text_table DISTRIBUTION KEY id;")
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "could not setup sharding: %s\n", err)
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
		_, _ = fmt.Fprintf(os.Stderr, "could not create table: %s\n", err)
	}
	_, err = conn.Exec(context.Background(), "CREATE TABLE text_table (id text)")
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "could not create table: %s\n", err)
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
					String: "ROLLBACK",
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
					CommandTag: []byte("ROLLBACK"),
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
						[]byte(`"$user", public`),
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

func TestPrepStmtSimpleParametrizedQuery(t *testing.T) {
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
					Name:  "stmtcache_sr_1",
					Query: "BEGIN",
				},
				&pgproto3.Describe{
					Name:       "stmtcache_sr_1",
					ObjectType: 'S',
				},
				&pgproto3.Sync{},
				&pgproto3.Bind{
					PreparedStatement: "stmtcache_sr_1",
				},
				&pgproto3.Execute{},
				&pgproto3.Sync{},

				&pgproto3.Parse{
					Name:  "stmtcache_sr_2",
					Query: "INSERT INTO t (id) VALUES($1);",
				},
				&pgproto3.Describe{
					Name:       "stmtcache_sr_2",
					ObjectType: 'S',
				},
				&pgproto3.Sync{},
				&pgproto3.Bind{
					PreparedStatement: "stmtcache_sr_2",
					Parameters: [][]byte{
						[]byte("1"),
					},
					ParameterFormatCodes: []int16{0},
				},
				&pgproto3.Execute{},
				&pgproto3.Sync{},

				&pgproto3.Query{
					String: `SELECT * FROM t`,
				},

				&pgproto3.Parse{
					Name:  "stmtcache_sr_3",
					Query: "ROLLBACK",
				},
				&pgproto3.Describe{
					Name:       "stmtcache_sr_3",
					ObjectType: 'S',
				},
				&pgproto3.Sync{},
				&pgproto3.Bind{
					PreparedStatement: "stmtcache_sr_3",
				},
				&pgproto3.Execute{},
				&pgproto3.Sync{},
			},
			Response: []pgproto3.BackendMessage{
				&pgproto3.ParseComplete{},

				&pgproto3.ParameterDescription{
					ParameterOIDs: []uint32{},
				},

				&pgproto3.NoData{},

				&pgproto3.ReadyForQuery{
					TxStatus: byte(txstatus.TXIDLE),
				},

				&pgproto3.BindComplete{},

				&pgproto3.CommandComplete{
					CommandTag: []byte("BEGIN"),
				},

				&pgproto3.ReadyForQuery{
					TxStatus: byte(txstatus.TXACT),
				},

				&pgproto3.ParseComplete{},

				&pgproto3.ParameterDescription{
					ParameterOIDs: []uint32{
						23,
					},
				},

				&pgproto3.NoData{},

				&pgproto3.ReadyForQuery{
					TxStatus: byte(txstatus.TXACT),
				},
				&pgproto3.BindComplete{},
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
							DataTypeSize:         4,
							TypeModifier:         -1,
							TableAttributeNumber: 1,
						},
					},
				},

				&pgproto3.DataRow{
					Values: [][]byte{
						[]byte("1"),
					},
				},

				&pgproto3.CommandComplete{
					CommandTag: []byte("SELECT 1"),
				},

				&pgproto3.ReadyForQuery{
					TxStatus: byte(txstatus.TXACT),
				},

				&pgproto3.ParseComplete{},
				&pgproto3.ParameterDescription{
					ParameterOIDs: []uint32{},
				},

				&pgproto3.NoData{},

				&pgproto3.ReadyForQuery{
					TxStatus: byte(txstatus.TXACT),
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
			assert.Equal(t, msg, retMsg, fmt.Sprintf("index=%d", ind))
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

func TestPrepStmtDescribeAndBind(t *testing.T) {
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

	for i, msgroup := range []MessageGroup{
		{
			Request: []pgproto3.FrontendMessage{
				&pgproto3.Parse{
					Name:  "stmtcache_dab_1",
					Query: "select 'Hello, world!';",
				},
				&pgproto3.Describe{
					Name:       "stmtcache_dab_1",
					ObjectType: 'S',
				},
				&pgproto3.Sync{},
				&pgproto3.Bind{
					PreparedStatement: "stmtcache_dab_1",
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

				&pgproto3.BindComplete{},
				&pgproto3.DataRow{
					Values: [][]byte{
						[]byte("Hello, world!"),
					},
				},

				&pgproto3.CommandComplete{
					CommandTag: []byte("SELECT 1"),
				},

				&pgproto3.ReadyForQuery{
					TxStatus: byte(txstatus.TXIDLE),
				},
			},
		},

		{
			Request: []pgproto3.FrontendMessage{
				&pgproto3.Parse{
					Name:  "stmtcache_dab_2_1",
					Query: "BEGIN",
				},
				&pgproto3.Describe{
					Name:       "stmtcache_dab_2_1",
					ObjectType: 'S',
				},
				&pgproto3.Sync{},

				&pgproto3.Parse{
					Name:  "stmtcache_dab_2_2",
					Query: "ROLLBACK",
				},
				&pgproto3.Describe{
					Name:       "stmtcache_dab_2_2",
					ObjectType: 'S',
				},
				&pgproto3.Sync{},

				&pgproto3.Parse{
					Name:  "stmtcache_dab_2_3",
					Query: "SELECT * FROM t WHERE id = 1",
				},
				&pgproto3.Describe{
					Name:       "stmtcache_dab_2_3",
					ObjectType: 'S',
				},
				&pgproto3.Sync{},

				&pgproto3.Parse{
					Name:  "stmtcache_dab_2_4",
					Query: "INSERT INTO t (id) values (1)",
				},
				&pgproto3.Describe{
					Name:       "stmtcache_dab_2_4",
					ObjectType: 'S',
				},
				&pgproto3.Sync{},

				&pgproto3.Bind{
					PreparedStatement: "stmtcache_dab_2_1",
				},
				&pgproto3.Execute{},
				&pgproto3.Sync{},

				&pgproto3.Bind{
					PreparedStatement: "stmtcache_dab_2_4",
				},
				&pgproto3.Execute{},
				&pgproto3.Sync{},

				&pgproto3.Bind{
					PreparedStatement: "stmtcache_dab_2_3",
				},
				&pgproto3.Execute{},
				&pgproto3.Sync{},

				&pgproto3.Bind{
					PreparedStatement: "stmtcache_dab_2_2",
				},
				&pgproto3.Execute{},
				&pgproto3.Sync{},
			},
			Response: []pgproto3.BackendMessage{
				&pgproto3.ParseComplete{},

				&pgproto3.ParameterDescription{
					ParameterOIDs: []uint32{},
				},
				&pgproto3.NoData{},
				&pgproto3.ReadyForQuery{
					TxStatus: byte(txstatus.TXIDLE),
				},

				&pgproto3.ParseComplete{},

				&pgproto3.ParameterDescription{
					ParameterOIDs: []uint32{},
				},
				&pgproto3.NoData{},

				&pgproto3.ReadyForQuery{
					TxStatus: byte(txstatus.TXIDLE),
				},

				&pgproto3.ParseComplete{},

				&pgproto3.ParameterDescription{
					ParameterOIDs: []uint32{},
				},
				&pgproto3.RowDescription{
					Fields: []pgproto3.FieldDescription{
						{
							Name:                 []byte("id"),
							TableOID:             0,
							TableAttributeNumber: 1,
							DataTypeOID:          23, /*  */
							DataTypeSize:         4,
							TypeModifier:         -1,
							Format:               0,
						},
					},
				},
				&pgproto3.ReadyForQuery{
					TxStatus: byte(txstatus.TXIDLE),
				},

				&pgproto3.ParseComplete{},

				&pgproto3.ParameterDescription{
					ParameterOIDs: []uint32{},
				},
				&pgproto3.NoData{},
				&pgproto3.ReadyForQuery{
					TxStatus: byte(txstatus.TXIDLE),
				},

				&pgproto3.BindComplete{},
				&pgproto3.CommandComplete{
					CommandTag: []byte("BEGIN"),
				},
				&pgproto3.ReadyForQuery{
					TxStatus: byte(txstatus.TXACT),
				},
				&pgproto3.BindComplete{},
				&pgproto3.CommandComplete{
					CommandTag: []byte("INSERT 0 1"),
				},
				&pgproto3.ReadyForQuery{
					TxStatus: byte(txstatus.TXACT),
				},
				&pgproto3.BindComplete{},
				&pgproto3.DataRow{
					Values: [][]byte{
						[]byte("1"),
					},
				},
				&pgproto3.CommandComplete{
					CommandTag: []byte("SELECT 1"),
				},
				&pgproto3.ReadyForQuery{
					TxStatus: byte(txstatus.TXACT),
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
			assert.Equal(t, msg, retMsg, fmt.Sprintf("gr %d tc %d", i, ind))
		}
	}
}

func TestPrepStmtDescribePortalAndBind(t *testing.T) {
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
					Query: "SHOW transaction_read_only",
				},
				&pgproto3.Bind{},
				&pgproto3.Describe{
					ObjectType: 'P',
				},
				&pgproto3.Execute{},
				&pgproto3.Sync{},
			},
			Response: []pgproto3.BackendMessage{
				&pgproto3.ParseComplete{},
				&pgproto3.BindComplete{},
				&pgproto3.RowDescription{
					Fields: []pgproto3.FieldDescription{
						{
							Name:         []byte("transaction_read_only"),
							DataTypeOID:  25,
							DataTypeSize: -1,
							TypeModifier: -1,
						},
					},
				},
				&pgproto3.DataRow{
					Values: [][]byte{
						[]byte("off"),
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
			assert.Equal(t, msg, retMsg, fmt.Sprintf("tc %d", ind))
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

func TestPrepExtendedPipeline(t *testing.T) {
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

	tt := []MessageGroup{
		{
			Request: []pgproto3.FrontendMessage{
				&pgproto3.Parse{
					Name:  "sssssdss",
					Query: "SELECT 1",
				},
				&pgproto3.Sync{},
				&pgproto3.Describe{
					ObjectType: 'S',
					Name:       "sssssdss",
				},
				&pgproto3.Sync{},
			},

			Response: []pgproto3.BackendMessage{
				&pgproto3.ParseComplete{},

				&pgproto3.ReadyForQuery{
					TxStatus: byte(txstatus.TXIDLE),
				},
				&pgproto3.ParameterDescription{
					ParameterOIDs: []uint32{},
				},
				&pgproto3.RowDescription{
					Fields: []pgproto3.FieldDescription{
						{
							Name:         []byte("?column?"),
							DataTypeOID:  23,
							DataTypeSize: 4,
							TypeModifier: -1,
						},
					},
				},
				&pgproto3.ReadyForQuery{
					TxStatus: byte(txstatus.TXIDLE),
				},
			},
		},
		{
			Request: []pgproto3.FrontendMessage{
				&pgproto3.Parse{
					Name:  "jodwjdewo",
					Query: "SELECT 1",
				},
				&pgproto3.Sync{},
				&pgproto3.Describe{
					ObjectType: 'S',
					Name:       "jodwjdewo",
				},
				&pgproto3.Sync{},
				&pgproto3.Bind{PreparedStatement: "jodwjdewo"},
				&pgproto3.Execute{},
				&pgproto3.Sync{},
			},
			Response: []pgproto3.BackendMessage{
				&pgproto3.ParseComplete{},

				&pgproto3.ReadyForQuery{
					TxStatus: byte(txstatus.TXIDLE),
				},

				&pgproto3.ParameterDescription{
					ParameterOIDs: []uint32{},
				},
				&pgproto3.RowDescription{
					Fields: []pgproto3.FieldDescription{
						{
							Name:         []byte("?column?"),
							DataTypeOID:  23,
							DataTypeSize: 4,
							TypeModifier: -1,
						},
					},
				},
				&pgproto3.ReadyForQuery{
					TxStatus: byte(txstatus.TXIDLE),
				},
				&pgproto3.BindComplete{},
				&pgproto3.DataRow{
					Values: [][]byte{
						[]byte("1"),
					},
				},
				&pgproto3.CommandComplete{
					CommandTag: []byte("SELECT 1"),
				},
				&pgproto3.ReadyForQuery{
					TxStatus: byte(txstatus.TXIDLE),
				},
			},
		},
		{
			Request: []pgproto3.FrontendMessage{
				&pgproto3.Parse{
					Name:  "n1",
					Query: "SELECT 1",
				},
				&pgproto3.Describe{
					ObjectType: 'S',
					Name:       "n1",
				},
				&pgproto3.Sync{},
				&pgproto3.Bind{PreparedStatement: "n1"},
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
							Name:         []byte("?column?"),
							DataTypeOID:  23,
							DataTypeSize: 4,
							TypeModifier: -1,
						},
					},
				},
				&pgproto3.ReadyForQuery{
					TxStatus: byte(txstatus.TXIDLE),
				},
				&pgproto3.BindComplete{},
				&pgproto3.DataRow{
					Values: [][]byte{
						[]byte("1"),
					},
				},
				&pgproto3.CommandComplete{
					CommandTag: []byte("SELECT 1"),
				},
				&pgproto3.ReadyForQuery{
					TxStatus: byte(txstatus.TXIDLE),
				},
			},
		},
		{
			Request: []pgproto3.FrontendMessage{
				&pgproto3.Parse{
					Name:  "nn1",
					Query: "SELECT 1",
				},
				&pgproto3.Bind{PreparedStatement: "nn1"},
				&pgproto3.Execute{},
				&pgproto3.Parse{
					Name:  "nn2",
					Query: "SELECT 2",
				},
				&pgproto3.Bind{PreparedStatement: "nn2"},
				&pgproto3.Execute{},
				&pgproto3.Sync{},
			},
			Response: []pgproto3.BackendMessage{
				&pgproto3.ParseComplete{},
				&pgproto3.BindComplete{},
				&pgproto3.DataRow{
					Values: [][]byte{
						[]byte("1"),
					},
				},
				&pgproto3.CommandComplete{
					CommandTag: []byte("SELECT 1"),
				},

				&pgproto3.ParseComplete{},
				&pgproto3.BindComplete{},
				&pgproto3.DataRow{
					Values: [][]byte{
						[]byte("2"),
					},
				},
				&pgproto3.CommandComplete{
					CommandTag: []byte("SELECT 1"),
				},
				&pgproto3.ReadyForQuery{
					TxStatus: byte(txstatus.TXIDLE),
				},
			},
		},
	}
	for _, msgroup := range tt {
		for _, msg := range msgroup.Request {
			frontend.Send(msg)
		}
	}
	_ = frontend.Flush()
	for _, msgroup := range tt {
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
			assert.Equal(t, msg, retMsg, fmt.Sprintf("tc %d", ind))
		}
	}
}

func TestPrepExtendedErrorParse(t *testing.T) {
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

	tt := []MessageGroup{
		{
			Request: []pgproto3.FrontendMessage{
				&pgproto3.Parse{
					Name:  "sssssdss",
					Query: "SELECT lol", /* should not compile */
				},
				&pgproto3.Sync{},
				&pgproto3.Parse{
					Name:  "sssssdss",
					Query: "SELECT lol2", /* should not compile */
				},
				&pgproto3.Sync{},
				&pgproto3.Parse{
					Name:  "sssssdss",
					Query: "SELECT 1", /* should not compile */
				},
				&pgproto3.Describe{
					ObjectType: 'S',
					Name:       "sssssdss",
				},
				&pgproto3.Sync{},
			},

			Response: []pgproto3.BackendMessage{
				&pgproto3.ErrorResponse{
					Severity:            "ERROR",
					SeverityUnlocalized: "ERROR",
					Code:                "42703",
					Message:             `column "lol" does not exist`,
					File:                `parse_relation.c`,
				},

				&pgproto3.ReadyForQuery{
					TxStatus: byte(txstatus.TXIDLE),
				},

				&pgproto3.ErrorResponse{
					Severity:            "ERROR",
					SeverityUnlocalized: "ERROR",
					Code:                "42703",
					Message:             `column "lol2" does not exist`,
					File:                `parse_relation.c`,
				},

				&pgproto3.ReadyForQuery{
					TxStatus: byte(txstatus.TXIDLE),
				},

				&pgproto3.ParseComplete{},
				&pgproto3.ParameterDescription{
					ParameterOIDs: []uint32{},
				},
				&pgproto3.RowDescription{
					Fields: []pgproto3.FieldDescription{
						{
							Name:         []byte("?column?"),
							DataTypeOID:  23,
							DataTypeSize: 4,
							TypeModifier: -1,
						},
					},
				},
				&pgproto3.ReadyForQuery{
					TxStatus: byte(txstatus.TXIDLE),
				},
			},
		},
	}
	for _, msgroup := range tt {
		for _, msg := range msgroup.Request {
			frontend.Send(msg)
		}
	}
	_ = frontend.Flush()
	for _, msgroup := range tt {
		backendFinished := false
		for ind, msg := range msgroup.Response {
			if backendFinished {
				break
			}
			retMsg, err := frontend.Receive()
			assert.NoError(t, err)
			switch retMsgType := retMsg.(type) {
			case *pgproto3.ErrorResponse:
				retMsgType.Line = 0
				retMsgType.Routine = ""
				retMsgType.Position = 0
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
			assert.Equal(t, msg, retMsg, fmt.Sprintf("tc %d", ind))
		}
	}
}
