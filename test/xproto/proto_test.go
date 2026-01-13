package prep_stmt_test

import (
	"context"
	"fmt"
	"net"
	"os"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/pg-sharding/spqr/pkg/catalog"
	"github.com/pg-sharding/spqr/pkg/models/spqrerror"
	"github.com/pg-sharding/spqr/pkg/txstatus"
	"github.com/pg-sharding/spqr/router/xproto"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/stretchr/testify/assert"
)

type MessageGroup struct {
	Request  []pgproto3.FrontendMessage
	Response []pgproto3.BackendMessage
}

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
	addr := net.JoinHostPort(host, port)
	return net.Dial(proto, addr)
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
	_, err = conn.Exec(context.Background(), "CREATE KEY RANGE krid2 FROM 11 ROUTE TO sh2 FOR DISTRIBUTION ds1;")
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "could not setup sharding: %s\n", err)
	}
	_, err = conn.Exec(context.Background(), "CREATE KEY RANGE krid1 FROM 0 ROUTE TO sh1 FOR DISTRIBUTION ds1;")
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "could not setup sharding: %s\n", err)
	}
	_, err = conn.Exec(context.Background(), "CREATE RELATION t(id) IN ds1;")
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "could not setup sharding: %s\n", err)
	}

	_, err = conn.Exec(context.Background(), "CREATE RELATION t2(id) IN ds1;")
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "could not setup sharding: %s\n", err)
	}

	_, err = conn.Exec(context.Background(), "CREATE DISTRIBUTION ds2 COLUMN TYPES varchar hash;")
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "could not setup sharding: %s\n", err)
	}
	_, err = conn.Exec(context.Background(), "CREATE KEY RANGE krid4 FROM 2147483648 ROUTE TO sh2 FOR DISTRIBUTION ds2;")
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "could not setup sharding: %s\n", err)
	}
	_, err = conn.Exec(context.Background(), "CREATE KEY RANGE krid3 FROM 0 ROUTE TO sh1 FOR DISTRIBUTION ds2;")
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "could not setup sharding: %s\n", err)
	}
	_, err = conn.Exec(context.Background(), "CREATE RELATION text_table (id HASH MURMUR) IN ds2;")
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "could not setup sharding: %s\n", err)
	}

	_, err = conn.Exec(context.Background(), "CREATE REFERENCE TABLE xproto_ref;")
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "could not setup sharding: %s\n", err)
	}
	_, err = conn.Exec(context.Background(), "CREATE REFERENCE TABLE xproto_ref_autoinc AUTO INCREMENT id;")
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

	_, err = conn.Exec(context.Background(), "CREATE TABLE t2 (id int, val int)")
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "could not create table: %s\n", err)
	}

	_, err = conn.Exec(context.Background(), "CREATE TABLE text_table (id text)")
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "could not create table: %s\n", err)
	}
	_, err = conn.Exec(context.Background(), "CREATE TABLE xproto_ref (a int, b int, c int)")
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "could not create table: %s\n", err)
	}
	_, err = conn.Exec(context.Background(), "CREATE TABLE xproto_ref_autoinc (a int, b int, id int)")
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

func TestSimpleMultiShardTxBlock(t *testing.T) {
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

	for _, msgroup := range []MessageGroup{
		{
			Request: []pgproto3.FrontendMessage{
				&pgproto3.Query{
					String: "BEGIN",
				},

				&pgproto3.Query{
					String: "SET __spqr__engine_v2 TO true",
				},

				&pgproto3.Parse{
					Query: "INSERT INTO xproto_ref (a) VALUES(1)",
				},

				&pgproto3.Bind{},
				&pgproto3.Execute{},
				&pgproto3.Sync{},

				&pgproto3.Parse{
					Query: "TRUNCATE xproto_ref",
				},
				&pgproto3.Bind{},
				&pgproto3.Execute{},
				&pgproto3.Sync{},

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
					CommandTag: []byte("SET"),
				},

				&pgproto3.ReadyForQuery{
					TxStatus: byte(txstatus.TXACT),
				},

				&pgproto3.ParseComplete{},

				&pgproto3.BindComplete{},

				&pgproto3.CommandComplete{
					CommandTag: []byte("INSERT 0 1"),
				},

				&pgproto3.ReadyForQuery{
					TxStatus: byte(txstatus.TXACT),
				},

				&pgproto3.ParseComplete{},

				&pgproto3.BindComplete{},

				&pgproto3.CommandComplete{
					CommandTag: []byte("TRUNCATE TABLE"),
				},

				&pgproto3.ReadyForQuery{
					TxStatus: byte(txstatus.TXACT),
				},

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
		for i, msg := range msgroup.Response {
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
			assert.Equal(t, msg, retMsg, "iter %d", i)
		}
	}
}

func TestSimpleReferenceRelationAutoinc(t *testing.T) {
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

	for _, msgroup := range []MessageGroup{
		{
			Request: []pgproto3.FrontendMessage{
				&pgproto3.Query{
					String: "BEGIN",
				},

				&pgproto3.Query{
					String: "SET __spqr__.engine_v2 TO true",
				},

				&pgproto3.Parse{
					Name:  "autoinc_p_0",
					Query: "INSERT INTO xproto_ref_autoinc (a,b) VALUES(1,2)",
				},

				&pgproto3.Bind{
					PreparedStatement: "autoinc_p_0",
				},
				&pgproto3.Execute{},
				&pgproto3.Sync{},

				&pgproto3.Query{
					String: "TABLE xproto_ref_autoinc",
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
					CommandTag: []byte("SET"),
				},

				&pgproto3.ReadyForQuery{
					TxStatus: byte(txstatus.TXACT),
				},

				&pgproto3.ParseComplete{},

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
							Name:                 []byte("a"),
							DataTypeOID:          23,
							TableAttributeNumber: 1,
							DataTypeSize:         4,
							TypeModifier:         -1,
						},
						{
							Name:                 []byte("b"),
							DataTypeOID:          23,
							TableAttributeNumber: 2,
							DataTypeSize:         4,
							TypeModifier:         -1,
						},
						{
							Name:                 []byte("id"),
							DataTypeOID:          23,
							TableAttributeNumber: 3,
							DataTypeSize:         4,
							TypeModifier:         -1,
						},
					},
				},

				&pgproto3.DataRow{
					Values: [][]byte{
						{0x31},
						{0x32},
						{0x31},
					},
				},

				&pgproto3.CommandComplete{
					CommandTag: []byte{},
				},

				&pgproto3.ReadyForQuery{
					TxStatus: byte(txstatus.TXACT),
				},

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
				&pgproto3.Query{
					String: "BEGIN",
				},

				&pgproto3.Query{
					String: "SET __spqr__.engine_v2 TO true",
				},

				&pgproto3.Parse{
					Name:  "autoinc_p_1",
					Query: "INSERT INTO xproto_ref_autoinc (a,b) VALUES($1,122)",
				},

				&pgproto3.Describe{
					Name:       "autoinc_p_1",
					ObjectType: 'S',
				},

				&pgproto3.Bind{
					PreparedStatement: "autoinc_p_1",
					Parameters:        [][]byte{fmt.Appendf(nil, "%d", 112)},
				},
				&pgproto3.Execute{},
				&pgproto3.Sync{},

				&pgproto3.Describe{
					Name:       "autoinc_p_1",
					ObjectType: 'S',
				},

				&pgproto3.Bind{
					PreparedStatement: "autoinc_p_1",
					Parameters:        [][]byte{fmt.Appendf(nil, "%d", 113)},
				},
				&pgproto3.Execute{},
				&pgproto3.Sync{},

				&pgproto3.Query{
					String: "TABLE xproto_ref_autoinc",
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
					CommandTag: []byte("SET"),
				},

				&pgproto3.ReadyForQuery{
					TxStatus: byte(txstatus.TXACT),
				},

				&pgproto3.ParseComplete{},

				&pgproto3.ParameterDescription{
					ParameterOIDs: []uint32{23},
				},
				&pgproto3.NoData{},

				&pgproto3.BindComplete{},

				&pgproto3.CommandComplete{
					CommandTag: []byte("INSERT 0 1"),
				},

				&pgproto3.ReadyForQuery{
					TxStatus: byte(txstatus.TXACT),
				},

				&pgproto3.ParameterDescription{
					ParameterOIDs: []uint32{23},
				},
				&pgproto3.NoData{},

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
							Name:                 []byte("a"),
							DataTypeOID:          23,
							TableAttributeNumber: 1,
							DataTypeSize:         4,
							TypeModifier:         -1,
						},
						{
							Name:                 []byte("b"),
							DataTypeOID:          23,
							TableAttributeNumber: 2,
							DataTypeSize:         4,
							TypeModifier:         -1,
						},
						{
							Name:                 []byte("id"),
							DataTypeOID:          23,
							TableAttributeNumber: 3,
							DataTypeSize:         4,
							TypeModifier:         -1,
						},
					},
				},

				&pgproto3.DataRow{
					Values: [][]byte{
						{0x31, 0x31, 0x32},
						{0x31, 0x32, 0x32},
						{0x33},
					},
				},

				&pgproto3.DataRow{
					Values: [][]byte{
						{0x31, 0x31, 0x33},
						{0x31, 0x32, 0x32},
						{0x35},
					},
				},

				&pgproto3.CommandComplete{
					CommandTag: []byte{},
				},

				&pgproto3.ReadyForQuery{
					TxStatus: byte(txstatus.TXACT),
				},

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
		for i, msg := range msgroup.Response {
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

			assert.Equal(t, msg, retMsg, "iter %d", i)
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

func TestHintRoutingXproto(t *testing.T) {
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

	for gr, msgroup := range []MessageGroup{
		{
			Request: []pgproto3.FrontendMessage{
				&pgproto3.Query{
					String: "RESET ALL",
				},
				&pgproto3.Query{
					String: "INSERT INTO t (id) VALUES(1)",
				},
				&pgproto3.Query{
					String: "SET __spqr__sharding_key = 12",
				},

				&pgproto3.Query{
					String: "SET __spqr__distribution = 'ds1'",
				},
				&pgproto3.Parse{
					Name:  "rh_x_proto_1",
					Query: "SELECT * from t WHERE id < $1",
				},
				&pgproto3.Bind{
					PreparedStatement: "rh_x_proto_1",
					Parameters: [][]byte{
						[]byte("2"),
					},
					//  xproto.FormatCodeText = 0
					ParameterFormatCodes: []int16{0},
				},

				&pgproto3.Execute{},
				&pgproto3.Sync{},

				&pgproto3.Query{
					String: "DELETE FROM t WHERE /* __spqr__sharding_key: 1, __spqr__distribution: ds1 */ id = 1",
				},
			},
			Response: []pgproto3.BackendMessage{
				&pgproto3.CommandComplete{CommandTag: []byte("RESET")},
				&pgproto3.ReadyForQuery{
					TxStatus: byte(txstatus.TXIDLE),
				},

				&pgproto3.CommandComplete{CommandTag: []byte("INSERT 0 1")},
				&pgproto3.ReadyForQuery{
					TxStatus: byte(txstatus.TXIDLE),
				},

				&pgproto3.CommandComplete{CommandTag: []byte("SET")},
				&pgproto3.ReadyForQuery{
					TxStatus: byte(txstatus.TXIDLE),
				},

				&pgproto3.CommandComplete{CommandTag: []byte("SET")},
				&pgproto3.ReadyForQuery{
					TxStatus: byte(txstatus.TXIDLE),
				},

				&pgproto3.ParseComplete{},
				&pgproto3.BindComplete{},

				&pgproto3.CommandComplete{CommandTag: []byte("SELECT 0")},
				&pgproto3.ReadyForQuery{
					TxStatus: byte(txstatus.TXIDLE),
				},

				&pgproto3.CommandComplete{CommandTag: []byte("DELETE 1")},
				&pgproto3.ReadyForQuery{
					TxStatus: byte(txstatus.TXIDLE),
				},
			},
		},
		{
			Request: []pgproto3.FrontendMessage{
				&pgproto3.Query{
					String: "RESET ALL",
				},

				&pgproto3.Query{
					String: "SET __spqr__sharding_key = 1",
				},

				&pgproto3.Query{
					String: "SET __spqr__distribution = 'ds1'",
				},

				&pgproto3.Query{
					String: "INSERT INTO t (id) VALUES(1)",
				},

				&pgproto3.Parse{
					Name:  "rh_x_proto_1",
					Query: "SELECT * from t WHERE id < $1",
				},
				&pgproto3.Bind{
					PreparedStatement: "rh_x_proto_1",
					Parameters: [][]byte{
						[]byte("2"),
					},
					//  xproto.FormatCodeText = 0
					ParameterFormatCodes: []int16{0},
				},

				&pgproto3.Execute{},
				&pgproto3.Sync{},

				&pgproto3.Query{
					String: "DELETE FROM t WHERE /* __spqr__sharding_key: 1, __spqr__distribution: ds1 */ id = 1",
				},
			},
			Response: []pgproto3.BackendMessage{
				&pgproto3.CommandComplete{CommandTag: []byte("RESET")},
				&pgproto3.ReadyForQuery{
					TxStatus: byte(txstatus.TXIDLE),
				},

				&pgproto3.CommandComplete{CommandTag: []byte("SET")},
				&pgproto3.ReadyForQuery{
					TxStatus: byte(txstatus.TXIDLE),
				},
				&pgproto3.CommandComplete{CommandTag: []byte("SET")},
				&pgproto3.ReadyForQuery{
					TxStatus: byte(txstatus.TXIDLE),
				},

				/* TODO: fix order here */

				&pgproto3.CommandComplete{CommandTag: []byte("INSERT 0 1")},
				&pgproto3.ReadyForQuery{
					TxStatus: byte(txstatus.TXIDLE),
				},

				&pgproto3.ParseComplete{},
				&pgproto3.BindComplete{},

				&pgproto3.DataRow{
					Values: [][]byte{
						{byte(0x31)},
					},
				},

				&pgproto3.CommandComplete{CommandTag: []byte("SELECT 1")},
				&pgproto3.ReadyForQuery{
					TxStatus: byte(txstatus.TXIDLE),
				},

				&pgproto3.CommandComplete{CommandTag: []byte("DELETE 1")},
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
			assert.Equal(t, msg, retMsg, fmt.Sprintf("group %d iter msg %d", gr, ind))
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

func TestUnknownBindStatementError(t *testing.T) {
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

	for _, msgroup := range []MessageGroup{
		{
			Request: []pgproto3.FrontendMessage{
				&pgproto3.Parse{
					Name:  "ppp_qqq_1",
					Query: "SELECT now()",
				},
				&pgproto3.Sync{},
				&pgproto3.Bind{
					PreparedStatement: "ppp_qqq_2",
				},
				&pgproto3.Execute{},
				&pgproto3.Sync{},
			},
			Response: []pgproto3.BackendMessage{
				&pgproto3.ParseComplete{},

				&pgproto3.ReadyForQuery{
					TxStatus: byte(txstatus.TXIDLE),
				},

				&pgproto3.ErrorResponse{
					Severity: "ERROR",
					Code:     spqrerror.PG_PREPARED_STATEMENT_DOES_NOT_EXISTS,
					Message:  "prepared statement \"ppp_qqq_2\" does not exist",
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
			case *pgproto3.ErrorResponse:
				/* do not check this */
				retMsgType.Line = 0
				retMsgType.Routine = ""
				retMsgType.Position = 0
				retMsgType.SeverityUnlocalized = ""
				retMsgType.File = ""
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

func TestUnknownDescribeStatementError(t *testing.T) {
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

	for _, msgroup := range []MessageGroup{
		{
			Request: []pgproto3.FrontendMessage{
				&pgproto3.Parse{
					Name:  "ppp_qqq_d_1",
					Query: "SELECT now()",
				},
				&pgproto3.Sync{},
				&pgproto3.Describe{
					ObjectType: 'S',
					Name:       "ppp_qqq_d_2",
				},
				&pgproto3.Execute{},
				&pgproto3.Sync{},
			},
			Response: []pgproto3.BackendMessage{
				&pgproto3.ParseComplete{},

				&pgproto3.ReadyForQuery{
					TxStatus: byte(txstatus.TXIDLE),
				},

				&pgproto3.ErrorResponse{
					Severity: "ERROR",
					Code:     spqrerror.PG_PREPARED_STATEMENT_DOES_NOT_EXISTS,
					Message:  "prepared statement \"ppp_qqq_d_2\" does not exist",
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
			case *pgproto3.ErrorResponse:
				/* do not check this */
				retMsgType.Line = 0
				retMsgType.Routine = ""
				retMsgType.Position = 0
				retMsgType.SeverityUnlocalized = ""
				retMsgType.File = ""
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

func TestPrepStmtParametrizedQuerySimple(t *testing.T) {
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

	for gr, msgroup := range []MessageGroup{
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
					ParameterFormatCodes: []int16{xproto.FormatCodeText},
				},
				&pgproto3.Execute{},
				&pgproto3.Sync{},

				&pgproto3.Bind{
					PreparedStatement: "stmtcache_sr_2",
					Parameters: [][]byte{
						{0x0, 0x0, 0x0, 0x1},
					},
					ParameterFormatCodes: []int16{xproto.FormatCodeBinary},
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

				&pgproto3.DataRow{
					Values: [][]byte{
						[]byte("1"),
					},
				},

				&pgproto3.CommandComplete{
					CommandTag: []byte("SELECT 2"),
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

		{
			Request: []pgproto3.FrontendMessage{
				&pgproto3.Query{
					String: "begin;",
				},

				&pgproto3.Query{
					String: "SET __spqr__engine_v2 to TRUE;",
				},

				&pgproto3.Parse{
					Name:  "stmtcache_sr_ms_2",
					Query: "INSERT INTO t (id) VALUES($1);",
				},

				&pgproto3.Describe{
					Name:       "stmtcache_sr_ms_2",
					ObjectType: 'S',
				},
				&pgproto3.Sync{},
				&pgproto3.Bind{
					PreparedStatement: "stmtcache_sr_ms_2",
					Parameters: [][]byte{
						[]byte("1"),
					},
					ParameterFormatCodes: []int16{xproto.FormatCodeText},
				},
				&pgproto3.Execute{},
				&pgproto3.Sync{},

				&pgproto3.Bind{
					PreparedStatement: "stmtcache_sr_ms_2",
					Parameters: [][]byte{
						[]byte("101"),
					},
					ParameterFormatCodes: []int16{xproto.FormatCodeText},
				},
				&pgproto3.Execute{},
				&pgproto3.Sync{},

				&pgproto3.Bind{
					PreparedStatement: "stmtcache_sr_ms_2",
					Parameters: [][]byte{
						[]byte("1"),
					},
					ParameterFormatCodes: []int16{xproto.FormatCodeText},
				},
				&pgproto3.Execute{},
				&pgproto3.Sync{},

				&pgproto3.Query{
					String: `SELECT * FROM t /*__spqr__execute_on: sh1 */`,
				},

				&pgproto3.Query{
					String: `SELECT * FROM t /*__spqr__execute_on: sh2 */`,
				},

				&pgproto3.Query{
					String: "ROLLBACK;",
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
					CommandTag: []byte("SET"),
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

				&pgproto3.BindComplete{},
				&pgproto3.CommandComplete{
					CommandTag: []byte("INSERT 0 1"),
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

				&pgproto3.DataRow{
					Values: [][]byte{
						[]byte("1"),
					},
				},

				&pgproto3.CommandComplete{
					// XXX: FIX	CommandTag: []byte("SELECT 2"),
					CommandTag: []byte{},
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
						[]byte("101"),
					},
				},

				&pgproto3.CommandComplete{
					// XXX: FIX	CommandTag: []byte("SELECT 1"),
					CommandTag: []byte{},
				},

				&pgproto3.ReadyForQuery{
					TxStatus: byte(txstatus.TXACT),
				},
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
					Name:  "stmtcache_sr_1_tt",
					Query: "BEGIN",
				},
				&pgproto3.Describe{
					Name:       "stmtcache_sr_1_tt",
					ObjectType: 'S',
				},
				&pgproto3.Sync{},
				&pgproto3.Bind{
					PreparedStatement: "stmtcache_sr_1_tt",
				},
				&pgproto3.Execute{},
				&pgproto3.Sync{},

				&pgproto3.Parse{
					Name:  "stmtcache_sr_2_tt",
					Query: "INSERT INTO text_table (id) VALUES($1);",
				},

				&pgproto3.Describe{
					Name:       "stmtcache_sr_2_tt",
					ObjectType: 'S',
				},

				&pgproto3.Sync{},

				&pgproto3.Parse{
					Name:  "stmtcache_sr_4_tt",
					Query: "UPDATE text_table SET id = id || $1 WHERE id = $2;",
				},

				&pgproto3.Describe{
					Name:       "stmtcache_sr_2_tt",
					ObjectType: 'S',
				},

				&pgproto3.Sync{},

				&pgproto3.Bind{
					PreparedStatement: "stmtcache_sr_2_tt",
					Parameters: [][]byte{
						[]byte("23i923i99032"),
					},
					ParameterFormatCodes: []int16{xproto.FormatCodeText},
				},
				&pgproto3.Execute{},
				&pgproto3.Sync{},

				&pgproto3.Bind{
					PreparedStatement: "stmtcache_sr_2_tt",
					Parameters: [][]byte{
						[]byte("23i923i99032"),
					},
					ParameterFormatCodes: []int16{xproto.FormatCodeBinary},
				},
				&pgproto3.Execute{},
				&pgproto3.Sync{},

				&pgproto3.Bind{
					PreparedStatement: "stmtcache_sr_4_tt",
					Parameters: [][]byte{
						[]byte("zz"),
						[]byte("23i923i99032"),
					},
					ParameterFormatCodes: []int16{xproto.FormatCodeBinary},
				},
				&pgproto3.Execute{},
				&pgproto3.Sync{},

				&pgproto3.Query{
					String: `SELECT * FROM text_table`,
				},

				&pgproto3.Parse{
					Name:  "stmtcache_sr_3_tt",
					Query: "ROLLBACK",
				},
				&pgproto3.Describe{
					Name:       "stmtcache_sr_3_tt",
					ObjectType: 'S',
				},
				&pgproto3.Sync{},

				&pgproto3.Bind{
					PreparedStatement: "stmtcache_sr_3_tt",
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
						catalog.TEXTOID,
					},
				},

				&pgproto3.NoData{},

				&pgproto3.ReadyForQuery{
					TxStatus: byte(txstatus.TXACT),
				},

				&pgproto3.ParseComplete{},

				&pgproto3.ParameterDescription{
					ParameterOIDs: []uint32{
						catalog.TEXTOID,
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

				&pgproto3.BindComplete{},

				&pgproto3.CommandComplete{
					CommandTag: []byte("INSERT 0 1"),
				},

				&pgproto3.ReadyForQuery{
					TxStatus: byte(txstatus.TXACT),
				},

				&pgproto3.BindComplete{},

				&pgproto3.CommandComplete{
					CommandTag: []byte("UPDATE 2"),
				},

				&pgproto3.ReadyForQuery{
					TxStatus: byte(txstatus.TXACT),
				},

				&pgproto3.RowDescription{
					Fields: []pgproto3.FieldDescription{
						{
							Name:                 []byte("id"),
							DataTypeOID:          catalog.TEXTOID,
							DataTypeSize:         -1,
							TypeModifier:         -1,
							TableAttributeNumber: 1,
						},
					},
				},

				&pgproto3.DataRow{
					Values: [][]byte{
						[]byte("23i923i99032zz"),
					},
				},
				&pgproto3.DataRow{
					Values: [][]byte{
						[]byte("23i923i99032zz"),
					},
				},

				&pgproto3.CommandComplete{
					CommandTag: []byte("SELECT 2"),
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
			assert.Equal(t, msg, retMsg, fmt.Sprintf("gr=%d index=%d", gr, ind))
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
		{
			Request: []pgproto3.FrontendMessage{
				&pgproto3.Parse{
					Name:  "",
					Query: "select 'Hello, world!';",
				},
				&pgproto3.Bind{},

				&pgproto3.Describe{
					Name:       "",
					ObjectType: 'P',
				},
				&pgproto3.Execute{},
				&pgproto3.Sync{},

				&pgproto3.Parse{
					Name:  "",
					Query: "select 'Hello, world 2!';",
				},
				&pgproto3.Bind{},

				&pgproto3.Describe{
					Name:       "",
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

				&pgproto3.ParseComplete{},

				&pgproto3.BindComplete{},

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

				&pgproto3.DataRow{
					Values: [][]byte{
						[]byte("Hello, world 2!"),
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
	} {
		for _, msg := range msgroup.Request {
			frontend.Send(msg)
		}
		_ = frontend.Flush()
		backendFinished := false
		for i, msg := range msgroup.Response {
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
			assert.Equal(t, msg, retMsg, "iter %d", i)
		}
	}
}

func TestPrepStmtSimpleProtoViolation(t *testing.T) {
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

	for _, msgroup := range []MessageGroup{
		{
			Request: []pgproto3.FrontendMessage{
				&pgproto3.Parse{
					Name:  "stmtcache_v_1",
					Query: "select 'Hello, world!';",
				},
				&pgproto3.Describe{
					Name:       "",
					ObjectType: 'P',
				},
				&pgproto3.Sync{},
			},
			Response: []pgproto3.BackendMessage{
				&pgproto3.ParseComplete{},
				&pgproto3.ErrorResponse{
					Severity: "ERROR",
					Code:     "34000",
					Message:  "portal \"\" does not exist",
				},
				&pgproto3.ReadyForQuery{
					TxStatus: byte(txstatus.TXIDLE),
				},
			},
		},

		{
			Request: []pgproto3.FrontendMessage{
				&pgproto3.Parse{
					Name:  "stmtcache_v_2",
					Query: "select 'Hello, world!';",
				},
				&pgproto3.Bind{
					PreparedStatement: "stmtcache_v_2",
				},
				&pgproto3.Describe{
					Name:       "",
					ObjectType: 'P',
				},
				&pgproto3.Sync{},

				&pgproto3.Describe{
					Name:       "",
					ObjectType: 'P',
				},
				&pgproto3.Sync{},
			},
			Response: []pgproto3.BackendMessage{
				&pgproto3.ParseComplete{},
				&pgproto3.BindComplete{},
				&pgproto3.RowDescription{
					Fields: []pgproto3.FieldDescription{
						{
							Name:         []byte("?column?"),
							DataTypeOID:  25,
							DataTypeSize: -1,
							TypeModifier: -1,
						},
					},
				},
				&pgproto3.ReadyForQuery{
					TxStatus: byte(txstatus.TXIDLE),
				},
				&pgproto3.ErrorResponse{
					Severity: "ERROR",
					Code:     "34000",
					Message:  "portal \"\" does not exist",
				},
				&pgproto3.ReadyForQuery{
					TxStatus: byte(txstatus.TXIDLE),
				},
			},
		},

		{
			Request: []pgproto3.FrontendMessage{
				&pgproto3.Bind{
					PreparedStatement: "out-of-nowhere",
				},
				&pgproto3.Sync{},
			},
			Response: []pgproto3.BackendMessage{
				&pgproto3.ErrorResponse{
					Severity: "ERROR",
					Code:     "26000",
					Message:  "prepared statement \"out-of-nowhere\" does not exist",
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
		for i, msg := range msgroup.Response {
			if backendFinished {
				break
			}
			retMsg, err := frontend.Receive()
			assert.NoError(t, err)
			switch retMsgType := retMsg.(type) {
			case *pgproto3.ErrorResponse:
				retMsgType.Routine = ""
				retMsgType.Line = 0
				retMsgType.File = ""
				retMsgType.SeverityUnlocalized = ""
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
			assert.Equal(t, msg, retMsg, "iter %d", i)
		}
	}
}

func TestPrepStmtMultishardXproto(t *testing.T) {
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

	for _, msgroup := range []MessageGroup{
		{
			Request: []pgproto3.FrontendMessage{
				&pgproto3.Parse{
					Name:  "xproto_ddl_multishard",
					Query: "CREATE SCHEMA test_schema;",
				},
				&pgproto3.Bind{
					PreparedStatement: "xproto_ddl_multishard",
				},
				&pgproto3.Describe{
					Name:       "",
					ObjectType: 'P',
				},
				&pgproto3.Execute{},
				&pgproto3.Sync{},

				&pgproto3.Parse{
					Name:  "xproto_ddl_multishard_2",
					Query: "DROP SCHEMA test_schema;",
				},
				&pgproto3.Bind{
					PreparedStatement: "xproto_ddl_multishard_2",
				},
				&pgproto3.Describe{
					Name:       "",
					ObjectType: 'P',
				},
				&pgproto3.Execute{},
				&pgproto3.Sync{},
			},
			Response: []pgproto3.BackendMessage{
				&pgproto3.ParseComplete{},
				&pgproto3.BindComplete{},
				&pgproto3.NoData{},
				&pgproto3.CommandComplete{
					CommandTag: []byte("CREATE SCHEMA"),
				},
				&pgproto3.ReadyForQuery{
					TxStatus: byte(txstatus.TXIDLE),
				},

				&pgproto3.ParseComplete{},
				&pgproto3.BindComplete{},
				&pgproto3.NoData{},
				&pgproto3.CommandComplete{

					CommandTag: []byte("DROP SCHEMA"),
				},
				&pgproto3.ReadyForQuery{
					TxStatus: byte(txstatus.TXIDLE),
				},
			},
		},

		{
			Request: []pgproto3.FrontendMessage{

				&pgproto3.Parse{
					Name:  "xproto_ddl_multishard_t_s",
					Query: "SELECT FROM t /* __spqr__engine_v2: true */;",
				},
				&pgproto3.Bind{
					PreparedStatement: "xproto_ddl_multishard_t_s",
				},
				&pgproto3.Describe{
					Name:       "",
					ObjectType: 'P',
				},
				&pgproto3.Execute{},
				&pgproto3.Sync{},
			},
			Response: []pgproto3.BackendMessage{
				&pgproto3.ParseComplete{},
				&pgproto3.BindComplete{},
				&pgproto3.RowDescription{},
				&pgproto3.CommandComplete{
					CommandTag: []byte("SELECT 0"),
				},
				&pgproto3.ReadyForQuery{
					TxStatus: byte(txstatus.TXIDLE),
				},
			},
		},

		{
			Request: []pgproto3.FrontendMessage{

				&pgproto3.Parse{
					Name:  "xproto_ddl_multishard_t_s_1",
					Query: "UPDATE t2 SET val = val + 1 /* __spqr__engine_v2: true */;",
				},
				&pgproto3.Parse{
					Name:  "xproto_ddl_multishard_t_s_2",
					Query: "SELECT FROM t2 /* __spqr__engine_v2: true */;",
				},
				&pgproto3.Sync{},
				&pgproto3.Bind{
					PreparedStatement: "xproto_ddl_multishard_t_s_2",
				},
				&pgproto3.Execute{},
				&pgproto3.Sync{},
				&pgproto3.Bind{
					PreparedStatement: "xproto_ddl_multishard_t_s_1",
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
				&pgproto3.BindComplete{},
				&pgproto3.CommandComplete{
					CommandTag: []byte("SELECT 0"),
				},
				&pgproto3.ReadyForQuery{
					TxStatus: byte(txstatus.TXIDLE),
				},
				&pgproto3.BindComplete{},
				&pgproto3.CommandComplete{
					CommandTag: []byte("UPDATE 0"),
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
		for i, msg := range msgroup.Response {
			if backendFinished {
				break
			}
			retMsg, err := frontend.Receive()
			assert.NoError(t, err)
			switch retMsgType := retMsg.(type) {
			case *pgproto3.ErrorResponse:
				retMsgType.Routine = ""
				retMsgType.Line = 0
				retMsgType.File = ""
				retMsgType.SeverityUnlocalized = ""
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
			assert.Equal(t, msg, retMsg, "iter %d", i)
		}
	}
}

func TestSplitUpdateXproto(t *testing.T) {
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

	for _, msgroup := range []MessageGroup{
		{
			Request: []pgproto3.FrontendMessage{

				&pgproto3.Query{
					String: "SET __spqr__.engine_v2 TO on",
				},

				&pgproto3.Query{
					String: "SET __spqr__.allow_split_update TO on",
				},

				&pgproto3.Query{
					String: "BEGIN;",
				},
				&pgproto3.Query{
					String: "INSERT INTO t (id) VALUES(3)",
				},

				&pgproto3.Parse{
					Name:  "xproto_split_update_p1",
					Query: "UPDATE t SET id = 12 WHERE id = 3;",
				},

				&pgproto3.Bind{
					PreparedStatement: "xproto_split_update_p1",
				},

				&pgproto3.Describe{
					Name:       "",
					ObjectType: 'P',
				},

				&pgproto3.Execute{},
				&pgproto3.Sync{},

				&pgproto3.Query{
					String: "SELECT * FROM t WHERE id < 10 /*__spqr__execute_on: sh1 */;",
				},

				&pgproto3.Query{
					String: "SELECT * FROM t /*__spqr__execute_on: sh2 */;",
				},

				&pgproto3.Query{
					String: "ROLLBACK;",
				},
			},
			Response: []pgproto3.BackendMessage{

				&pgproto3.CommandComplete{
					CommandTag: []byte("SET"),
				},
				&pgproto3.ReadyForQuery{
					TxStatus: byte(txstatus.TXIDLE),
				},

				&pgproto3.CommandComplete{
					CommandTag: []byte("SET"),
				},
				&pgproto3.ReadyForQuery{
					TxStatus: byte(txstatus.TXIDLE),
				},

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

				&pgproto3.ParseComplete{},

				&pgproto3.BindComplete{},

				&pgproto3.NoData{},

				&pgproto3.CommandComplete{
					CommandTag: []byte("UPDATE 1"),
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

				&pgproto3.CommandComplete{
					/* XXX: fix that */
					// CommandTag: []byte("SELECT 0"),
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
						[]byte{0x31, 0x32},
					},
				},

				&pgproto3.CommandComplete{
					/* XXX: fix that */
					// CommandTag: []byte("SELECT 1"),
				},
				&pgproto3.ReadyForQuery{
					TxStatus: byte(txstatus.TXACT),
				},

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
		for i, msg := range msgroup.Response {
			if backendFinished {
				break
			}
			retMsg, err := frontend.Receive()
			assert.NoError(t, err)
			switch retMsgType := retMsg.(type) {
			case *pgproto3.ErrorResponse:
				retMsgType.Routine = ""
				retMsgType.Line = 0
				retMsgType.File = ""
				retMsgType.SeverityUnlocalized = ""
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
			assert.Equal(t, msg, retMsg, "iter %d", i)
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

func TestPrepStmtNamedPortalBind(t *testing.T) {
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
	for _, msgroup := range []MessageGroup{
		{
			Request: []pgproto3.FrontendMessage{
				&pgproto3.Parse{
					Name:  "named_p_s_1",
					Query: "SELECT 1 AS z FROM t WHERE id = $1",
				},
				&pgproto3.Bind{
					PreparedStatement: "named_p_s_1",
					DestinationPortal: "d_p_1",
					Parameters:        [][]byte{[]byte("1")},
				},
				&pgproto3.Describe{
					ObjectType: 'S',
					Name:       "named_p_s_1",
				},

				&pgproto3.Describe{
					ObjectType: 'P',
					Name:       "d_p_1",
				},
				&pgproto3.Execute{
					Portal: "d_p_1",
				},
				&pgproto3.Sync{},
			},
			Response: []pgproto3.BackendMessage{
				&pgproto3.ParseComplete{},
				&pgproto3.BindComplete{},
				&pgproto3.ParameterDescription{
					ParameterOIDs: []uint32{23},
				},
				&pgproto3.RowDescription{
					Fields: []pgproto3.FieldDescription{
						{
							Name:         []byte("z"),
							DataTypeOID:  23,
							DataTypeSize: 4,
							TypeModifier: -1,
						},
					},
				},
				&pgproto3.RowDescription{
					Fields: []pgproto3.FieldDescription{
						{
							Name:         []byte("z"),
							DataTypeOID:  23,
							DataTypeSize: 4,
							TypeModifier: -1,
						},
					},
				},
				&pgproto3.CommandComplete{
					CommandTag: []byte("SELECT 0"),
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

func TestPrepStmtNamedPortal_NO_TX_bounds(t *testing.T) {
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
	for _, msgroup := range []MessageGroup{
		{
			Request: []pgproto3.FrontendMessage{
				&pgproto3.Parse{
					Name:  "named_tx_p_s_1",
					Query: "SELECT 1+$1 AS z /* __spqr__execute_on: sh1 */",
				},
				&pgproto3.Bind{
					PreparedStatement: "named_tx_p_s_1",
					DestinationPortal: "d_n_tx_p_1",
					Parameters:        [][]byte{[]byte("1")},
				},
				&pgproto3.Describe{
					ObjectType: 'S',
					Name:       "named_tx_p_s_1",
				},

				&pgproto3.Describe{
					ObjectType: 'P',
					Name:       "d_n_tx_p_1",
				},

				&pgproto3.Parse{
					Name:  "named_tx_p_s_2",
					Query: "SELECT 2+$1 AS z /* __spqr__execute_on: sh1 */",
				},
				&pgproto3.Bind{
					PreparedStatement: "named_tx_p_s_2",
					DestinationPortal: "d_n_tx_p_2",
					Parameters:        [][]byte{[]byte("1")},
				},
				&pgproto3.Describe{
					ObjectType: 'S',
					Name:       "named_tx_p_s_2",
				},

				&pgproto3.Describe{
					ObjectType: 'P',
					Name:       "d_n_tx_p_2",
				},

				&pgproto3.Execute{
					Portal: "d_n_tx_p_1",
				},

				&pgproto3.Execute{
					Portal: "d_n_tx_p_2",
				},
				&pgproto3.Sync{},
			},
			Response: []pgproto3.BackendMessage{
				&pgproto3.ParseComplete{},
				&pgproto3.BindComplete{},
				&pgproto3.ParameterDescription{
					ParameterOIDs: []uint32{23},
				},
				&pgproto3.RowDescription{
					Fields: []pgproto3.FieldDescription{
						{
							Name:         []byte("z"),
							DataTypeOID:  23,
							DataTypeSize: 4,
							TypeModifier: -1,
						},
					},
				},
				&pgproto3.RowDescription{
					Fields: []pgproto3.FieldDescription{
						{
							Name:         []byte("z"),
							DataTypeOID:  23,
							DataTypeSize: 4,
							TypeModifier: -1,
						},
					},
				},

				&pgproto3.ParseComplete{},
				&pgproto3.BindComplete{},
				&pgproto3.ParameterDescription{
					ParameterOIDs: []uint32{23},
				},
				&pgproto3.RowDescription{
					Fields: []pgproto3.FieldDescription{
						{
							Name:         []byte("z"),
							DataTypeOID:  23,
							DataTypeSize: 4,
							TypeModifier: -1,
						},
					},
				},
				&pgproto3.RowDescription{
					Fields: []pgproto3.FieldDescription{
						{
							Name:         []byte("z"),
							DataTypeOID:  23,
							DataTypeSize: 4,
							TypeModifier: -1,
						},
					},
				},

				&pgproto3.DataRow{
					Values: [][]byte{{0x32}},
				},
				&pgproto3.CommandComplete{
					CommandTag: []byte("SELECT 1"),
				},

				&pgproto3.DataRow{
					Values: [][]byte{{0x33}},
				},
				&pgproto3.CommandComplete{
					CommandTag: []byte("SELECT 1"),
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

func TestPrepStmtNamedPortal_TX_bounds(t *testing.T) {
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
	for _, msgroup := range []MessageGroup{
		{
			Request: []pgproto3.FrontendMessage{
				&pgproto3.Query{
					String: `BEGIN`,
				},
				&pgproto3.Parse{
					Name:  "named_tx_p_s_1",
					Query: "SELECT 1+$1 AS z /* __spqr__execute_on: sh1 */",
				},
				&pgproto3.Bind{
					PreparedStatement: "named_tx_p_s_1",
					DestinationPortal: "d_tx_p_1",
					Parameters:        [][]byte{[]byte("1")},
				},
				&pgproto3.Describe{
					ObjectType: 'S',
					Name:       "named_tx_p_s_1",
				},

				&pgproto3.Describe{
					ObjectType: 'P',
					Name:       "d_tx_p_1",
				},
				&pgproto3.Sync{},

				&pgproto3.Parse{
					Name:  "named_tx_p_s_2",
					Query: "SELECT 2+$1 AS z /* __spqr__execute_on: sh1 */",
				},
				&pgproto3.Bind{
					PreparedStatement: "named_tx_p_s_2",
					DestinationPortal: "d_tx_p_2",
					Parameters:        [][]byte{[]byte("1")},
				},
				&pgproto3.Describe{
					ObjectType: 'S',
					Name:       "named_tx_p_s_2",
				},

				&pgproto3.Describe{
					ObjectType: 'P',
					Name:       "d_tx_p_2",
				},
				&pgproto3.Sync{},

				&pgproto3.Execute{
					Portal: "d_tx_p_1",
				},
				&pgproto3.Sync{},

				&pgproto3.Execute{
					Portal: "d_tx_p_2",
				},
				&pgproto3.Sync{},

				&pgproto3.Query{
					String: `COMMIT`,
				},
			},
			Response: []pgproto3.BackendMessage{
				&pgproto3.CommandComplete{
					CommandTag: []byte("BEGIN"),
				},
				&pgproto3.ReadyForQuery{
					TxStatus: byte(txstatus.TXACT),
				},

				&pgproto3.ParseComplete{},
				&pgproto3.BindComplete{},
				&pgproto3.ParameterDescription{
					ParameterOIDs: []uint32{23},
				},
				&pgproto3.RowDescription{
					Fields: []pgproto3.FieldDescription{
						{
							Name:         []byte("z"),
							DataTypeOID:  23,
							DataTypeSize: 4,
							TypeModifier: -1,
						},
					},
				},
				&pgproto3.RowDescription{
					Fields: []pgproto3.FieldDescription{
						{
							Name:         []byte("z"),
							DataTypeOID:  23,
							DataTypeSize: 4,
							TypeModifier: -1,
						},
					},
				},
				&pgproto3.ReadyForQuery{
					TxStatus: byte(txstatus.TXACT),
				},

				&pgproto3.ParseComplete{},
				&pgproto3.BindComplete{},
				&pgproto3.ParameterDescription{
					ParameterOIDs: []uint32{23},
				},
				&pgproto3.RowDescription{
					Fields: []pgproto3.FieldDescription{
						{
							Name:         []byte("z"),
							DataTypeOID:  23,
							DataTypeSize: 4,
							TypeModifier: -1,
						},
					},
				},
				&pgproto3.RowDescription{
					Fields: []pgproto3.FieldDescription{
						{
							Name:         []byte("z"),
							DataTypeOID:  23,
							DataTypeSize: 4,
							TypeModifier: -1,
						},
					},
				},
				&pgproto3.ReadyForQuery{
					TxStatus: byte(txstatus.TXACT),
				},

				&pgproto3.DataRow{
					Values: [][]byte{{0x32}},
				},
				&pgproto3.CommandComplete{
					CommandTag: []byte("SELECT 1"),
				},

				&pgproto3.ReadyForQuery{
					TxStatus: byte(txstatus.TXACT),
				},

				&pgproto3.DataRow{
					Values: [][]byte{{0x33}},
				},
				&pgproto3.CommandComplete{
					CommandTag: []byte("SELECT 1"),
				},

				&pgproto3.ReadyForQuery{
					TxStatus: byte(txstatus.TXACT),
				},

				&pgproto3.CommandComplete{
					CommandTag: []byte("COMMIT"),
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

func TestPrepStmtAdvancedParsing(t *testing.T) {
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
					Query: "SELECT 11;",
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
						[]byte("11"),
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
					Query: "SELECT 12;",
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
						[]byte("12"),
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

	tt := []MessageGroup{
		{
			Request: []pgproto3.FrontendMessage{
				&pgproto3.Parse{
					Name:  "sssssdss",
					Query: "SELECT 13",
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
					Query: "SELECT 14",
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
						[]byte("14"),
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
					Query: "SELECT 15",
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
						[]byte("15"),
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
					Query: "SELECT 16",
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
						[]byte("16"),
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
					Query: "SELECT 17", /* should compile */
				},
				&pgproto3.Describe{
					ObjectType: 'S',
					Name:       "sssssdss",
				},
				&pgproto3.Sync{},
			},

			Response: []pgproto3.BackendMessage{
				&pgproto3.ErrorResponse{
					Severity: "ERROR",
				},

				&pgproto3.ReadyForQuery{
					TxStatus: byte(txstatus.TXIDLE),
				},

				&pgproto3.ErrorResponse{
					Severity: "ERROR",
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
				/* do not compare this fields */
				retMsgType.Line = 0
				retMsgType.Routine = ""
				retMsgType.Position = 0
				retMsgType.SeverityUnlocalized = ""
				retMsgType.File = ""
				retMsgType.Message = ""
				retMsgType.Code = ""

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

func TestDoubleDescribe(t *testing.T) {
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

	tt := []MessageGroup{
		{
			Request: []pgproto3.FrontendMessage{
				&pgproto3.Parse{
					Name:  "dd-1",
					Query: "SELECT 1;",
				},
				&pgproto3.Describe{
					ObjectType: 'S',
					Name:       "dd-1",
				},
				&pgproto3.Sync{},
				&pgproto3.Describe{
					ObjectType: 'S',
					Name:       "dd-1",
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
				/* do not compare this fields */
				retMsgType.Line = 0
				retMsgType.Routine = ""
				retMsgType.Position = 0
				retMsgType.SeverityUnlocalized = ""
				retMsgType.File = ""
				// retMsgType.Message = ""

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

func TestMultiPortal(t *testing.T) {
	t.Skip("todo")
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

	tt := []MessageGroup{
		{
			Request: []pgproto3.FrontendMessage{
				&pgproto3.Parse{
					Name:  "mp-0-1",
					Query: "SELECT 1",
				},
				&pgproto3.Sync{},

				&pgproto3.Bind{
					DestinationPortal: "",
					PreparedStatement: "mp-0-1",
				},
				&pgproto3.Sync{},

				&pgproto3.Execute{
					Portal: "",
				},
				&pgproto3.Sync{},
			},

			Response: []pgproto3.BackendMessage{

				&pgproto3.ParseComplete{},
				&pgproto3.ReadyForQuery{
					TxStatus: byte(txstatus.TXIDLE),
				},

				&pgproto3.BindComplete{},

				&pgproto3.ReadyForQuery{
					TxStatus: byte(txstatus.TXIDLE),
				},

				&pgproto3.ErrorResponse{
					Severity: "ERROR",
					Code:     "34000",
				},
				&pgproto3.ReadyForQuery{
					TxStatus: byte(txstatus.TXIDLE),
				},
			},
		},
		{
			Request: []pgproto3.FrontendMessage{
				&pgproto3.Parse{
					Name:  "mp-1",
					Query: "SELECT 1",
				},
				&pgproto3.Sync{},
				&pgproto3.Parse{
					Name:  "mp-2",
					Query: "BEGIN",
				},
				&pgproto3.Parse{
					Name:  "mp-3",
					Query: "ABORT",
				},
				&pgproto3.Sync{},

				&pgproto3.Bind{
					DestinationPortal: "p1",
					PreparedStatement: "mp-1",
				},

				&pgproto3.Bind{
					DestinationPortal: "p2",
					PreparedStatement: "mp-2",
				},

				&pgproto3.Execute{
					Portal: "p1",
				},

				&pgproto3.Execute{
					Portal: "p2",
				},

				&pgproto3.Sync{},

				&pgproto3.Close{
					ObjectType: 'P',
					Name:       "p1",
				},

				&pgproto3.Bind{
					DestinationPortal: "p1",
					PreparedStatement: "mp-3",
				},

				&pgproto3.Execute{
					Portal: "p1",
				},

				&pgproto3.Sync{},
			},

			Response: []pgproto3.BackendMessage{

				&pgproto3.ParseComplete{},
				&pgproto3.ReadyForQuery{
					TxStatus: byte(txstatus.TXIDLE),
				},

				&pgproto3.ParseComplete{},
				&pgproto3.ParseComplete{},
				&pgproto3.ReadyForQuery{
					TxStatus: byte(txstatus.TXIDLE),
				},

				&pgproto3.BindComplete{},
				&pgproto3.BindComplete{},

				&pgproto3.DataRow{
					Values: [][]byte{
						{'1'},
					},
				},
				&pgproto3.CommandComplete{
					CommandTag: []byte("SELECT 1"),
				},
				&pgproto3.CommandComplete{
					CommandTag: []byte("BEGIN"),
				},

				&pgproto3.ReadyForQuery{TxStatus: byte(txstatus.TXACT)},

				&pgproto3.CloseComplete{},

				&pgproto3.BindComplete{},
				&pgproto3.CommandComplete{
					CommandTag: []byte("ROLLBACK"),
				},

				&pgproto3.ReadyForQuery{TxStatus: byte(txstatus.TXIDLE)},
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
				/* do not compare this fields */
				retMsgType.Line = 0
				retMsgType.Routine = ""
				retMsgType.Position = 0
				retMsgType.SeverityUnlocalized = ""
				retMsgType.File = ""
				retMsgType.Message = ""

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

func TestPrepStmtBinaryFormat(t *testing.T) {
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

	for _, msgroup := range []MessageGroup{
		{
			Request: []pgproto3.FrontendMessage{
				&pgproto3.Query{String: "begin"},
				&pgproto3.Query{String: "insert into t (id) values(1022)"},

				&pgproto3.Parse{
					Name:  "stmtcache_ft_1",
					Query: "SELECT * FROM t where id = $1;",
				},
				&pgproto3.Describe{
					Name:       "stmtcache_ft_1",
					ObjectType: 'S',
				},
				&pgproto3.Bind{
					PreparedStatement: "stmtcache_ft_1",
					Parameters: [][]byte{
						// 1022
						{0, 0, 3, 254},
					},
					ParameterFormatCodes: []int16{1},
				},
				&pgproto3.Execute{},
				&pgproto3.Sync{},

				&pgproto3.Query{String: "rollback"},
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

				&pgproto3.DataRow{
					Values: [][]byte{
						[]byte("1022"),
					},
				},

				&pgproto3.CommandComplete{
					CommandTag: []byte("SELECT 1"),
				},

				&pgproto3.ReadyForQuery{
					TxStatus: byte(txstatus.TXACT),
				},

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

func TestDDL(t *testing.T) {
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

	for _, msgroup := range []MessageGroup{
		{
			Request: []pgproto3.FrontendMessage{

				&pgproto3.Parse{
					Name:  "ddl_xproto_1",
					Query: "ALTER TABLE t DROP CONSTRAINT IF EXISTS some_constraint",
				},
				&pgproto3.Sync{},
				&pgproto3.Bind{
					PreparedStatement: "ddl_xproto_1",
					Parameters:        [][]byte{},
					//  xproto.FormatCodeText = 0
					ParameterFormatCodes: []int16{},
				},
				&pgproto3.Execute{},
				&pgproto3.Sync{},
			},
			Response: []pgproto3.BackendMessage{
				&pgproto3.ParseComplete{},
				&pgproto3.ReadyForQuery{
					TxStatus: byte(txstatus.TXIDLE),
				},
				&pgproto3.BindComplete{},
				&pgproto3.CommandComplete{CommandTag: []byte("ALTER TABLE")},
				&pgproto3.ReadyForQuery{
					TxStatus: byte(txstatus.TXIDLE),
				},
			},
		},
		{
			Request: []pgproto3.FrontendMessage{

				&pgproto3.Parse{
					Name:  "ddl_xproto_2",
					Query: "ALTER TABLE t DROP CONSTRAINT IF EXISTS other_constraint",
				},
				&pgproto3.Sync{},
				&pgproto3.Bind{
					PreparedStatement: "ddl_xproto_2",
					Parameters:        [][]byte{},
					//  xproto.FormatCodeText = 0
					ParameterFormatCodes: []int16{},
				},
				&pgproto3.Execute{},
				&pgproto3.Sync{},
			},
			Response: []pgproto3.BackendMessage{
				&pgproto3.ParseComplete{},
				&pgproto3.ReadyForQuery{TxStatus: 73},
				&pgproto3.BindComplete{},
				&pgproto3.CommandComplete{CommandTag: []byte("ALTER TABLE")},
				&pgproto3.ReadyForQuery{
					TxStatus: byte(txstatus.TXIDLE),
				},
			},
		},
		{
			Request: []pgproto3.FrontendMessage{
				&pgproto3.Bind{
					PreparedStatement: "ddl_xproto_1",
					Parameters:        [][]byte{},
					//  xproto.FormatCodeText = 0
					ParameterFormatCodes: []int16{},
				},
				&pgproto3.Execute{},
				&pgproto3.Sync{},
			},
			Response: []pgproto3.BackendMessage{
				&pgproto3.BindComplete{},
				&pgproto3.CommandComplete{CommandTag: []byte("ALTER TABLE")},
				&pgproto3.ReadyForQuery{
					TxStatus: byte(txstatus.TXIDLE),
				},
			},
		},
		{
			Request: []pgproto3.FrontendMessage{
				&pgproto3.Describe{ObjectType: byte('S'), Name: "ddl_xproto_1"},
				&pgproto3.Sync{},
				&pgproto3.Bind{
					PreparedStatement: "ddl_xproto_1",
					Parameters:        [][]byte{},
					//  xproto.FormatCodeText = 0
					ParameterFormatCodes: []int16{},
				},
				&pgproto3.Execute{},
				&pgproto3.Sync{},
			},
			Response: []pgproto3.BackendMessage{
				&pgproto3.ParameterDescription{ParameterOIDs: []uint32{}},
				&pgproto3.NoData{},
				&pgproto3.ReadyForQuery{
					TxStatus: byte(txstatus.TXIDLE),
				},
				&pgproto3.BindComplete{},
				&pgproto3.CommandComplete{CommandTag: []byte("ALTER TABLE")},
				&pgproto3.ReadyForQuery{
					TxStatus: byte(txstatus.TXIDLE),
				},
			},
		},
		{
			Request: []pgproto3.FrontendMessage{
				&pgproto3.Bind{
					PreparedStatement: "ddl_xproto_1",
					Parameters:        [][]byte{},
					//  xproto.FormatCodeText = 0
					ParameterFormatCodes: []int16{},
				},
				&pgproto3.Describe{ObjectType: byte('P'), Name: ""},
				&pgproto3.Execute{},
				&pgproto3.Sync{},
			},
			Response: []pgproto3.BackendMessage{
				&pgproto3.BindComplete{},
				&pgproto3.NoData{},
				&pgproto3.CommandComplete{CommandTag: []byte("ALTER TABLE")},
				&pgproto3.ReadyForQuery{
					TxStatus: byte(txstatus.TXIDLE),
				},
			},
		},
		{
			Request: []pgproto3.FrontendMessage{
				&pgproto3.Parse{
					Name:  "ddl_xproto_3",
					Query: "ALTER TABLE t DROP CONSTRAINT IF EXISTS some_constraint",
				},
				&pgproto3.Sync{},
				&pgproto3.Bind{
					PreparedStatement: "ddl_xproto_3",
					Parameters:        [][]byte{},
					//  xproto.FormatCodeText = 0
					ParameterFormatCodes: []int16{},
				},
				&pgproto3.Execute{},
				&pgproto3.Sync{},
				&pgproto3.Describe{ObjectType: byte('S'), Name: "ddl_xproto_3"},
				&pgproto3.Sync{},
			},
			Response: []pgproto3.BackendMessage{
				&pgproto3.ParseComplete{},
				&pgproto3.ReadyForQuery{
					TxStatus: byte(txstatus.TXIDLE),
				},
				&pgproto3.BindComplete{},
				&pgproto3.CommandComplete{CommandTag: []byte("ALTER TABLE")},
				&pgproto3.ReadyForQuery{
					TxStatus: byte(txstatus.TXIDLE),
				},
				&pgproto3.ParameterDescription{ParameterOIDs: []uint32{}},
				&pgproto3.NoData{},
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
			case *pgproto3.NoticeResponse:
				retMsg, err = frontend.Receive()
				assert.NoError(t, err)
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
			assert.Equal(t, msg, retMsg, fmt.Sprintf("iter msg %d", ind))
		}
	}
}

func TestMixedProtoTxcommands(t *testing.T) {
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

	for _, msgroup := range []MessageGroup{
		{
			Request: []pgproto3.FrontendMessage{
				&pgproto3.Query{
					String: "BEGIN;",
				},
				&pgproto3.Query{
					String: "BEGIN;",
				},
				&pgproto3.Query{
					String: "COMMIT;",
				},
			},
			Response: []pgproto3.BackendMessage{
				&pgproto3.CommandComplete{CommandTag: []byte("BEGIN")},
				&pgproto3.ReadyForQuery{
					TxStatus: byte(txstatus.TXACT),
				},
				&pgproto3.CommandComplete{CommandTag: []byte("BEGIN")},
				&pgproto3.ReadyForQuery{
					TxStatus: byte(txstatus.TXACT),
				},
				&pgproto3.CommandComplete{CommandTag: []byte("COMMIT")},
				&pgproto3.ReadyForQuery{
					TxStatus: byte(txstatus.TXIDLE),
				},
			},
		},

		{
			Request: []pgproto3.FrontendMessage{
				&pgproto3.Query{
					String: "BEGIN;",
				},
				&pgproto3.Query{
					String: "BEGIN;",
				},
				&pgproto3.Query{
					String: "ROLLBACK;",
				},
			},
			Response: []pgproto3.BackendMessage{
				&pgproto3.CommandComplete{CommandTag: []byte("BEGIN")},
				&pgproto3.ReadyForQuery{
					TxStatus: byte(txstatus.TXACT),
				},
				&pgproto3.CommandComplete{CommandTag: []byte("BEGIN")},
				&pgproto3.ReadyForQuery{
					TxStatus: byte(txstatus.TXACT),
				},
				&pgproto3.CommandComplete{CommandTag: []byte("ROLLBACK")},
				&pgproto3.ReadyForQuery{
					TxStatus: byte(txstatus.TXIDLE),
				},
			},
		},

		{
			Request: []pgproto3.FrontendMessage{
				&pgproto3.Parse{
					Name:  "p1",
					Query: "BEGIN",
				},

				&pgproto3.Parse{
					Name:  "p2",
					Query: "COMMIT",
				},

				&pgproto3.Parse{
					Name:  "p3",
					Query: "ROLLBACK",
				},

				/* TODO tests savepoint here */

				&pgproto3.Sync{},

				&pgproto3.Bind{
					PreparedStatement: "p1",
				},
				&pgproto3.Execute{},
				&pgproto3.Sync{},

				&pgproto3.Bind{
					PreparedStatement: "p1",
				},
				&pgproto3.Execute{},
				&pgproto3.Sync{},

				&pgproto3.Bind{
					PreparedStatement: "p2",
				},
				&pgproto3.Execute{},
				&pgproto3.Sync{},
			},
			Response: []pgproto3.BackendMessage{
				&pgproto3.ParseComplete{},
				&pgproto3.ParseComplete{},
				&pgproto3.ParseComplete{},
				&pgproto3.ReadyForQuery{
					TxStatus: byte(txstatus.TXIDLE),
				},
				&pgproto3.BindComplete{},
				&pgproto3.CommandComplete{CommandTag: []byte("BEGIN")},
				&pgproto3.ReadyForQuery{
					TxStatus: byte(txstatus.TXACT),
				},

				&pgproto3.BindComplete{},
				&pgproto3.CommandComplete{CommandTag: []byte("BEGIN")},
				&pgproto3.ReadyForQuery{
					TxStatus: byte(txstatus.TXACT),
				},

				&pgproto3.BindComplete{},
				&pgproto3.CommandComplete{CommandTag: []byte("COMMIT")},
				&pgproto3.ReadyForQuery{
					TxStatus: byte(txstatus.TXIDLE),
				},
			},
		},

		{
			Request: []pgproto3.FrontendMessage{
				&pgproto3.Parse{
					Name:  "pp1",
					Query: "BEGIN",
				},

				&pgproto3.Parse{
					Name:  "pp2",
					Query: "COMMIT",
				},

				&pgproto3.Parse{
					Name:  "pp3",
					Query: "ROLLBACK",
				},

				/* TODO tests savepoint here */

				&pgproto3.Sync{},

				&pgproto3.Bind{
					PreparedStatement: "pp1",
				},
				&pgproto3.Execute{},
				&pgproto3.Sync{},

				&pgproto3.Bind{
					PreparedStatement: "pp1",
				},
				&pgproto3.Execute{},
				&pgproto3.Sync{},

				&pgproto3.Bind{
					PreparedStatement: "pp3",
				},
				&pgproto3.Execute{},
				&pgproto3.Sync{},
			},
			Response: []pgproto3.BackendMessage{
				&pgproto3.ParseComplete{},
				&pgproto3.ParseComplete{},
				&pgproto3.ParseComplete{},
				&pgproto3.ReadyForQuery{
					TxStatus: byte(txstatus.TXIDLE),
				},
				&pgproto3.BindComplete{},
				&pgproto3.CommandComplete{CommandTag: []byte("BEGIN")},
				&pgproto3.ReadyForQuery{
					TxStatus: byte(txstatus.TXACT),
				},

				&pgproto3.BindComplete{},
				&pgproto3.CommandComplete{CommandTag: []byte("BEGIN")},
				&pgproto3.ReadyForQuery{
					TxStatus: byte(txstatus.TXACT),
				},

				&pgproto3.BindComplete{},
				&pgproto3.CommandComplete{CommandTag: []byte("ROLLBACK")},
				&pgproto3.ReadyForQuery{
					TxStatus: byte(txstatus.TXIDLE),
				},
			},
		},

		{
			Request: []pgproto3.FrontendMessage{
				&pgproto3.Parse{
					Name:  "ppp1",
					Query: "BEGIN",
				},

				&pgproto3.Parse{
					Name:  "ppp2",
					Query: "COMMIT",
				},

				&pgproto3.Parse{
					Name:  "ppp3",
					Query: "ROLLBACK",
				},

				/* TODO tests savepoint here */

				&pgproto3.Sync{},

				&pgproto3.Query{String: "BEGIN;"},

				&pgproto3.Bind{PreparedStatement: "ppp2"},
				&pgproto3.Execute{},
				&pgproto3.Sync{},

				&pgproto3.Query{String: "BEGIN;"},

				&pgproto3.Bind{PreparedStatement: "ppp1"},
				&pgproto3.Execute{},
				&pgproto3.Bind{PreparedStatement: "ppp3"},
				&pgproto3.Execute{},
				&pgproto3.Sync{},

				&pgproto3.Bind{PreparedStatement: "ppp1"},
				&pgproto3.Execute{},
				&pgproto3.Sync{},

				&pgproto3.Query{String: "COMMIT;"},
			},
			Response: []pgproto3.BackendMessage{
				&pgproto3.ParseComplete{},
				&pgproto3.ParseComplete{},
				&pgproto3.ParseComplete{},

				&pgproto3.ReadyForQuery{
					TxStatus: byte(txstatus.TXIDLE),
				},

				&pgproto3.CommandComplete{
					CommandTag: []byte("BEGIN"),
				},
				&pgproto3.ReadyForQuery{TxStatus: byte(txstatus.TXACT)},

				&pgproto3.BindComplete{},
				&pgproto3.CommandComplete{
					CommandTag: []byte("COMMIT"),
				},
				&pgproto3.ReadyForQuery{TxStatus: byte(txstatus.TXIDLE)},

				&pgproto3.CommandComplete{
					CommandTag: []byte("BEGIN"),
				},
				&pgproto3.ReadyForQuery{TxStatus: byte(txstatus.TXACT)},

				&pgproto3.BindComplete{},
				&pgproto3.CommandComplete{
					CommandTag: []byte("BEGIN"),
				},

				&pgproto3.BindComplete{},
				&pgproto3.CommandComplete{
					CommandTag: []byte("ROLLBACK"),
				},
				&pgproto3.ReadyForQuery{TxStatus: byte(txstatus.TXIDLE)},

				&pgproto3.BindComplete{},
				&pgproto3.CommandComplete{
					CommandTag: []byte("BEGIN"),
				},
				&pgproto3.ReadyForQuery{TxStatus: byte(txstatus.TXACT)},

				&pgproto3.CommandComplete{
					CommandTag: []byte("COMMIT"),
				},
				&pgproto3.ReadyForQuery{TxStatus: byte(txstatus.TXIDLE)},
			},
		},

		{
			Request: []pgproto3.FrontendMessage{
				&pgproto3.Parse{
					Name:  "pppp1",
					Query: "BEGIN",
				},

				&pgproto3.Parse{
					Name:  "pppp2",
					Query: "COMMIT",
				},

				&pgproto3.Parse{
					Name:  "pppp3",
					Query: "ROLLBACK",
				},

				&pgproto3.Parse{
					Name:  "pppp_query",
					Query: "SELECT 1 as a",
				},

				/* TODO tests savepoint here */

				&pgproto3.Sync{},

				&pgproto3.Query{String: "BEGIN;"},

				&pgproto3.Query{String: "SELECT 1 as a;"},

				&pgproto3.Bind{PreparedStatement: "pppp2"},
				&pgproto3.Execute{},
				&pgproto3.Sync{},

				&pgproto3.Query{String: "BEGIN;"},

				&pgproto3.Bind{PreparedStatement: "pppp_query"},
				&pgproto3.Execute{},
				&pgproto3.Bind{PreparedStatement: "pppp3"},
				&pgproto3.Execute{},
				&pgproto3.Sync{},
			},
			Response: []pgproto3.BackendMessage{
				&pgproto3.ParseComplete{},
				&pgproto3.ParseComplete{},
				&pgproto3.ParseComplete{},

				&pgproto3.ParseComplete{},

				&pgproto3.ReadyForQuery{
					TxStatus: byte(txstatus.TXIDLE),
				},

				&pgproto3.CommandComplete{
					CommandTag: []byte("BEGIN"),
				},
				&pgproto3.ReadyForQuery{TxStatus: byte(txstatus.TXACT)},

				&pgproto3.RowDescription{
					Fields: []pgproto3.FieldDescription{
						{
							Name:         []byte("a"),
							DataTypeOID:  23,
							DataTypeSize: 4,
							TypeModifier: -1,
						},
					},
				},
				&pgproto3.DataRow{
					Values: [][]byte{{0x31}},
				},
				&pgproto3.CommandComplete{
					CommandTag: []byte("SELECT 1"),
				},
				&pgproto3.ReadyForQuery{TxStatus: byte(txstatus.TXACT)},

				&pgproto3.BindComplete{},
				&pgproto3.CommandComplete{
					CommandTag: []byte("COMMIT"),
				},
				&pgproto3.ReadyForQuery{TxStatus: byte(txstatus.TXIDLE)},

				&pgproto3.CommandComplete{
					CommandTag: []byte("BEGIN"),
				},
				&pgproto3.ReadyForQuery{TxStatus: byte(txstatus.TXACT)},

				&pgproto3.BindComplete{},
				&pgproto3.DataRow{
					Values: [][]byte{{0x31}},
				},
				&pgproto3.CommandComplete{
					CommandTag: []byte("SELECT 1"),
				},
				&pgproto3.BindComplete{},
				&pgproto3.CommandComplete{
					CommandTag: []byte("ROLLBACK"),
				},
				&pgproto3.ReadyForQuery{TxStatus: byte(txstatus.TXIDLE)},
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
			case *pgproto3.ErrorResponse:
				/* skip */
				if retMsgType.Severity != "ERROR" {
					retMsg, err = frontend.Receive()
					assert.NoError(t, err)
				}
			case *pgproto3.NoticeResponse:
				retMsg, err = frontend.Receive()
				assert.NoError(t, err)
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
			assert.Equal(t, msg, retMsg, fmt.Sprintf("iter msg %d", ind))
		}
	}
}

func TestXProtoPureVirtual(t *testing.T) {
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

	for _, msgroup := range []MessageGroup{
		{
			Request: []pgproto3.FrontendMessage{
				&pgproto3.Parse{
					Query: "SELECT 1",
					Name:  "q1",
				},
				&pgproto3.Parse{
					Query: "SELECT pg_is_in_recovery()",
					Name:  "q2",
				},
				&pgproto3.Describe{
					ObjectType: 'S',
					Name:       "q2",
				},
				&pgproto3.Bind{
					PreparedStatement: "q2",
				},
				&pgproto3.Execute{},

				&pgproto3.Describe{
					ObjectType: 'S',
					Name:       "q1",
				},
				&pgproto3.Bind{
					PreparedStatement: "q1",
				},
				&pgproto3.Execute{},
				&pgproto3.Sync{},
			},
			Response: []pgproto3.BackendMessage{
				&pgproto3.ParseComplete{},
				&pgproto3.ParseComplete{},
				&pgproto3.ParameterDescription{ParameterOIDs: []uint32{}},
				&pgproto3.RowDescription{
					Fields: []pgproto3.FieldDescription{
						{
							Name:                 []byte("pg_is_in_recovery"),
							TableOID:             0x0,
							TableAttributeNumber: 0x0,
							DataTypeOID:          catalog.ARRAYOID,
							DataTypeSize:         1,
							TypeModifier:         -1,
							Format:               0,
						},
					},
				},
				&pgproto3.BindComplete{},
				&pgproto3.DataRow{
					Values: [][]byte{[]byte("f")},
				},

				&pgproto3.CommandComplete{CommandTag: []byte("SELECT 1")},
				&pgproto3.ParameterDescription{ParameterOIDs: []uint32{}},
				&pgproto3.RowDescription{
					Fields: []pgproto3.FieldDescription{
						{
							Name:                 []byte("?column?"),
							TableOID:             0x0,
							TableAttributeNumber: 0x0,
							DataTypeOID:          catalog.INT4OID,
							DataTypeSize:         4,
							TypeModifier:         -1,
							Format:               0,
						},
					},
				},
				&pgproto3.BindComplete{},
				&pgproto3.DataRow{
					Values: [][]byte{[]byte("1")},
				},

				&pgproto3.CommandComplete{CommandTag: []byte("SELECT 1")},
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
			case *pgproto3.ErrorResponse:
				/* skip */
				if retMsgType.Severity != "ERROR" {
					retMsg, err = frontend.Receive()
					assert.NoError(t, err)
				}
			case *pgproto3.NoticeResponse:
				retMsg, err = frontend.Receive()
				assert.NoError(t, err)
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
			assert.Equal(t, msg, retMsg, fmt.Sprintf("iter msg %d", ind))
		}
	}
}

func TestVirtualParams(t *testing.T) {
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

	for gr, msgroup := range []MessageGroup{
		{
			Request: []pgproto3.FrontendMessage{
				&pgproto3.Query{
					String: `SET __spqr__engine_v2 TO true;  SET __spqr__target_session_attrs TO "read-only"`,
				},
				&pgproto3.Query{String: `SHOW __spqr__engine_v2; SHOW __spqr__target_session_attrs;`},

				&pgproto3.Query{
					String: `SET __spqr__engine_v2 TO false; SET  __spqr__target_session_attrs TO "read-write"`,
				},
				&pgproto3.Query{String: `SHOW __spqr__engine_v2; SHOW __spqr__target_session_attrs;`},
			},
			Response: []pgproto3.BackendMessage{
				&pgproto3.CommandComplete{CommandTag: []byte("SET")},
				&pgproto3.CommandComplete{CommandTag: []byte("SET")},
				&pgproto3.ReadyForQuery{
					TxStatus: byte(txstatus.TXIDLE),
				},

				&pgproto3.RowDescription{
					Fields: []pgproto3.FieldDescription{
						{
							Name:         []byte("engine v2"),
							DataTypeOID:  catalog.TEXTOID,
							DataTypeSize: -1,
							TypeModifier: -1,
						},
					},
				},
				&pgproto3.DataRow{
					Values: [][]byte{[]byte("on")},
				},
				&pgproto3.CommandComplete{CommandTag: []byte("SHOW")},
				&pgproto3.RowDescription{
					Fields: []pgproto3.FieldDescription{
						{
							Name:         []byte("target session attrs"),
							DataTypeOID:  catalog.TEXTOID,
							DataTypeSize: -1,
							TypeModifier: -1,
						},
					},
				},
				&pgproto3.DataRow{
					Values: [][]byte{[]byte("read-only")},
				},
				&pgproto3.CommandComplete{CommandTag: []byte("SHOW")},
				&pgproto3.ReadyForQuery{
					TxStatus: byte(txstatus.TXIDLE),
				},

				&pgproto3.CommandComplete{CommandTag: []byte("SET")},
				&pgproto3.CommandComplete{CommandTag: []byte("SET")},
				&pgproto3.ReadyForQuery{
					TxStatus: byte(txstatus.TXIDLE),
				},

				&pgproto3.RowDescription{
					Fields: []pgproto3.FieldDescription{
						{
							Name:         []byte("engine v2"),
							DataTypeOID:  catalog.TEXTOID,
							DataTypeSize: -1,
							TypeModifier: -1,
						},
					},
				},
				&pgproto3.DataRow{
					Values: [][]byte{[]byte("off")},
				},
				&pgproto3.CommandComplete{CommandTag: []byte("SHOW")},
				&pgproto3.RowDescription{
					Fields: []pgproto3.FieldDescription{
						{
							Name:         []byte("target session attrs"),
							DataTypeOID:  catalog.TEXTOID,
							DataTypeSize: -1,
							TypeModifier: -1,
						},
					},
				},
				&pgproto3.DataRow{
					Values: [][]byte{[]byte("read-write")},
				},
				&pgproto3.CommandComplete{CommandTag: []byte("SHOW")},
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
			assert.Equal(t, msg, retMsg, fmt.Sprintf("group %d iter msg %d", gr, ind))
		}
	}
}
