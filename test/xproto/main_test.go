package prep_stmt_test

import (
	"context"
	"fmt"
	"net"
	"os"
	"testing"

	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/stretchr/testify/assert"
)

type MessageGroup struct {
	Request             []pgproto3.FrontendMessage
	Response            []pgproto3.BackendMessage
	CheckCodeOnly       bool
	CheckErrAll         bool
	SkipCheckCommandTag bool
}

func isPurePGTesting() bool {
	_, defined := os.LookupEnv("SPQR_XPROTO_TEST_PURE_PG_ONLY")
	return defined
}

func thisIsSPQRSpecificTest(t *testing.T) {
	if isPurePGTesting() {
		t.Skip("This test is incompatible with vanilla PG")
	}
}

func protoTestRunner(t *testing.T, frontend *pgproto3.Frontend, tt []MessageGroup) {
	for i := 0; i < 5; i++ {
		for _, msgroup := range tt {
			for _, msg := range msgroup.Request {
				frontend.Send(msg)
			}
		}
		_ = frontend.Flush()
		for gr, msgroup := range tt {
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
					retMsgType.Hint = ""
					retMsgType.Detail = ""

					retMsgType.SeverityUnlocalized = ""
					retMsgType.File = ""
					retMsgType.Where = ""
					if !msgroup.CheckErrAll {
						if msgroup.CheckCodeOnly {
							retMsgType.Message = ""
						} else {
							retMsgType.Code = ""
						}
					}
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
				case *pgproto3.CommandComplete:

					switch q := msg.(type) {
					case *pgproto3.CommandComplete:
						if q.CommandTag == nil {
							if msgroup.SkipCheckCommandTag {
								retMsgType.CommandTag = nil
							}
						}
					}
				default:
					break
				}
				if !assert.Equal(t, msg, retMsg, fmt.Sprintf("gr %d tc %d", gr, ind)) {
					t.FailNow()
				}
			}
		}
	}
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

	_, err = conn.Exec(context.Background(), "CREATE KEY RANGE krid6 FROM 2000 ROUTE TO sh4 FOR DISTRIBUTION ds1;")
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "could not setup sharding: %s\n", err)
	}

	_, err = conn.Exec(context.Background(), "CREATE KEY RANGE krid5 FROM 1000 ROUTE TO sh3 FOR DISTRIBUTION ds1;")
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

func bootstrapConnection(_ *testing.T) (*pgproto3.Frontend, net.Conn, error) {
	conn, err := getC()
	if err != nil {
		return nil, nil, err
	}

	frontend := pgproto3.NewFrontend(conn, conn)
	frontend.Send(&pgproto3.StartupMessage{
		ProtocolVersion: 196608,
		Parameters:      getConnectionParams(),
	})
	if err := frontend.Flush(); err != nil {
		return nil, nil, err
	}

	if err := waitRFQ(frontend); err != nil {
		return nil, nil, err
	}
	return frontend, conn, nil
}
