//go:build simple || all

package prep_stmt_test

import (
	"fmt"
	"testing"

	"github.com/pg-sharding/spqr/pkg/catalog"
	"github.com/pg-sharding/spqr/pkg/models/spqrerror"
	"github.com/pg-sharding/spqr/pkg/txstatus"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/stretchr/testify/assert"
)

func TestSimpleQuery(t *testing.T) {

	frontend, conn, err := bootstrapConnection(t)
	assert.NoError(t, err, "startup failed")

	defer func() {
		_ = conn.Close()
	}()

	for _, msgroup := range []MessageGroup{
		{

			Request: []pgproto3.FrontendMessage{
				&pgproto3.Query{
					String: "BEGIN",
				},
				&pgproto3.Query{
					String: "BEGIN",
				},

				&pgproto3.Query{
					String: "ROLLBACK",
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

				&pgproto3.NoticeResponse{
					Severity: "WARNING",
					Message:  "there is already a transaction in progress",
					Code:     spqrerror.PG_ACTIVE_SQL_TRANSACTION,
				},

				&pgproto3.CommandComplete{
					CommandTag: []byte("BEGIN"),
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

				&pgproto3.NoticeResponse{
					Severity: "WARNING",
					Message:  "there is no transaction in progress",
					Code:     spqrerror.PG_NO_ACTIVE_SQL_TRANSACTION,
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
			case *pgproto3.NoticeResponse:
				retMsgType.File = ""
				retMsgType.Line = 0
				retMsgType.SeverityUnlocalized = ""
				retMsgType.Routine = ""
				retMsgType.Detail = ""
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
	thisIsSPQRSpecificTest(t)

	frontend, conn, err := bootstrapConnection(t)
	assert.NoError(t, err, "startup failed")

	defer func() {
		_ = conn.Close()
	}()

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

func TestSimpleMixedPreparedStmt(t *testing.T) {
	thisIsSPQRSpecificTest(t)

	frontend, conn, err := bootstrapConnection(t)
	assert.NoError(t, err, "startup failed")

	defer func() {
		_ = conn.Close()
	}()

	tt := []MessageGroup{
		{
			Request: []pgproto3.FrontendMessage{

				&pgproto3.Query{
					String: "PREPARE p1 AS SELECT round(4+ 2);",
				},

				&pgproto3.Bind{
					PreparedStatement: "p1",
				},
				&pgproto3.Execute{},
				&pgproto3.Sync{},
			},
			Response: []pgproto3.BackendMessage{
				&pgproto3.CommandComplete{
					CommandTag: []byte("PREPARE"),
				},

				&pgproto3.ReadyForQuery{
					TxStatus: byte(txstatus.TXIDLE),
				},

				&pgproto3.BindComplete{},

				&pgproto3.DataRow{
					Values: [][]byte{[]byte("6")},
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

	protoTestRunner(t, frontend, tt)
}

func TestSimpleReferenceRelationAutoinc(t *testing.T) {
	thisIsSPQRSpecificTest(t)

	frontend, conn, err := bootstrapConnection(t)
	assert.NoError(t, err, "startup failed")

	defer func() {
		_ = conn.Close()
	}()

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
					CommandTag: []byte("SELECT 2"),
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

	frontend, conn, err := bootstrapConnection(t)
	assert.NoError(t, err, "startup failed")

	defer func() {
		_ = conn.Close()
	}()

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
	thisIsSPQRSpecificTest(t)

	frontend, conn, err := bootstrapConnection(t)
	assert.NoError(t, err, "startup failed")

	defer func() {
		_ = conn.Close()
	}()

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

func TestMixedProtoTxcommands(t *testing.T) {

	frontend, conn, err := bootstrapConnection(t)
	assert.NoError(t, err, "startup failed")

	defer func() {
		_ = conn.Close()
	}()

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

func TestVirtualParams(t *testing.T) {
	thisIsSPQRSpecificTest(t)

	frontend, conn, err := bootstrapConnection(t)
	assert.NoError(t, err, "startup failed")

	defer func() {
		_ = conn.Close()
	}()

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
