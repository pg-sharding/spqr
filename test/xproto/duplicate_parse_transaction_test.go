//go:build xproto || all

package prep_stmt_test

import (
	"os"
	"testing"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/pg-sharding/spqr/pkg/txstatus"
	"github.com/stretchr/testify/require"
)

func TestDuplicateParseUnsupportedSavepoint(t *testing.T) {
	if os.Getenv("ODYSSEY_XPROTO_VIRTUAL_TX") != "1" {
		t.Skip("Odyssey's virtual transaction state deliberately does not support savepoints")
	}
	frontend, conn, err := bootstrapConnection(t)
	require.NoError(t, err)
	defer conn.Close()

	protoTestRunner(t, frontend, []MessageGroup{
		{
			Request: []pgproto3.FrontendMessage{
				&pgproto3.Query{String: "BEGIN; SAVEPOINT s"},
				&pgproto3.Parse{Name: "duplicate_savepoint", Query: "SELECT 1"},
				&pgproto3.Parse{Name: "duplicate_savepoint", Query: "SELECT 2"},
				&pgproto3.Sync{},
			},
			CheckCodeOnly: true,
			Response: []pgproto3.BackendMessage{
				&pgproto3.CommandComplete{CommandTag: []byte("BEGIN")},
				&pgproto3.CommandComplete{CommandTag: []byte("SAVEPOINT")},
				&pgproto3.ReadyForQuery{TxStatus: byte(txstatus.TXACT)},
				&pgproto3.ParseComplete{},
				&pgproto3.ErrorResponse{Severity: "ERROR", Code: "42P05"},
				&pgproto3.ReadyForQuery{TxStatus: byte(txstatus.TXERR)},
			},
		},
		{
			CheckCodeOnly: true,
			Request: []pgproto3.FrontendMessage{
				&pgproto3.Query{String: "ROLLBACK TO SAVEPOINT s"},
				&pgproto3.Query{String: "SAVEPOINT other"},
				&pgproto3.Query{String: "COMMIT; SELECT 1"},
				&pgproto3.Parse{Name: "rollback_to_s", Query: "ROLLBACK TO s"},
				&pgproto3.Sync{},
				&pgproto3.Query{String: "ROLLBACK"},
				&pgproto3.Close{ObjectType: 'S', Name: "duplicate_savepoint"},
				&pgproto3.Sync{},
			},
			Response: []pgproto3.BackendMessage{
				&pgproto3.ErrorResponse{Severity: "ERROR", Code: "25P02"},
				&pgproto3.ReadyForQuery{TxStatus: byte(txstatus.TXERR)},
				&pgproto3.ErrorResponse{Severity: "ERROR", Code: "25P02"},
				&pgproto3.ReadyForQuery{TxStatus: byte(txstatus.TXERR)},
				&pgproto3.ErrorResponse{Severity: "ERROR", Code: "25P02"},
				&pgproto3.ReadyForQuery{TxStatus: byte(txstatus.TXERR)},
				&pgproto3.ErrorResponse{Severity: "ERROR", Code: "25P02"},
				&pgproto3.ReadyForQuery{TxStatus: byte(txstatus.TXERR)},
				&pgproto3.CommandComplete{CommandTag: []byte("ROLLBACK")},
				&pgproto3.ReadyForQuery{TxStatus: byte(txstatus.TXIDLE)},
				&pgproto3.CloseComplete{},
				&pgproto3.ReadyForQuery{TxStatus: byte(txstatus.TXIDLE)},
			},
		},
	})
}

func TestDuplicateParseSync(t *testing.T) {
	frontend, conn, err := bootstrapConnection(t)
	require.NoError(t, err)
	defer conn.Close()

	protoTestRunner(t, frontend, []MessageGroup{
		{
			CheckCodeOnly: true,
			Request: []pgproto3.FrontendMessage{
				&pgproto3.Parse{Name: "duplicate_sync", Query: "SELECT 1"},
				&pgproto3.Parse{Name: "duplicate_sync", Query: "SELECT 2"},
				&pgproto3.Parse{Name: "ignored_sync", Query: "SELECT 3"},
				&pgproto3.Sync{},
			},
			Response: []pgproto3.BackendMessage{
				&pgproto3.ParseComplete{},
				&pgproto3.ErrorResponse{Severity: "ERROR", Code: "42P05"},
				&pgproto3.ReadyForQuery{TxStatus: byte(txstatus.TXIDLE)},
			},
		},
		{
			Request: []pgproto3.FrontendMessage{
				&pgproto3.Parse{Name: "ignored_sync", Query: "SELECT 3"},
				&pgproto3.Close{ObjectType: 'S', Name: "ignored_sync"},
				&pgproto3.Close{ObjectType: 'S', Name: "duplicate_sync"},
				&pgproto3.Sync{},
			},
			Response: []pgproto3.BackendMessage{
				&pgproto3.ParseComplete{}, &pgproto3.CloseComplete{},
				&pgproto3.CloseComplete{},
				&pgproto3.ReadyForQuery{TxStatus: byte(txstatus.TXIDLE)},
			},
		},
	})
}

func TestDuplicateParseAfterFlushedExtendedBegin(t *testing.T) {
	for _, query := range []string{"BEGIN", "START TRANSACTION", "BEGIN ISOLATION LEVEL SERIALIZABLE"} {
		t.Run(query, func(t *testing.T) { testDuplicateAfterFlushedBegin(t, query) })
	}
}

func testDuplicateAfterFlushedBegin(t *testing.T, beginQuery string) {
	frontend, conn, err := bootstrapConnection(t)
	require.NoError(t, err)
	defer conn.Close()
	beginTag := "BEGIN"
	if beginQuery == "START TRANSACTION" {
		beginTag = "START TRANSACTION"
	}

	protoTestRunner(t, frontend, []MessageGroup{
		{
			Request: []pgproto3.FrontendMessage{
				&pgproto3.Parse{Name: "begin_flushed", Query: beginQuery},
				&pgproto3.Bind{PreparedStatement: "begin_flushed"},
				&pgproto3.Execute{},
				&pgproto3.Flush{},
			},
			Response: []pgproto3.BackendMessage{
				&pgproto3.ParseComplete{}, &pgproto3.BindComplete{},
				&pgproto3.CommandComplete{CommandTag: []byte(beginTag)},
			},
		},
		{
			CheckCodeOnly: true,
			Request: []pgproto3.FrontendMessage{
				&pgproto3.Parse{Name: "duplicate_flushed", Query: "SELECT 1"},
				&pgproto3.Parse{Name: "duplicate_flushed", Query: "SELECT 2"},
				&pgproto3.Sync{},
				&pgproto3.Query{String: "ROLLBACK"},
				&pgproto3.Close{ObjectType: 'S', Name: "begin_flushed"},
				&pgproto3.Close{ObjectType: 'S', Name: "duplicate_flushed"},
				&pgproto3.Sync{},
			},
			Response: []pgproto3.BackendMessage{
				&pgproto3.ParseComplete{},
				&pgproto3.ErrorResponse{Severity: "ERROR", Code: "42P05"},
				&pgproto3.ReadyForQuery{TxStatus: byte(txstatus.TXERR)},
				&pgproto3.CommandComplete{CommandTag: []byte("ROLLBACK")},
				&pgproto3.ReadyForQuery{TxStatus: byte(txstatus.TXIDLE)},
				&pgproto3.CloseComplete{}, &pgproto3.CloseComplete{},
				&pgproto3.ReadyForQuery{TxStatus: byte(txstatus.TXIDLE)},
			},
		},
	})
}

func TestDuplicateParseDescribeProcedureInAbortedTransaction(t *testing.T) {
	frontend, conn, err := bootstrapConnection(t)
	require.NoError(t, err)
	defer conn.Close()

	protoTestRunner(t, frontend, []MessageGroup{
		{
			Request: []pgproto3.FrontendMessage{
				&pgproto3.Query{String: "CREATE PROCEDURE duplicate_describe_proc(INOUT n int) LANGUAGE plpgsql AS 'BEGIN n := n + 1; END'"},
				&pgproto3.Parse{Name: "describe_call", Query: "CALL duplicate_describe_proc(1)"},
				&pgproto3.Sync{},
				&pgproto3.Query{String: "BEGIN"},
			},
			Response: []pgproto3.BackendMessage{
				&pgproto3.CommandComplete{CommandTag: []byte("CREATE PROCEDURE")},
				&pgproto3.ReadyForQuery{TxStatus: byte(txstatus.TXIDLE)},
				&pgproto3.ParseComplete{},
				&pgproto3.ReadyForQuery{TxStatus: byte(txstatus.TXIDLE)},
				&pgproto3.CommandComplete{CommandTag: []byte("BEGIN")},
				&pgproto3.ReadyForQuery{TxStatus: byte(txstatus.TXACT)},
			},
		},
		{
			CheckCodeOnly: true,
			Request: []pgproto3.FrontendMessage{
				&pgproto3.Parse{Name: "duplicate_call", Query: "SELECT 1"},
				&pgproto3.Parse{Name: "duplicate_call", Query: "SELECT 2"},
				&pgproto3.Sync{},
				&pgproto3.Describe{ObjectType: 'S', Name: "describe_call"},
				&pgproto3.Sync{},
				&pgproto3.Query{String: "ROLLBACK"},
				&pgproto3.Close{ObjectType: 'S', Name: "describe_call"},
				&pgproto3.Close{ObjectType: 'S', Name: "duplicate_call"},
				&pgproto3.Sync{},
				&pgproto3.Query{String: "DROP PROCEDURE duplicate_describe_proc(int)"},
			},
			Response: []pgproto3.BackendMessage{
				&pgproto3.ParseComplete{},
				&pgproto3.ErrorResponse{Severity: "ERROR", Code: "42P05"},
				&pgproto3.ReadyForQuery{TxStatus: byte(txstatus.TXERR)},
				&pgproto3.ErrorResponse{Severity: "ERROR", Code: "25P02"},
				&pgproto3.ReadyForQuery{TxStatus: byte(txstatus.TXERR)},
				&pgproto3.CommandComplete{CommandTag: []byte("ROLLBACK")},
				&pgproto3.ReadyForQuery{TxStatus: byte(txstatus.TXIDLE)},
				&pgproto3.CloseComplete{}, &pgproto3.CloseComplete{},
				&pgproto3.ReadyForQuery{TxStatus: byte(txstatus.TXIDLE)},
				&pgproto3.CommandComplete{CommandTag: []byte("DROP PROCEDURE")},
				&pgproto3.ReadyForQuery{TxStatus: byte(txstatus.TXIDLE)},
			},
		},
	})
}

func TestDuplicateParseTransaction(t *testing.T) {
	frontend, conn, err := bootstrapConnection(t)
	require.NoError(t, err)
	defer conn.Close()

	protoTestRunner(t, frontend, []MessageGroup{
		{
			Request: []pgproto3.FrontendMessage{&pgproto3.Query{String: "BEGIN; SAVEPOINT s"}},
			Response: []pgproto3.BackendMessage{
				&pgproto3.CommandComplete{CommandTag: []byte("BEGIN")},
				&pgproto3.CommandComplete{CommandTag: []byte("SAVEPOINT")},
				&pgproto3.ReadyForQuery{TxStatus: byte(txstatus.TXACT)},
			},
		},
		{
			CheckCodeOnly: true,
			Request: []pgproto3.FrontendMessage{
				&pgproto3.Parse{Name: "duplicate_xproto", Query: "SELECT 1"},
				&pgproto3.Parse{Name: "duplicate_xproto", Query: "SELECT 2"},
				&pgproto3.Parse{Name: "ignored_xproto", Query: "SELECT 3"},
				&pgproto3.Sync{},
			},
			Response: []pgproto3.BackendMessage{
				&pgproto3.ParseComplete{},
				&pgproto3.ErrorResponse{Severity: "ERROR", Code: "42P05"},
				&pgproto3.ReadyForQuery{TxStatus: byte(txstatus.TXERR)},
			},
		},
		{
			CheckCodeOnly: true,
			Request:       []pgproto3.FrontendMessage{&pgproto3.Query{String: "SELECT 1"}},
			Response: []pgproto3.BackendMessage{
				&pgproto3.ErrorResponse{Severity: "ERROR", Code: "25P02"},
				&pgproto3.ReadyForQuery{TxStatus: byte(txstatus.TXERR)},
			},
		},
		{
			CheckCodeOnly: true,
			Request: []pgproto3.FrontendMessage{
				&pgproto3.Sync{},
				&pgproto3.Parse{Name: "aborted_parse", Query: "SELECT 1"},
				&pgproto3.Sync{},
				&pgproto3.Bind{PreparedStatement: "duplicate_xproto"},
				&pgproto3.Sync{},
				&pgproto3.Describe{ObjectType: 'S', Name: "duplicate_xproto"},
				&pgproto3.Sync{},
			},
			Response: []pgproto3.BackendMessage{
				&pgproto3.ReadyForQuery{TxStatus: byte(txstatus.TXERR)},
				&pgproto3.ErrorResponse{Severity: "ERROR", Code: "25P02"},
				&pgproto3.ReadyForQuery{TxStatus: byte(txstatus.TXERR)},
				&pgproto3.ErrorResponse{Severity: "ERROR", Code: "25P02"},
				&pgproto3.ReadyForQuery{TxStatus: byte(txstatus.TXERR)},
				&pgproto3.ErrorResponse{Severity: "ERROR", Code: "25P02"},
				&pgproto3.ReadyForQuery{TxStatus: byte(txstatus.TXERR)},
			},
		},
		{
			Request: []pgproto3.FrontendMessage{
				&pgproto3.Parse{Name: "rollback_stmt", Query: "ROLLBACK"},
				&pgproto3.Bind{PreparedStatement: "rollback_stmt"},
				&pgproto3.Execute{},
				&pgproto3.Sync{},
			},
			Response: []pgproto3.BackendMessage{
				&pgproto3.ParseComplete{}, &pgproto3.BindComplete{},
				&pgproto3.CommandComplete{CommandTag: []byte("ROLLBACK")},
				&pgproto3.ReadyForQuery{TxStatus: byte(txstatus.TXIDLE)},
			},
		},
		{
			Request: []pgproto3.FrontendMessage{
				&pgproto3.Parse{Name: "ignored_xproto", Query: "SELECT 3"},
				&pgproto3.Close{ObjectType: 'S', Name: "ignored_xproto"},
				&pgproto3.Close{ObjectType: 'S', Name: "duplicate_xproto"},
				&pgproto3.Close{ObjectType: 'S', Name: "rollback_stmt"},
				&pgproto3.Sync{},
			},
			Response: []pgproto3.BackendMessage{
				&pgproto3.ParseComplete{}, &pgproto3.CloseComplete{},
				&pgproto3.CloseComplete{},
				&pgproto3.CloseComplete{},
				&pgproto3.ReadyForQuery{TxStatus: byte(txstatus.TXIDLE)},
			},
		},
	})
}

func TestDuplicateParseRollsBackImplicitTransaction(t *testing.T) {
	frontend, conn, err := bootstrapConnection(t)
	require.NoError(t, err)
	defer conn.Close()

	protoTestRunner(t, frontend, []MessageGroup{
		{
			Request: []pgproto3.FrontendMessage{&pgproto3.Query{String: "CREATE TABLE duplicate_xproto_tx (v int)"}},
			Response: []pgproto3.BackendMessage{
				&pgproto3.CommandComplete{CommandTag: []byte("CREATE TABLE")},
				&pgproto3.ReadyForQuery{TxStatus: byte(txstatus.TXIDLE)},
			},
		},
		{
			CheckCodeOnly: true,
			Request: []pgproto3.FrontendMessage{
				&pgproto3.Parse{Name: "duplicate_insert", Query: "INSERT INTO duplicate_xproto_tx VALUES (1)"},
				&pgproto3.Bind{PreparedStatement: "duplicate_insert"},
				&pgproto3.Execute{},
				&pgproto3.Parse{Name: "duplicate_insert", Query: "SELECT 2"},
				&pgproto3.Sync{},
			},
			Response: []pgproto3.BackendMessage{
				&pgproto3.ParseComplete{}, &pgproto3.BindComplete{},
				&pgproto3.CommandComplete{CommandTag: []byte("INSERT 0 1")},
				&pgproto3.ErrorResponse{Severity: "ERROR", Code: "42P05"},
				&pgproto3.ReadyForQuery{TxStatus: byte(txstatus.TXIDLE)},
			},
		},
		{
			Request: []pgproto3.FrontendMessage{
				&pgproto3.Close{ObjectType: 'S', Name: "duplicate_insert"},
				&pgproto3.Sync{},
				&pgproto3.Query{String: "SELECT count(*)::int AS n FROM duplicate_xproto_tx"},
				&pgproto3.Query{String: "DROP TABLE duplicate_xproto_tx"},
			},
			Response: []pgproto3.BackendMessage{
				&pgproto3.CloseComplete{},
				&pgproto3.ReadyForQuery{TxStatus: byte(txstatus.TXIDLE)},
				&pgproto3.RowDescription{Fields: []pgproto3.FieldDescription{{Name: []byte("n"), DataTypeOID: 23, DataTypeSize: 4, TypeModifier: -1}}},
				&pgproto3.DataRow{Values: [][]byte{[]byte("0")}},
				&pgproto3.CommandComplete{CommandTag: []byte("SELECT 1")},
				&pgproto3.ReadyForQuery{TxStatus: byte(txstatus.TXIDLE)},
				&pgproto3.CommandComplete{CommandTag: []byte("DROP TABLE")},
				&pgproto3.ReadyForQuery{TxStatus: byte(txstatus.TXIDLE)},
			},
		},
	})
}

func TestDuplicateParseCommitRollsBackWrites(t *testing.T) {
	frontend, conn, err := bootstrapConnection(t)
	require.NoError(t, err)
	defer conn.Close()

	protoTestRunner(t, frontend, []MessageGroup{
		{
			CheckCodeOnly: true,
			Request: []pgproto3.FrontendMessage{
				&pgproto3.Query{String: "CREATE TABLE duplicate_commit_tx (v int)"},
				&pgproto3.Query{String: "BEGIN; INSERT INTO duplicate_commit_tx VALUES (1)"},
				&pgproto3.Parse{Name: "duplicate_commit_write", Query: "SELECT 1"},
				&pgproto3.Parse{Name: "duplicate_commit_write", Query: "SELECT 2"},
				&pgproto3.Sync{},
				&pgproto3.Query{String: "COMMIT"},
				&pgproto3.Query{String: "SELECT count(*)::int AS n FROM duplicate_commit_tx"},
				&pgproto3.Close{ObjectType: 'S', Name: "duplicate_commit_write"},
				&pgproto3.Sync{},
				&pgproto3.Query{String: "DROP TABLE duplicate_commit_tx"},
			},
			Response: []pgproto3.BackendMessage{
				&pgproto3.CommandComplete{CommandTag: []byte("CREATE TABLE")},
				&pgproto3.ReadyForQuery{TxStatus: byte(txstatus.TXIDLE)},
				&pgproto3.CommandComplete{CommandTag: []byte("BEGIN")},
				&pgproto3.CommandComplete{CommandTag: []byte("INSERT 0 1")},
				&pgproto3.ReadyForQuery{TxStatus: byte(txstatus.TXACT)},
				&pgproto3.ParseComplete{},
				&pgproto3.ErrorResponse{Severity: "ERROR", Code: "42P05"},
				&pgproto3.ReadyForQuery{TxStatus: byte(txstatus.TXERR)},
				&pgproto3.CommandComplete{CommandTag: []byte("ROLLBACK")},
				&pgproto3.ReadyForQuery{TxStatus: byte(txstatus.TXIDLE)},
				&pgproto3.RowDescription{Fields: []pgproto3.FieldDescription{{Name: []byte("n"), DataTypeOID: 23, DataTypeSize: 4, TypeModifier: -1}}},
				&pgproto3.DataRow{Values: [][]byte{[]byte("0")}},
				&pgproto3.CommandComplete{CommandTag: []byte("SELECT 1")},
				&pgproto3.ReadyForQuery{TxStatus: byte(txstatus.TXIDLE)},
				&pgproto3.CloseComplete{},
				&pgproto3.ReadyForQuery{TxStatus: byte(txstatus.TXIDLE)},
				&pgproto3.CommandComplete{CommandTag: []byte("DROP TABLE")},
				&pgproto3.ReadyForQuery{TxStatus: byte(txstatus.TXIDLE)},
			},
		},
	})
}

func TestDuplicateParseKeepsBackendAcrossCommitChain(t *testing.T) {
	frontend, conn, err := bootstrapConnection(t)
	require.NoError(t, err)
	defer conn.Close()

	send := func(messages ...pgproto3.FrontendMessage) {
		for _, message := range messages {
			frontend.Send(message)
		}
		require.NoError(t, frontend.Flush())
	}
	receive := func() pgproto3.BackendMessage {
		message, err := frontend.Receive()
		require.NoError(t, err)
		return message
	}
	readPID := func() string {
		require.IsType(t, &pgproto3.RowDescription{}, receive())
		row, ok := receive().(*pgproto3.DataRow)
		require.True(t, ok)
		require.Len(t, row.Values, 1)
		require.IsType(t, &pgproto3.CommandComplete{}, receive())
		return string(row.Values[0])
	}

	send(&pgproto3.Query{String: "BEGIN; SELECT pg_backend_pid()"})
	require.IsType(t, &pgproto3.CommandComplete{}, receive())
	before := readPID()
	require.Equal(t, byte(txstatus.TXACT), receive().(*pgproto3.ReadyForQuery).TxStatus)
	send(&pgproto3.Parse{Name: "duplicate_pid", Query: "SELECT 1"},
		&pgproto3.Parse{Name: "duplicate_pid", Query: "SELECT 2"}, &pgproto3.Sync{})
	require.IsType(t, &pgproto3.ParseComplete{}, receive())
	errResponse, ok := receive().(*pgproto3.ErrorResponse)
	require.True(t, ok)
	require.Equal(t, "42P05", errResponse.Code)
	require.Equal(t, byte(txstatus.TXERR), receive().(*pgproto3.ReadyForQuery).TxStatus)
	send(&pgproto3.Query{String: "COMMIT AND CHAIN"})
	require.Equal(t, "ROLLBACK", string(receive().(*pgproto3.CommandComplete).CommandTag))
	require.Equal(t, byte(txstatus.TXACT), receive().(*pgproto3.ReadyForQuery).TxStatus)
	send(&pgproto3.Query{String: "SELECT pg_backend_pid()"})
	require.Equal(t, before, readPID())
	require.Equal(t, byte(txstatus.TXACT), receive().(*pgproto3.ReadyForQuery).TxStatus)
	send(&pgproto3.Query{String: "ROLLBACK"})
	require.IsType(t, &pgproto3.CommandComplete{}, receive())
	require.Equal(t, byte(txstatus.TXIDLE), receive().(*pgproto3.ReadyForQuery).TxStatus)
}

func TestDuplicateParseFlushBeforeSync(t *testing.T) {
	frontend, conn, err := bootstrapConnection(t)
	require.NoError(t, err)
	defer conn.Close()

	protoTestRunner(t, frontend, []MessageGroup{
		{
			Request: []pgproto3.FrontendMessage{&pgproto3.Query{String: "BEGIN"}},
			Response: []pgproto3.BackendMessage{
				&pgproto3.CommandComplete{CommandTag: []byte("BEGIN")},
				&pgproto3.ReadyForQuery{TxStatus: byte(txstatus.TXACT)},
			},
		},
		{
			CheckCodeOnly: true,
			Request: []pgproto3.FrontendMessage{
				&pgproto3.Parse{Name: "duplicate_flush", Query: "SELECT 1"},
				&pgproto3.Parse{Name: "duplicate_flush", Query: "SELECT 2"},
				&pgproto3.Flush{},
			},
			Response: []pgproto3.BackendMessage{
				&pgproto3.ParseComplete{},
				&pgproto3.ErrorResponse{Severity: "ERROR", Code: "42P05"},
			},
		},
		{
			Request: []pgproto3.FrontendMessage{
				&pgproto3.Query{String: "SELECT 1"},
				&pgproto3.Parse{Name: "ignored_flush", Query: "SELECT 3"},
				&pgproto3.Sync{},
				&pgproto3.Query{String: "COMMIT"},
			},
			Response: []pgproto3.BackendMessage{
				&pgproto3.ReadyForQuery{TxStatus: byte(txstatus.TXERR)},
				&pgproto3.CommandComplete{CommandTag: []byte("ROLLBACK")},
				&pgproto3.ReadyForQuery{TxStatus: byte(txstatus.TXIDLE)},
			},
		},
		{
			Request: []pgproto3.FrontendMessage{
				&pgproto3.Parse{Name: "ignored_flush", Query: "SELECT 3"},
				&pgproto3.Close{ObjectType: 'S', Name: "ignored_flush"},
				&pgproto3.Close{ObjectType: 'S', Name: "duplicate_flush"},
				&pgproto3.Sync{},
			},
			Response: []pgproto3.BackendMessage{
				&pgproto3.ParseComplete{}, &pgproto3.CloseComplete{},
				&pgproto3.CloseComplete{},
				&pgproto3.ReadyForQuery{TxStatus: byte(txstatus.TXIDLE)},
			},
		},
	})
}

func TestDuplicateParseExtendedCommit(t *testing.T) {
	frontend, conn, err := bootstrapConnection(t)
	require.NoError(t, err)
	defer conn.Close()

	protoTestRunner(t, frontend, []MessageGroup{
		{
			Request: []pgproto3.FrontendMessage{&pgproto3.Query{String: "BEGIN"}},
			Response: []pgproto3.BackendMessage{
				&pgproto3.CommandComplete{CommandTag: []byte("BEGIN")},
				&pgproto3.ReadyForQuery{TxStatus: byte(txstatus.TXACT)},
			},
		},
		{
			CheckCodeOnly: true,
			Request: []pgproto3.FrontendMessage{
				&pgproto3.Parse{Name: "duplicate_commit", Query: "SELECT 1"},
				&pgproto3.Parse{Name: "duplicate_commit", Query: "SELECT 2"},
				&pgproto3.Sync{},
			},
			Response: []pgproto3.BackendMessage{
				&pgproto3.ParseComplete{},
				&pgproto3.ErrorResponse{Severity: "ERROR", Code: "42P05"},
				&pgproto3.ReadyForQuery{TxStatus: byte(txstatus.TXERR)},
			},
		},
		{
			Request: []pgproto3.FrontendMessage{
				&pgproto3.Parse{Name: "extended_commit", Query: "COMMIT"},
				&pgproto3.Describe{ObjectType: 'S', Name: "extended_commit"},
				&pgproto3.Bind{PreparedStatement: "extended_commit"},
				&pgproto3.Describe{ObjectType: 'P'},
				&pgproto3.Execute{},
				&pgproto3.Sync{},
			},
			Response: []pgproto3.BackendMessage{
				&pgproto3.ParseComplete{}, &pgproto3.ParameterDescription{ParameterOIDs: []uint32{}},
				&pgproto3.NoData{}, &pgproto3.BindComplete{}, &pgproto3.NoData{},
				&pgproto3.CommandComplete{CommandTag: []byte("ROLLBACK")},
				&pgproto3.ReadyForQuery{TxStatus: byte(txstatus.TXIDLE)},
			},
		},
		{
			Request: []pgproto3.FrontendMessage{
				&pgproto3.Close{ObjectType: 'S', Name: "duplicate_commit"},
				&pgproto3.Close{ObjectType: 'S', Name: "extended_commit"},
				&pgproto3.Sync{},
			},
			Response: []pgproto3.BackendMessage{
				&pgproto3.CloseComplete{}, &pgproto3.CloseComplete{},
				&pgproto3.ReadyForQuery{TxStatus: byte(txstatus.TXIDLE)},
			},
		},
	})
}

func TestDuplicateParseAfterExtendedBegin(t *testing.T) {
	frontend, conn, err := bootstrapConnection(t)
	require.NoError(t, err)
	defer conn.Close()

	protoTestRunner(t, frontend, []MessageGroup{
		{
			CheckCodeOnly: true,
			Request: []pgproto3.FrontendMessage{
				&pgproto3.Parse{Name: "begin_extended", Query: "BEGIN"},
				&pgproto3.Bind{PreparedStatement: "begin_extended"},
				&pgproto3.Execute{},
				&pgproto3.Parse{Name: "duplicate_begin", Query: "SELECT 1"},
				&pgproto3.Parse{Name: "duplicate_begin", Query: "SELECT 2"},
				&pgproto3.Sync{},
			},
			Response: []pgproto3.BackendMessage{
				&pgproto3.ParseComplete{}, &pgproto3.BindComplete{},
				&pgproto3.CommandComplete{CommandTag: []byte("BEGIN")},
				&pgproto3.ParseComplete{},
				&pgproto3.ErrorResponse{Severity: "ERROR", Code: "42P05"},
				&pgproto3.ReadyForQuery{TxStatus: byte(txstatus.TXERR)},
			},
		},
		{
			Request: []pgproto3.FrontendMessage{
				&pgproto3.Query{String: "ROLLBACK"},
				&pgproto3.Close{ObjectType: 'S', Name: "begin_extended"},
				&pgproto3.Close{ObjectType: 'S', Name: "duplicate_begin"},
				&pgproto3.Sync{},
			},
			Response: []pgproto3.BackendMessage{
				&pgproto3.CommandComplete{CommandTag: []byte("ROLLBACK")},
				&pgproto3.ReadyForQuery{TxStatus: byte(txstatus.TXIDLE)},
				&pgproto3.CloseComplete{}, &pgproto3.CloseComplete{},
				&pgproto3.ReadyForQuery{TxStatus: byte(txstatus.TXIDLE)},
			},
		},
	})
}

func TestDuplicateParseExistingCommitPortal(t *testing.T) {
	frontend, conn, err := bootstrapConnection(t)
	require.NoError(t, err)
	defer conn.Close()

	protoTestRunner(t, frontend, []MessageGroup{
		{
			Request: []pgproto3.FrontendMessage{
				&pgproto3.Query{String: "BEGIN"},
				&pgproto3.Parse{Name: "prior_commit", Query: "COMMIT"},
				&pgproto3.Bind{DestinationPortal: "prior_portal", PreparedStatement: "prior_commit"},
				&pgproto3.Sync{},
			},
			Response: []pgproto3.BackendMessage{
				&pgproto3.CommandComplete{CommandTag: []byte("BEGIN")},
				&pgproto3.ReadyForQuery{TxStatus: byte(txstatus.TXACT)},
				&pgproto3.ParseComplete{}, &pgproto3.BindComplete{},
				&pgproto3.ReadyForQuery{TxStatus: byte(txstatus.TXACT)},
			},
		},
		{
			CheckCodeOnly: true,
			Request: []pgproto3.FrontendMessage{
				&pgproto3.Parse{Name: "duplicate_prior", Query: "SELECT 1"},
				&pgproto3.Parse{Name: "duplicate_prior", Query: "SELECT 2"},
				&pgproto3.Sync{},
				&pgproto3.Execute{Portal: "prior_portal"},
				&pgproto3.Sync{},
			},
			Response: []pgproto3.BackendMessage{
				&pgproto3.ParseComplete{},
				&pgproto3.ErrorResponse{Severity: "ERROR", Code: "42P05"},
				&pgproto3.ReadyForQuery{TxStatus: byte(txstatus.TXERR)},
				&pgproto3.ErrorResponse{Severity: "ERROR", Code: "25P02"},
				&pgproto3.ReadyForQuery{TxStatus: byte(txstatus.TXERR)},
			},
		},
		{
			Request: []pgproto3.FrontendMessage{
				&pgproto3.Query{String: "ROLLBACK"},
				&pgproto3.Close{ObjectType: 'S', Name: "prior_commit"},
				&pgproto3.Close{ObjectType: 'S', Name: "duplicate_prior"},
				&pgproto3.Sync{},
			},
			Response: []pgproto3.BackendMessage{
				&pgproto3.CommandComplete{CommandTag: []byte("ROLLBACK")},
				&pgproto3.ReadyForQuery{TxStatus: byte(txstatus.TXIDLE)},
				&pgproto3.CloseComplete{}, &pgproto3.CloseComplete{},
				&pgproto3.ReadyForQuery{TxStatus: byte(txstatus.TXIDLE)},
			},
		},
	})
}
