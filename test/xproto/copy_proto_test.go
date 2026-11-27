//go:build copy || all

package prep_stmt_test

import (
	"testing"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/pg-sharding/spqr/pkg/txstatus"
	"github.com/stretchr/testify/assert"
)

func TestCopySimple(t *testing.T) {
	frontend, conn, err := bootstrapConnection(t)
	assert.NoError(t, err, "startup failed")

	defer func() {
		_ = conn.Close()
	}()

	tt := []MessageGroup{
		{
			Request: []pgproto3.FrontendMessage{
				&pgproto3.Query{
					String: "COPY t(id) FROM STDIN",
				},

				&pgproto3.CopyDone{},
			},
			Response: []pgproto3.BackendMessage{
				&pgproto3.CopyInResponse{
					ColumnFormatCodes: []uint16{0},
				},
				&pgproto3.CommandComplete{
					CommandTag: []byte("COPY 0"),
				},
				&pgproto3.ReadyForQuery{
					TxStatus: byte(txstatus.TXIDLE),
				},
			},
		},
	}

	protoTestRunner(t, frontend, tt)
}

func TestCopySimpleData(t *testing.T) {
	for _, tc := range copyDataTestCases() {
		t.Run(tc.name, func(t *testing.T) {
			frontend, conn, err := bootstrapConnection(t)
			assert.NoError(t, err, "startup failed")
			defer func() {
				_ = conn.Close()
			}()

			deleteCopiedRows(t, frontend, tc.cleanup)
			sendCopyMessages(t, frontend, &pgproto3.Query{String: tc.query})
			expectCopyMessage(t, frontend, &pgproto3.CopyInResponse{ColumnFormatCodes: []uint16{0}})
			for _, data := range tc.chunks {
				sendCopyMessages(t, frontend, &pgproto3.CopyData{Data: []byte(data)})
			}
			sendCopyMessages(t, frontend, &pgproto3.CopyDone{})
			expectCopyMessage(t, frontend, &pgproto3.CommandComplete{CommandTag: []byte("COPY 2")})
			expectCopyMessage(t, frontend, &pgproto3.ReadyForQuery{TxStatus: byte(txstatus.TXIDLE)})
			assert.Equal(t, []byte("DELETE 2"), deleteCopiedRows(t, frontend, tc.cleanup))
		})
	}
}

func TestCopyExtendedData(t *testing.T) {
	for _, tc := range copyDataTestCases() {
		t.Run(tc.name, func(t *testing.T) {
			frontend, conn, err := bootstrapConnection(t)
			assert.NoError(t, err, "startup failed")
			defer func() {
				_ = conn.Close()
			}()

			deleteCopiedRows(t, frontend, tc.cleanup)
			sendCopyMessages(t, frontend,
				&pgproto3.Parse{Query: tc.query},
				&pgproto3.Bind{PreparedStatement: ""},
				&pgproto3.Execute{},
			)
			expectCopyMessage(t, frontend, &pgproto3.ParseComplete{})
			expectCopyMessage(t, frontend, &pgproto3.BindComplete{})
			expectCopyMessage(t, frontend, &pgproto3.CopyInResponse{ColumnFormatCodes: []uint16{0}})
			for _, data := range tc.chunks {
				sendCopyMessages(t, frontend, &pgproto3.CopyData{Data: []byte(data)})
			}
			sendCopyMessages(t, frontend, &pgproto3.CopyDone{}, &pgproto3.Sync{})
			expectCopyMessage(t, frontend, &pgproto3.CommandComplete{CommandTag: []byte("COPY 2")})
			expectCopyMessage(t, frontend, &pgproto3.ReadyForQuery{TxStatus: byte(txstatus.TXIDLE)})
			assert.Equal(t, []byte("DELETE 2"), deleteCopiedRows(t, frontend, tc.cleanup))
		})
	}
}

func copyDataTestCases() []struct {
	name    string
	query   string
	chunks  []string
	cleanup string
} {
	return []struct {
		name    string
		query   string
		chunks  []string
		cleanup string
	}{
		{
			name:    "single shard",
			query:   "COPY t(id) FROM STDIN /* __spqr__execute_on: sh1 */",
			chunks:  []string{"9\n1", "0\n"},
			cleanup: "DELETE FROM t WHERE id IN (9, 10) /* __spqr__execute_on: sh1 */",
		},
		{
			name:    "distributed",
			query:   "COPY t(id) FROM STDIN",
			chunks:  []string{"2100\n210", "1\n"},
			cleanup: "DELETE FROM t WHERE id IN (2100, 2101)",
		},
	}
}

func TestCopyExtendedExecuteSyncCopyDoneSync(t *testing.T) {
	frontend, conn, err := bootstrapConnection(t)
	assert.NoError(t, err, "startup failed")

	defer func() {
		_ = conn.Close()
	}()

	tt := []MessageGroup{
		{
			Request: []pgproto3.FrontendMessage{
				&pgproto3.Parse{
					Name:  "",
					Query: "COPY t(id) FROM STDIN",
				},
				&pgproto3.Bind{
					PreparedStatement: "",
				},
				&pgproto3.Execute{},
				&pgproto3.Sync{},

				&pgproto3.CopyDone{},
				&pgproto3.Sync{},
			},
			Response: []pgproto3.BackendMessage{

				&pgproto3.ParseComplete{},
				&pgproto3.BindComplete{},
				&pgproto3.CopyInResponse{
					ColumnFormatCodes: []uint16{0},
				},
				&pgproto3.CommandComplete{
					CommandTag: []byte("COPY 0"),
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
					Query: "COPY t(id) FROM STDIN",
				},
				&pgproto3.Bind{
					PreparedStatement: "",
				},
				&pgproto3.Execute{},
				&pgproto3.Sync{},
				&pgproto3.Sync{},
				&pgproto3.Sync{},

				&pgproto3.Sync{},
				&pgproto3.Sync{},

				&pgproto3.CopyDone{},
				&pgproto3.Sync{},
			},
			Response: []pgproto3.BackendMessage{

				&pgproto3.ParseComplete{},
				&pgproto3.BindComplete{},
				&pgproto3.CopyInResponse{
					ColumnFormatCodes: []uint16{0},
				},
				&pgproto3.CommandComplete{
					CommandTag: []byte("COPY 0"),
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
					Query: "COPY t(id) FROM STDIN",
				},
				&pgproto3.Bind{
					PreparedStatement: "",
				},
				&pgproto3.Execute{},
				&pgproto3.Sync{},

				&pgproto3.CopyFail{},
				&pgproto3.Sync{},
			},
			Response: []pgproto3.BackendMessage{
				&pgproto3.ParseComplete{},
				&pgproto3.BindComplete{},
				&pgproto3.CopyInResponse{
					ColumnFormatCodes: []uint16{0},
				},
				&pgproto3.ErrorResponse{
					Severity: "ERROR",
					Message:  "COPY from stdin failed: ",
				},
				&pgproto3.ReadyForQuery{
					TxStatus: byte(txstatus.TXIDLE),
				},
			},
		},
	}

	protoTestRunner(t, frontend, tt)
}

func TestCopyExtendedExecuteCopyDoneSync(t *testing.T) {
	frontend, conn, err := bootstrapConnection(t)
	assert.NoError(t, err, "startup failed")

	defer func() {
		_ = conn.Close()
	}()

	tt := []MessageGroup{
		{
			Request: []pgproto3.FrontendMessage{
				&pgproto3.Parse{
					Name:  "",
					Query: "COPY t(id) FROM STDIN",
				},
				&pgproto3.Bind{
					PreparedStatement: "",
				},
				&pgproto3.Execute{},

				&pgproto3.CopyDone{},
				&pgproto3.Sync{},
			},
			Response: []pgproto3.BackendMessage{
				&pgproto3.ParseComplete{},
				&pgproto3.BindComplete{},
				&pgproto3.CopyInResponse{
					ColumnFormatCodes: []uint16{0},
				},
				&pgproto3.CommandComplete{
					CommandTag: []byte("COPY 0"),
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
					Query: "COPY t(id) FROM STDIN",
				},
				&pgproto3.Bind{
					PreparedStatement: "",
				},
				&pgproto3.Execute{},

				&pgproto3.CopyFail{},
				&pgproto3.Sync{},
			},
			Response: []pgproto3.BackendMessage{
				&pgproto3.ParseComplete{},
				&pgproto3.BindComplete{},
				&pgproto3.CopyInResponse{
					ColumnFormatCodes: []uint16{0},
				},
				&pgproto3.ErrorResponse{
					Severity: "ERROR",
					Message:  "COPY from stdin failed: ",
				},
				&pgproto3.ReadyForQuery{
					TxStatus: byte(txstatus.TXIDLE),
				},
			},
		},
	}

	protoTestRunner(t, frontend, tt)
}

func sendCopyMessages(t *testing.T, frontend *pgproto3.Frontend, messages ...pgproto3.FrontendMessage) {
	t.Helper()
	for _, message := range messages {
		frontend.Send(message)
	}
	if err := frontend.Flush(); err != nil {
		t.Fatal(err)
	}
}

func expectCopyMessage(t *testing.T, frontend *pgproto3.Frontend, expected pgproto3.BackendMessage) {
	t.Helper()
	actual, err := frontend.Receive()
	if !assert.NoError(t, err) {
		t.FailNow()
	}
	assert.Equal(t, expected, actual)
}

func deleteCopiedRows(t *testing.T, frontend *pgproto3.Frontend, query string) []byte {
	sendCopyMessages(t, frontend, &pgproto3.Query{String: query})
	actual, err := frontend.Receive()
	if !assert.NoError(t, err) {
		t.FailNow()
	}
	complete, ok := actual.(*pgproto3.CommandComplete)
	if !assert.True(t, ok) {
		t.FailNow()
	}
	expectCopyMessage(t, frontend, &pgproto3.ReadyForQuery{TxStatus: byte(txstatus.TXIDLE)})
	return complete.CommandTag
}

func TestCopyExtendedExecuteFlushCopyDoneSync(t *testing.T) {
	frontend, conn, err := bootstrapConnection(t)
	assert.NoError(t, err, "startup failed")

	defer func() {
		_ = conn.Close()
	}()

	tt := []MessageGroup{
		{
			Request: []pgproto3.FrontendMessage{
				&pgproto3.Parse{
					Name:  "",
					Query: "COPY t(id) FROM STDIN",
				},
				&pgproto3.Bind{
					PreparedStatement: "",
				},
				&pgproto3.Execute{},
				&pgproto3.Flush{},

				&pgproto3.CopyDone{},
				&pgproto3.Sync{},
			},
			Response: []pgproto3.BackendMessage{
				&pgproto3.ParseComplete{},
				&pgproto3.BindComplete{},
				&pgproto3.CopyInResponse{
					ColumnFormatCodes: []uint16{0},
				},
				&pgproto3.CommandComplete{
					CommandTag: []byte("COPY 0"),
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
					Query: "COPY t(id) FROM STDIN",
				},
				&pgproto3.Bind{
					PreparedStatement: "",
				},
				&pgproto3.Execute{},
				&pgproto3.Flush{},

				&pgproto3.CopyFail{},
				&pgproto3.Sync{},
			},
			Response: []pgproto3.BackendMessage{
				&pgproto3.ParseComplete{},
				&pgproto3.BindComplete{},
				&pgproto3.CopyInResponse{
					ColumnFormatCodes: []uint16{0},
				},

				&pgproto3.ErrorResponse{
					Severity: "ERROR",
					Message:  "COPY from stdin failed: ",
				},
				&pgproto3.ReadyForQuery{
					TxStatus: byte(txstatus.TXIDLE),
				},
			},
		},
	}

	protoTestRunner(t, frontend, tt)
}
