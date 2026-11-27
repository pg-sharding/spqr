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

func TestCopyOutProtocols(t *testing.T) {
	query := "COPY (SELECT 11001) TO STDOUT /* __spqr__execute_on: sh1 */"
	for _, tc := range []struct {
		name     string
		request  []pgproto3.FrontendMessage
		response []pgproto3.BackendMessage
	}{
		{
			name: "simple",
			request: []pgproto3.FrontendMessage{
				&pgproto3.Query{String: query},
			},
			response: []pgproto3.BackendMessage{
				&pgproto3.CopyOutResponse{ColumnFormatCodes: []uint16{0}},
				&pgproto3.CopyData{Data: []byte("11001\n")},
				&pgproto3.CopyDone{},
				&pgproto3.CommandComplete{CommandTag: []byte("COPY 1")},
				&pgproto3.ReadyForQuery{TxStatus: byte(txstatus.TXIDLE)},
			},
		},
		{
			name: "extended",
			request: []pgproto3.FrontendMessage{
				&pgproto3.Parse{Query: query},
				&pgproto3.Bind{PreparedStatement: ""},
				&pgproto3.Execute{},
				&pgproto3.Sync{},
			},
			response: []pgproto3.BackendMessage{
				&pgproto3.ParseComplete{},
				&pgproto3.BindComplete{},
				&pgproto3.CopyOutResponse{ColumnFormatCodes: []uint16{0}},
				&pgproto3.CopyData{Data: []byte("11001\n")},
				&pgproto3.CopyDone{},
				&pgproto3.CommandComplete{CommandTag: []byte("COPY 1")},
				&pgproto3.ReadyForQuery{TxStatus: byte(txstatus.TXIDLE)},
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			frontend, conn, err := bootstrapConnection(t)
			assert.NoError(t, err)
			defer func() { _ = conn.Close() }()
			protoTestRunner(t, frontend, []MessageGroup{{Request: tc.request, Response: tc.response}})
		})
	}
}
