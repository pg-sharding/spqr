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

func TestCopyTextCRLF(t *testing.T) {
	for _, tc := range []struct {
		name     string
		request  []pgproto3.FrontendMessage
		response []pgproto3.BackendMessage
	}{
		{
			name: "simple",
			request: []pgproto3.FrontendMessage{
				&pgproto3.Query{String: "COPY t(id) FROM STDIN"},
				&pgproto3.CopyData{Data: []byte("1\r\n20\r\n")},
				&pgproto3.CopyDone{},
			},
		},
		{
			name: "extended",
			request: []pgproto3.FrontendMessage{
				&pgproto3.Parse{Query: "COPY t(id) FROM STDIN"},
				&pgproto3.Bind{PreparedStatement: ""},
				&pgproto3.Execute{},
				&pgproto3.CopyData{Data: []byte("1\r\n20\r\n")},
				&pgproto3.CopyDone{},
				&pgproto3.Sync{},
			},
			response: []pgproto3.BackendMessage{&pgproto3.ParseComplete{}, &pgproto3.BindComplete{}},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			frontend, conn, err := bootstrapConnection(t)
			assert.NoError(t, err, "startup failed")
			defer func() { _ = conn.Close() }()

			preDelete := MessageGroup{
				Request:             []pgproto3.FrontendMessage{&pgproto3.Query{String: "DELETE FROM t WHERE id IN (1, 20)"}},
				Response:            []pgproto3.BackendMessage{&pgproto3.CommandComplete{}, &pgproto3.ReadyForQuery{TxStatus: byte(txstatus.TXIDLE)}},
				SkipCheckCommandTag: true,
			}
			copyResponse := append([]pgproto3.BackendMessage(nil), tc.response...)
			copyResponse = append(copyResponse,
				&pgproto3.CopyInResponse{ColumnFormatCodes: []uint16{0}},
				&pgproto3.CommandComplete{},
				&pgproto3.ReadyForQuery{TxStatus: byte(txstatus.TXIDLE)},
			)
			copy := MessageGroup{Request: tc.request, Response: copyResponse, SkipCheckCommandTag: true}
			delete := MessageGroup{
				Request:  []pgproto3.FrontendMessage{&pgproto3.Query{String: "DELETE FROM t WHERE id IN (1, 20)"}},
				Response: []pgproto3.BackendMessage{&pgproto3.CommandComplete{CommandTag: []byte("DELETE 2")}, &pgproto3.ReadyForQuery{TxStatus: byte(txstatus.TXIDLE)}},
			}

			protoTestRunner(t, frontend, []MessageGroup{preDelete, copy, delete})
		})
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
