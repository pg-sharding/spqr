//go:build xproto || all

package prep_stmt_test

import (
	"os"
	"testing"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/pg-sharding/spqr/pkg/txstatus"
	"github.com/stretchr/testify/assert"
)

func TestXProtoNamedPortalRepeatedExecute(t *testing.T) {
	frontend, conn, err := bootstrapConnection(t)
	if !assert.NoError(t, err, "startup failed") {
		return
	}
	defer func() {
		_ = conn.Close()
	}()
	frontend.Trace(os.Stdout, pgproto3.TracerOptions{SuppressTimestamps: true})

	protoTestRunner(t, frontend, []MessageGroup{
		{
			Request: []pgproto3.FrontendMessage{
				&pgproto3.Query{
					String: "BEGIN",
				},
				&pgproto3.Parse{
					Query: "SELECT generate_series(1, 2) /* __spqr__execute_on: sh1 */",
				},
				&pgproto3.Bind{
					DestinationPortal: "p",
				},
				&pgproto3.Execute{
					Portal: "p",
				},
				&pgproto3.Sync{},
				&pgproto3.Execute{
					Portal: "p",
				},
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
				&pgproto3.ParseComplete{},
				&pgproto3.BindComplete{},
				&pgproto3.DataRow{
					Values: [][]byte{[]byte("1")},
				},
				&pgproto3.DataRow{
					Values: [][]byte{[]byte("2")},
				},
				&pgproto3.CommandComplete{
					CommandTag: []byte("SELECT 2"),
				},
				&pgproto3.ReadyForQuery{
					TxStatus: byte(txstatus.TXACT),
				},
				&pgproto3.CommandComplete{
					CommandTag: []byte("SELECT 0"),
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
			CheckErrAll: true,
		},
	})
}

func TestXProtoNamedPortalRepeatedDML(t *testing.T) {
	for _, tc := range []struct {
		name  string
		query string
		tag   string
	}{
		{"insert", "INSERT INTO t (id) VALUES (3)", "INSERT 0 1"},
		{"delete", "DELETE FROM t WHERE id = 1", "DELETE 1"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			frontend, conn, err := bootstrapConnection(t)
			if !assert.NoError(t, err, "startup failed") {
				return
			}
			defer func() {
				_ = conn.Close()
			}()
			frontend.Trace(os.Stdout, pgproto3.TracerOptions{SuppressTimestamps: true})

			const hint = " /* __spqr__execute_on: sh1 */"
			protoTestRunner(t, frontend, []MessageGroup{
				{
					Request: []pgproto3.FrontendMessage{
						&pgproto3.Query{
							String: "BEGIN",
						},
						&pgproto3.Query{
							String: "TRUNCATE t" + hint,
						},
						&pgproto3.Query{
							String: "INSERT INTO t (id) VALUES (1)" + hint,
						},
						&pgproto3.Parse{
							Query: tc.query + hint,
						},
						&pgproto3.Bind{
							DestinationPortal: "p",
						},
						&pgproto3.Execute{
							Portal: "p",
						},
						&pgproto3.Sync{},
						&pgproto3.Execute{
							Portal: "p",
						},
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
							CommandTag: []byte("TRUNCATE TABLE"),
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
						&pgproto3.CommandComplete{
							CommandTag: []byte(tc.tag),
						},
						&pgproto3.ReadyForQuery{
							TxStatus: byte(txstatus.TXACT),
						},
						&pgproto3.ErrorResponse{
							Severity: "ERROR",
							Code:     "55000",
							Message:  "portal \"p\" cannot be run",
						},
						&pgproto3.ReadyForQuery{
							TxStatus: byte(txstatus.TXERR),
						},
						&pgproto3.CommandComplete{
							CommandTag: []byte("ROLLBACK"),
						},
						&pgproto3.ReadyForQuery{
							TxStatus: byte(txstatus.TXIDLE),
						},
					},
					CheckErrAll: true,
				},
			})
		})
	}
}
