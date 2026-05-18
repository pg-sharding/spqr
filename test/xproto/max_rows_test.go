//go:build xproto_suspend || all

package prep_stmt_test

import (
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/pg-sharding/spqr/pkg/txstatus"
	"github.com/stretchr/testify/assert"
)

func TestExecuteMaxRows(t *testing.T) {
	frontend, conn, err := bootstrapConnection(t)
	assert.NoError(t, err, "startup failed")

	defer func() {
		_ = conn.Close()
	}()

	tt := []MessageGroup{
		{
			Request: []pgproto3.FrontendMessage{
				&pgproto3.Close{
					Name:       "pstmt",
					ObjectType: 'S',
				},
				&pgproto3.Parse{
					Name:  "pstmt",
					Query: "select * from generate_series(1, 10) /* __spqr__execute_on: sh1 */",
				},
				&pgproto3.Bind{
					PreparedStatement: "pstmt",
				},
				&pgproto3.Execute{
					MaxRows: 5,
				},
				&pgproto3.Flush{},

				&pgproto3.Sync{},
			},
			Response: []pgproto3.BackendMessage{
				&pgproto3.CloseComplete{},
				&pgproto3.ParseComplete{},
				&pgproto3.BindComplete{},
				&pgproto3.DataRow{
					Values: [][]byte{
						[]byte("1"),
					},
				},
				&pgproto3.DataRow{
					Values: [][]byte{
						[]byte("2"),
					},
				},
				&pgproto3.DataRow{
					Values: [][]byte{
						[]byte("3"),
					},
				},
				&pgproto3.DataRow{
					Values: [][]byte{
						[]byte("4"),
					},
				},
				&pgproto3.DataRow{
					Values: [][]byte{
						[]byte("5"),
					},
				},
				&pgproto3.PortalSuspended{},
				&pgproto3.ReadyForQuery{
					TxStatus: byte(txstatus.TXIDLE),
				},
			},
		},

		{
			Request: []pgproto3.FrontendMessage{
				&pgproto3.Close{
					Name:       "pstmt1",
					ObjectType: 'S',
				},
				&pgproto3.Parse{
					Name:  "pstmt1",
					Query: "select * from generate_series(1, 10) /* __spqr__execute_on: sh1 */",
				},
				&pgproto3.Bind{
					PreparedStatement: "pstmt1",
				},
				&pgproto3.Execute{
					MaxRows: 5,
				},
				&pgproto3.Flush{},

				&pgproto3.Execute{
					MaxRows: 3,
				},
				&pgproto3.Flush{},

				&pgproto3.Execute{
					MaxRows: 2,
				},
				&pgproto3.Sync{},
			},
			Response: []pgproto3.BackendMessage{
				&pgproto3.CloseComplete{},
				&pgproto3.ParseComplete{},
				&pgproto3.BindComplete{},
				&pgproto3.DataRow{
					Values: [][]byte{
						[]byte("1"),
					},
				},
				&pgproto3.DataRow{
					Values: [][]byte{
						[]byte("2"),
					},
				},
				&pgproto3.DataRow{
					Values: [][]byte{
						[]byte("3"),
					},
				},
				&pgproto3.DataRow{
					Values: [][]byte{
						[]byte("4"),
					},
				},
				&pgproto3.DataRow{
					Values: [][]byte{
						[]byte("5"),
					},
				},
				&pgproto3.PortalSuspended{},

				&pgproto3.DataRow{
					Values: [][]byte{
						[]byte("6"),
					},
				},
				&pgproto3.DataRow{
					Values: [][]byte{
						[]byte("7"),
					},
				},
				&pgproto3.DataRow{
					Values: [][]byte{
						[]byte("8"),
					},
				},
				&pgproto3.PortalSuspended{},

				&pgproto3.DataRow{
					Values: [][]byte{
						[]byte("9"),
					},
				},
				&pgproto3.DataRow{
					Values: [][]byte{
						[]byte("10"),
					},
				},
				&pgproto3.PortalSuspended{},

				&pgproto3.ReadyForQuery{
					TxStatus: byte(txstatus.TXIDLE),
				},
			},
		},

		{
			Request: []pgproto3.FrontendMessage{
				&pgproto3.Close{
					Name:       "pstmt2",
					ObjectType: 'S',
				},
				&pgproto3.Parse{
					Name:  "pstmt2",
					Query: "select * from generate_series(1, 10) /* __spqr__execute_on: sh1 */",
				},
				&pgproto3.Bind{
					PreparedStatement: "pstmt2",
				},
				&pgproto3.Execute{
					MaxRows: 5,
				},
				&pgproto3.Flush{},

				&pgproto3.Execute{
					MaxRows: 3,
				},
				&pgproto3.Flush{},

				&pgproto3.Execute{
					MaxRows: 2,
				},
				&pgproto3.Flush{},

				&pgproto3.Execute{
					MaxRows: 2,
				},
				&pgproto3.Sync{},
			},
			Response: []pgproto3.BackendMessage{
				&pgproto3.CloseComplete{},
				&pgproto3.ParseComplete{},
				&pgproto3.BindComplete{},
				&pgproto3.DataRow{
					Values: [][]byte{
						[]byte("1"),
					},
				},
				&pgproto3.DataRow{
					Values: [][]byte{
						[]byte("2"),
					},
				},
				&pgproto3.DataRow{
					Values: [][]byte{
						[]byte("3"),
					},
				},
				&pgproto3.DataRow{
					Values: [][]byte{
						[]byte("4"),
					},
				},
				&pgproto3.DataRow{
					Values: [][]byte{
						[]byte("5"),
					},
				},
				&pgproto3.PortalSuspended{},

				&pgproto3.DataRow{
					Values: [][]byte{
						[]byte("6"),
					},
				},
				&pgproto3.DataRow{
					Values: [][]byte{
						[]byte("7"),
					},
				},
				&pgproto3.DataRow{
					Values: [][]byte{
						[]byte("8"),
					},
				},
				&pgproto3.PortalSuspended{},

				&pgproto3.DataRow{
					Values: [][]byte{
						[]byte("9"),
					},
				},
				&pgproto3.DataRow{
					Values: [][]byte{
						[]byte("10"),
					},
				},
				&pgproto3.PortalSuspended{},

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
				&pgproto3.Close{
					Name:       "pstmt3",
					ObjectType: 'S',
				},
				&pgproto3.Parse{
					Name:  "pstmt3",
					Query: "select * from generate_series(1, 10) /* __spqr__execute_on: sh1 */",
				},
				&pgproto3.Bind{
					PreparedStatement: "pstmt3",
				},
				&pgproto3.Execute{
					MaxRows: 5,
				},
				&pgproto3.Sync{},

				&pgproto3.Execute{
					MaxRows: 5,
				},
				&pgproto3.Sync{},
			},
			Response: []pgproto3.BackendMessage{
				&pgproto3.CloseComplete{},
				&pgproto3.ParseComplete{},
				&pgproto3.BindComplete{},
				&pgproto3.DataRow{
					Values: [][]byte{
						[]byte("1"),
					},
				},
				&pgproto3.DataRow{
					Values: [][]byte{
						[]byte("2"),
					},
				},
				&pgproto3.DataRow{
					Values: [][]byte{
						[]byte("3"),
					},
				},
				&pgproto3.DataRow{
					Values: [][]byte{
						[]byte("4"),
					},
				},
				&pgproto3.DataRow{
					Values: [][]byte{
						[]byte("5"),
					},
				},
				&pgproto3.PortalSuspended{},
				&pgproto3.ReadyForQuery{
					TxStatus: byte(txstatus.TXIDLE),
				},

				&pgproto3.ErrorResponse{
					Message:  "portal \"\" does not exist",
					Severity: "ERROR",
				},
				&pgproto3.ReadyForQuery{
					TxStatus: byte(txstatus.TXIDLE),
				},
			},
		},

		{
			Request: []pgproto3.FrontendMessage{
				&pgproto3.Close{
					Name:       "pstmt4",
					ObjectType: 'S',
				},
				&pgproto3.Parse{
					Name:  "pstmt4",
					Query: "select * from generate_series(1, 3) /* __spqr__execute_on: sh1 */",
				},
				&pgproto3.Bind{
					PreparedStatement: "pstmt4",
				},
				&pgproto3.Execute{
					MaxRows: 0,
				},
				&pgproto3.Sync{},
			},
			Response: []pgproto3.BackendMessage{
				&pgproto3.CloseComplete{},
				&pgproto3.ParseComplete{},
				&pgproto3.BindComplete{},
				&pgproto3.DataRow{
					Values: [][]byte{
						[]byte("1"),
					},
				},
				&pgproto3.DataRow{
					Values: [][]byte{
						[]byte("2"),
					},
				},
				&pgproto3.DataRow{
					Values: [][]byte{
						[]byte("3"),
					},
				},
				&pgproto3.CommandComplete{
					CommandTag: []byte("SELECT 3"),
				},
				&pgproto3.ReadyForQuery{
					TxStatus: byte(txstatus.TXIDLE),
				},
			},
		},
	}

	assert.NoError(t, conn.SetDeadline(time.Now().Add(30*time.Second)))

	protoTestRunner(t, frontend, tt)
}
