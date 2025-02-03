package relay

import (
	"context"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/pg-sharding/lyx/lyx"
	"github.com/pg-sharding/spqr/pkg/meta"
	"github.com/pg-sharding/spqr/pkg/txstatus"
	"github.com/pg-sharding/spqr/router/parser"
	"github.com/pg-sharding/spqr/router/pgcopy"
)

// Execute requered command via
// some protoc-specific logic
type QueryStateExecutor interface {
	txstatus.TxStatusMgr

	ExecBegin(rst RelayStateMgr, query string, st *parser.ParseStateTXBegin) error
	ExecCommit(rst RelayStateMgr, query string) error
	ExecRollback(rst RelayStateMgr, query string) error

	/* Copy execution */
	ProcCopyPrepare(ctx context.Context, mgr meta.EntityMgr, stmt *lyx.Copy) (*pgcopy.CopyState, error)
	ProcCopy(ctx context.Context, data *pgproto3.CopyData, cps *pgcopy.CopyState) ([]byte, error)
	ProcCopyComplete(query pgproto3.FrontendMessage) error

	ExecSet(rst RelayStateMgr, query, name, value string) error
	ExecSetLocal(rst RelayStateMgr, query, name, value string) error
	ExecReset(rst RelayStateMgr, query, name string) error
	ExecResetMetadata(rst RelayStateMgr, query, setting string) error
}
