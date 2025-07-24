package relay

import (
	"context"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/pg-sharding/lyx/lyx"
	"github.com/pg-sharding/spqr/pkg/meta"
	"github.com/pg-sharding/spqr/pkg/txstatus"
	"github.com/pg-sharding/spqr/router/client"
	"github.com/pg-sharding/spqr/router/pgcopy"
	"github.com/pg-sharding/spqr/router/plan"
	"github.com/pg-sharding/spqr/router/server"
)

type QueryDesc struct {
	Msg pgproto3.FrontendMessage
	P   plan.Plan
}

// Execute required command via
// some protoc-specific logic
type QueryStateExecutor interface {
	txstatus.TxStatusMgr

	Client() client.RouterClient

	Deploy(server server.Server) error
	DeployTx(server server.Server, query string) error

	ExecBegin(rst RelayStateMgr, query string, st *lyx.TransactionStmt) error
	ExecCommit(rst RelayStateMgr, query string) error
	ExecRollback(rst RelayStateMgr, query string) error

	/* Copy execution */
	ProcCopyPrepare(ctx context.Context, mgr meta.EntityMgr, stmt *lyx.Copy, attached bool) (*pgcopy.CopyState, error)
	ProcCopy(ctx context.Context, data *pgproto3.CopyData, cps *pgcopy.CopyState) ([]byte, error)
	ProcCopyComplete(query pgproto3.FrontendMessage) (txstatus.TXStatus, error)

	ProcQuery(qd *QueryDesc, mgr meta.EntityMgr, waitForResp bool, replyCl bool) ([]pgproto3.BackendMessage, error)

	ExecSet(rst RelayStateMgr, query, name, value string) error
	ExecSetLocal(rst RelayStateMgr, query, name, value string) error
	ExecReset(rst RelayStateMgr, query, name string) error
	ExecResetMetadata(rst RelayStateMgr, query, setting string) error
}
