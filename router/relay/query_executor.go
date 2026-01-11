package relay

import (
	"context"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/pg-sharding/lyx/lyx"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/pkg/plan"
	"github.com/pg-sharding/spqr/pkg/txstatus"
	"github.com/pg-sharding/spqr/router/client"
	"github.com/pg-sharding/spqr/router/parser"
	"github.com/pg-sharding/spqr/router/pgcopy"
	"github.com/pg-sharding/spqr/router/poolmgr"
)

type QueryDesc struct {
	Msg pgproto3.FrontendMessage

	P plan.Plan

	simple bool
	exec   *pgproto3.Execute
}

type ExecutorState struct {
	doFinalizeTx  bool
	attachedCopy  bool
	expectRowDesc bool

	savedBegin *pgproto3.Query
	cc         *pgproto3.CommandComplete
	eMsg       *pgproto3.ErrorResponse

	replyEmptyQuery bool

	/* XXX: make gang table here */
	activeShards []kr.ShardKey
}

// Execute required command via
// some protoc-specific logic
type QueryStateExecutor interface {
	txstatus.TxStatusMgr

	poolmgr.GangMgr

	Client() client.RouterClient

	/* Do all gang allocation required for plan processing */
	InitPlan(p plan.Plan) error

	DeploySliceTransactionBlock() error
	DeploySliceTransactionQuery(query string) error

	ExecBegin(query string, st *parser.ParseStateTXBegin) error
	ExecCommit(query string) error
	ExecRollback(query string) error

	ReplyCommandComplete(commandTag string) error

	/* Copy execution */
	ProcCopyPrepare(ctx context.Context, stmt *lyx.Copy, attached bool) (*pgcopy.CopyState, error)
	ProcCopy(ctx context.Context, data *pgproto3.CopyData, cps *pgcopy.CopyState) ([]byte, error)
	ProcCopyComplete(query pgproto3.FrontendMessage) (txstatus.TXStatus, error)

	ExecuteSlice(qd *QueryDesc, replyCl bool) error

	DeriveCommandComplete() error
	CompleteTx(mgr poolmgr.GangMgr) error

	ReplyEmptyQuery()
	FailStatement(err *pgproto3.ErrorResponse)

	ExecSet(rst RelayStateMgr, query, name, value string) error
	ExecReset(rst RelayStateMgr, query, name string) error
	ExecResetMetadata(rst RelayStateMgr, query, setting string) error

	ExpandRoutes(routes []kr.ShardKey) error

	Reset()
}
