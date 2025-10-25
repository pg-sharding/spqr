package client

import (
	"context"
	"crypto/tls"
	"net"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/pg-sharding/spqr/pkg/session"
	"github.com/pg-sharding/spqr/pkg/shard"
	"github.com/pg-sharding/spqr/pkg/txstatus"
	"github.com/pg-sharding/spqr/router/statistics"
)

type Pmgr interface {
	SetParam(string, string)
	ResetParam(string)
	ResetAll()
	ConstructClientParams() *pgproto3.Query
	Params() map[string]string

	StartTx()
	CommitActiveSet()
	CleanupStatementSet()
	Savepoint(string)
	Rollback()
	RollbackToSP(string)
}

type Client interface {
	Pmgr
	session.SessionParamsHolder
	statistics.StatHolder

	ID() uint

	ReplyErrMsg(e string, c string, s txstatus.TXStatus) error
	ReplyErrWithTxStatus(e error, s txstatus.TXStatus) error
	ReplyErrMsgByCode(code string) error
	ReplyErr(errmsg error) error
	ReplyRFQ(txstatus txstatus.TXStatus) error
	ReplyNotice(message string) error
	ReplyDebugNotice(msg string) error
	ReplyDebugNoticef(fmt string, args ...any) error

	ReplyWarningMsg(msg string) error
	ReplyWarningf(fmt string, args ...any) error
	DefaultReply() error

	Init(cfg *tls.Config) error

	/* password clear text */
	PasswordCT() (string, error)
	PasswordMD5(salt [4]byte) (string, error)

	StartupMessage() *pgproto3.StartupMessage

	Usr() string
	/* XXX: also maybe ROLE support is meaningful? */
	DB() string

	Send(msg pgproto3.BackendMessage) error
	Receive() (pgproto3.FrontendMessage, error)

	Shutdown() error
	Reset() error
	Close() error

	Shards() []shard.ShardHostInstance
	Cancel() error
	CancelMsg() *pgproto3.CancelRequest

	Reply(msg string) error

	SetAuthType(uint32) error
}

type InteractRunner interface {
	ProcClient(ctx context.Context, conn net.Conn) error
}
