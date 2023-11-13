package client

import (
	"context"
	"crypto/tls"
	"net"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/pg-sharding/spqr/pkg/shard"
)

type Pmgr interface {
	SetParam(string, string)
	ResetParam(string)
	ResetAll()
	ConstructClientParams() *pgproto3.Query
	Params() map[string]string

	StartTx()
	CommitActiveSet()
	Savepoint(string)
	Rollback()
	RollbackToSP(string)
}

type Client interface {
	Pmgr

	ID() string

	ReplyErrMsg(errmsg string) error
	ReplyRFQ() error
	ReplyNotice(message string) error
	ReplyDebugNotice(msg string) error
	ReplyDebugNoticef(fmt string, args ...interface{}) error

	ReplyWarningMsg(msg string) error
	ReplyWarningf(fmt string, args ...interface{}) error
	DefaultReply() error

	Init(cfg *tls.Config) error

	/* password clear text */
	PasswordCT() (string, error)
	PasswordMD5(salt [4]byte) (string, error)

	StartupMessage() *pgproto3.StartupMessage

	Usr() string
	DB() string
	DS() string

	Send(msg pgproto3.BackendMessage) error
	SendCtx(ctx context.Context, msg pgproto3.BackendMessage) error
	Receive() (pgproto3.FrontendMessage, error)
	ReceiveCtx(ctx context.Context) (pgproto3.FrontendMessage, error)

	Shutdown() error
	Reset() error
	Close() error

	Shards() []shard.Shard
	Cancel() error

	Reply(msg string) error

	SetAuthType(uint32) error
}

type InteractRunner interface {
	ProcClient(ctx context.Context, conn net.Conn) error
}
