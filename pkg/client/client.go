package client

import (
	"context"
	"net"

	"github.com/pg-sharding/spqr/pkg/shard"
	"github.com/pg-sharding/spqr/router/session"
)

type Client interface {
	session.RouterSessionClient

	ID() string

	ReplyErrMsg(errmsg string) error
	ReplyRFQ() error
	ReplyNotice(message string) error
	ReplyDebugNotice(msg string) error
	ReplyDebugNoticef(fmt string, args ...interface{}) error

	ReplyWarningMsg(msg string) error
	ReplyWarningf(fmt string, args ...interface{}) error
	DefaultReply() error

	/* password clear text */
	PasswordCT() (string, error)
	PasswordMD5(salt [4]byte) (string, error)

	Usr() string
	DB() string

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
