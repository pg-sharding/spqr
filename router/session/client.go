package session

import (
	"context"
	"crypto/tls"
	"io"
	"net"

	"github.com/jackc/pgx/v5/pgproto3"
)

type CancelableClient interface {
	AssingCancelMsg(*pgproto3.CancelRequest)
	CancelRequest() *pgproto3.CancelRequest

	AssingCancelKeys(uint32, uint32)

	CancelPid() uint32
	CancelKey() uint32
}

type RouterSessionClient interface {
	io.ReadWriter
	CancelableClient
	ParamMgr

	EncyptNetConn(tlsconfig *tls.Config) net.Conn
	AssingBackend(b *pgproto3.Backend) error
	SetStartup(smsg *pgproto3.StartupMessage) error

	Send(msg pgproto3.BackendMessage) error
	SendCtx(ctx context.Context, msg pgproto3.BackendMessage) error
	Receive() (pgproto3.FrontendMessage, error)
	ReceiveCtx(ctx context.Context) (pgproto3.FrontendMessage, error)
}
