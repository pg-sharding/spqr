package internal

import (
	"crypto/tls"
	"log"
	"net"

	"github.com/jackc/pgproto3"
	"github.com/pg-sharding/spqr/internal/config"
	"github.com/wal-g/tracelog"
	"golang.org/x/xerrors"
)

type Shard interface {
	//Connect(proto string) (net.Conn, error)

	Cfg() *config.ShardCfg

	Name() string
	SHKey() ShardKey

	Send(query pgproto3.FrontendMessage) error
	Receive() (pgproto3.BackendMessage, error)

	ReqBackendSsl(tlscfg *tls.Config) error
}

type ShardImpl struct {
	cfg *config.ShardCfg

	lg log.Logger

	name string

	pgconn PgConn
}

func (sh *ShardImpl) ReqBackendSsl(tlscfg *tls.Config) error {
	return sh.ReqBackendSsl(tlscfg)
}

func (sh *ShardImpl) Send(query pgproto3.FrontendMessage) error {
	return sh.pgconn.Send(query)
}

func (sh *ShardImpl) Receive() (pgproto3.BackendMessage, error) {
	return sh.pgconn.Receive()
}

func (sh *ShardImpl) Name() string {
	return sh.name
}

func (sh *ShardImpl) Cfg() *config.ShardCfg {
	return sh.cfg
}

func (sh *ShardImpl) connect(proto string) (net.Conn, error) {
	return net.Dial(proto, sh.cfg.ConnAddr)
}

var _ Shard = &ShardImpl{}

func (sh *ShardImpl) SHKey() ShardKey {
	return ShardKey{
		name: sh.name,
	}
}

func NewShard(name string, cfg *config.ShardCfg) (Shard, error) {

	sh := &ShardImpl{
		cfg:  cfg,
		name: name,
	}

	netconn, err := sh.connect(cfg.Proto)
	if err != nil {
		return nil, err
	}

	pgconn, err := NewPgConn(netconn)
	if err != nil {
		return nil, err
	}

	sh.pgconn = pgconn

	return sh, nil
}

func (sh *ShardImpl) Auth(sm *pgproto3.StartupMessage) error {

	err := sh.pgconn.Send(sm)
	if err != nil {
		return err
	}

	for {
		msg, err := sh.Receive()
		if err != nil {
			return err
		}
		switch v := msg.(type) {
		case *pgproto3.ReadyForQuery:
			return nil
		case *pgproto3.Authentication:
			err := authBackend(sh, v)
			if err != nil {
				tracelog.InfoLogger.Printf("failed to perform backend auth %w", err)
				return err
			}
		case *pgproto3.ErrorResponse:
			return xerrors.New(v.Message)
		default:
			tracelog.InfoLogger.Printf("unexpected msg type received %T", v)
		}
	}
}
